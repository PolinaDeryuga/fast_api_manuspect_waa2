# app/api/routes/processing.py

import logging
import os
from typing import List, Dict, Any # Для аннотации возвращаемого типа результата Celery

# Импорты FastAPI и зависимостей
from fastapi import APIRouter, Depends, HTTPException, Body
from fastapi_limiter.depends import RateLimiter

# Импорты из вашего проекта
from app.api import deps # Предполагается, что deps.get_current_user находится здесь
from app.core.celery import celery # Ваш настроенный Celery инстанс
from app.schemas.response_schema import IPostResponseBase, create_response # Ваши модели ответа
from app.api.celery_task import ( # Импортируем нужные задачи и константы
    process_event_logs_to_dataframe_task,
    process_environment_batch_task,
    TEMP_DIR # Если TEMP_DIR используется в роутере (например, для информации)
)

# Импорт для Pydantic моделей запросов
from pydantic import BaseModel

# Импортируем chain для создания цепочек Celery
from celery import chain

# --- Настройка ---

# Настройка логирования (лучше использовать общий логгер приложения)
logger = logging.getLogger(__name__)

# Создаем APIRouter
# Добавляем префикс и теги для лучшей организации и документации
router = APIRouter(
    prefix="/processing",
    tags=["processing"]
)

# --- Pydantic Модели ---

# Модель для запроса на обработку JSON логов
class ProcessJsonRequest(BaseModel):
    """
    Request body for triggering JSON log processing.
    """
    # base_path - это путь на файловой системе, где смонтирован Google Drive
    # Убедитесь, что этот путь доступен для Celery воркеров.
    base_path: str = "/content/drive/MyDrive/Магистратура Итмо" # Пример дефолтного значения

# --- Вспомогательные функции (если нужны, но, похоже, create_response уже есть) ---
# def create_response(...): ... # Используем импортированную функцию

# --- Эндпоинты ---

# @router.post("/sentiment_analysis", ...)
# async def sentiment_analysis_prediction(...):
#     """
#     (Пример: Существующий эндпоинт NLP)
#     """
#     pass # Ваша реализация

# @router.post("/text_generation_prediction_batch_task", ...)
# async def text_generation_prediction_batch_task(...):
#     """
#     (Пример: Существующий эндпоинт NLP)
#     """
#     pass # Ваша реализация

# @router.post("/text_generation_prediction_batch_task_after_some_seconds", ...)
# async def text_generation_prediction_batch_task_after_some_seconds(...):
#     """
#     (Пример: Существующий эндпоинт NLP)
#     """
#     pass # Ваша реализация


@router.post(
    "/process_event_logs_to_dataframe_batch_task",
    response_model=IPostResponseBase, # Указываем модель ответа
    dependencies=[
        Depends(RateLimiter(times=5, hours=1)), # Ограничение
        Depends(deps.get_current_user), # Требуем аутентификацию (используйте функцию без вызова)
    ],
)
async def trigger_process_event_logs_to_dataframe_batch_task(
    request: ProcessJsonRequest = Body(...), # Принимаем base_path из тела запроса
) -> IPostResponseBase:
    """
    Triggers an async batch task (Celery) to find, parse JSON event log files
    from a specified base directory (e.g., Google Drive mounted path),
    and create a Pandas DataFrame from the data. Returns the task ID.
    """
    base_path = request.base_path
    logger.info(f"Received request to process logs from: {base_path}")

    # --- Валидация входных данных (опционально, но рекомендуется) ---
    # Важно: Эта проверка выполняется на сервере FastAPI.
    # Доступность пути на воркере Celery зависит от настройки вашей инфраструктуры.
    # if not os.path.isdir(base_path):
    #     logger.warning(f"Base path '{base_path}' not found or is not a directory on API server.")
    #     # Можно вернуть HTTPException, если отсутствие директории на API сервере - это ошибка
    #     # raise HTTPException(status_code=400, detail=f"Base path not found or is not a directory: {base_path}")
    #     # Или просто логировать и позволить задаче Celery обработать ошибку,
    #     # если путь может быть доступен только на воркере.

    # Запускаем Celery задачу
    # Убедитесь, что process_event_logs_to_dataframe_task импортирован
    process_task = process_event_logs_to_dataframe_task.delay(base_path)
    logger.info(f"Celery task 'process_event_logs_to_dataframe_task' triggered with ID: {process_task.task_id}")

    # Возвращаем ID задачи немедленно
    return create_response(
        message="JSON processing and DataFrame creation task triggered successfully",
        data={"task_id": process_task.task_id}
    )


@router.post(
    "/start-environment-workflow", # Путь относительно префикса /processing
    response_model=IPostResponseBase, # Указываем модель ответа
    dependencies=[
        Depends(RateLimiter(times=3, hours=1)), # Другое ограничение
        Depends(deps.get_current_user), # Требуем аутентификацию
    ],
)
async def start_environment_processing_workflow(
    request: ProcessJsonRequest = Body(...), # Используем ту же модель, если нужно
) -> IPostResponseBase:
    """
    Starts the Celery workflow (chain) to parse logs from a base path,
    extract environment data, and save it to a CSV file.
    Returns the root task ID of the chain.
    """
    base_path = request.base_path
    logger.info(f"API endpoint hit: /processing/start-environment-workflow with base_path='{base_path}'")

    # --- Определение путей для выходных данных ---
    # Важно: output_base_path должен быть доступен для ЗАПИСИ для Celery воркеров.
    # Этот путь внутри контейнера/системы, где работают воркеры.
    output_base_path = '/app/processed_data' # Пример пути внутри контейнера
    output_filename = 'processed_environment_data.csv'
    expected_output_full_path = os.path.join(output_base_path, output_filename)


    # --- Создание и запуск цепочки Celery ---
    logger.info("Creating Celery chain for environment processing workflow.")

    # 1. process_event_logs_to_dataframe_task.s(base_path):
    #    Сигнатура первой задачи. base_path передается как позиционный аргумент.
    #    Эта задача вернет путь к временному файлу (как результат).
    first_task_signature = process_event_logs_to_dataframe_task.s(base_path)

    # 2. process_environment_batch_task.s(output_filename=..., output_base_path=...):
    #    Сигнатура второй задачи. output_filename и output_base_path передаются как именованные аргументы.
    #    Celery автоматически передаст РЕЗУЛЬТАТ ПЕРВОЙ задачи (путь к временному файлу)
    #    как ПЕРВЫЙ ПОЗИЦИОННЫЙ аргумент этой второй задаче (аргумент input_file_path).
    second_task_signature = process_environment_batch_task.s(
        output_filename=output_filename,
        output_base_path=output_base_path
    )

    # Связываем задачи в цепочку: (task1 | task2)
    workflow_chain = chain(first_task_signature, second_task_signature)

    # Запускаем цепочку асинхронно
    # .apply_async() предпочтительнее .delay() для цепочек и более сложной настройки
    result = workflow_chain.apply_async()

    logger.info(f"Celery workflow chain started. Root Task ID: {result.id}")

    # Возвращаем немедленный ответ клиенту
    return create_response(
        message="Environment data processing workflow has been initiated.",
        data={
            "root_task_id": result.id, # Возвращаем ID корневой задачи цепочки
            "output_expected_path": expected_output_full_path # Возвращаем ожидаемый путь к результату
        }
    )


@router.get(
    "/get_result_from_batch_task",
    response_model=IPostResponseBase, # Указываем модель ответа
    dependencies=[
        Depends(RateLimiter(times=10, minutes=1)),
        Depends(deps.get_current_user), # Добавьте аутентификацию
    ],
)
async def get_result_from_batch_task(task_id: str) -> IPostResponseBase:
    """
    Get result from batch task using task_id.
    For the JSON processing task, the result will be the DataFrame data
    as a list of dictionaries. For a workflow chain, this will return the
    final result of the *last* task in the chain.
    """
    # Убедитесь, что celery импортирован
    async_result = celery.AsyncResult(task_id)
    logger.info(f"Checking status for task ID: {task_id}, state: {async_result.state}")

    if async_result.ready():
        if not async_result.successful():
             # Добавим информацию об ошибке, если задача провалилась
             error_message = f"Task {task_id} failed with state {async_result.state}."
             try:
                 # Получаем исключение и traceback из результата задачи
                 # propagate=False чтобы не поднять исключение здесь, а обработать его
                 exc, tb = async_result.get(timeout=1.0, propagate=False)
                 error_message += f" Exception type: {type(exc).__name__}."
                 if exc:
                      error_message += f" Exception message: {str(exc)}."
                 if tb:
                     # Обрежем traceback, так как он может быть очень длинным
                     error_message += f" Traceback (partial): {tb[:500]}..." # Уменьшил длину для лога
                 logger.error(f"Task {task_id} failed: {error_message}", exc_info=True) # Логируем полную ошибку
             except Exception as e:
                 # Если даже получение ошибки провалилось
                 error_message += f" Could not retrieve exception details: {e}"
                 logger.error(f"Task {task_id} failed, could not get details: {e}", exc_info=True)


             raise HTTPException(
                 status_code=500, # Изменим на 500, так как это ошибка сервера (задачи)
                 detail=error_message,
             )

        # Если задача успешна, получаем результат (который должен быть сериализован Celery)
        # Результат задачи process_event_logs_to_dataframe_task - это список словарей
        # Результат цепочки - это результат последней задачи (process_environment_batch_task),
        # которая, вероятно, возвращает путь к файлу или статус.
        try:
            result_data = async_result.get(timeout=1.0)
            logger.info(f"Task {task_id} completed successfully. Result type: {type(result_data)}")

            # Опционально: Проверить тип результата, если ожидаете DataFrame (список словарей)
            # Если этот эндпоинт универсальный для разных задач,
            # возможно, не стоит жестко проверять тип.
            # if async_result.task_name == 'app.api.celery_task.process_event_logs_to_dataframe_task':
            #     if not isinstance(result_data, list):
            #          logger.warning(f"Task {task_id} returned unexpected result type for DataFrame task: {type(result_data)}")
                      # Можно вернуть ошибку или просто вернуть данные как есть

            return create_response(
                message="Task result retrieved successfully",
                data={"task_id": task_id, "result": result_data}, # result_data уже сериализован Celery
            )
        except Exception as e:
             # Ошибка при получении успешного результата (редко, но возможно)
             logger.error(f"Error retrieving result for successful task {task_id}: {e}", exc_info=True)
             raise HTTPException(
                 status_code=500,
                 detail=f"Error retrieving result for successful task {task_id}: {e}"
             )


    else:
        # Задача еще не готова
        # Можно вернуть 202 Accepted с текущим состоянием задачи
        logger.info(f"Task {task_id} is still pending or running (state: {async_result.state}).")
        raise HTTPException(
            status_code=202, # 202 Accepted - хорошая практика для асинхронных операций
            detail=f"Task {task_id} is still running or pending (state: {async_result.state}).",
            # Можно добавить Location заголовок, указывающий на этот же URL
            # headers={"Location": f"/processing/get_result_from_batch_task?task_id={task_id}"}
        )

# Убедимся, что временная директория существует там, где ее ожидают задачи Celery.
# Это важно, если задачи используют TEMP_DIR для временных файлов между шагами цепочки.
# Лучше делать это при старте приложения (например, в main.py или при инициализации Celery),
# но можно и здесь, если уверены, что этот файл импортируется при старте.
# os.makedirs(TEMP_DIR, exist_ok=True)
# logger.info(f"Ensured temporary directory exists: {TEMP_DIR}")