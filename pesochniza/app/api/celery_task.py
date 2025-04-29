# app/api/celery_task.py

import logging
import os
import pandas as pd
from typing import List, Dict, Any
import uuid # Для создания уникальных имен файлов

from app.core.celery import celery
# Убедитесь, что эти импорты корректны и функции существуют
from app.utils.json_processor import parse_json_files_to_roots, create_dataframe
from app.utils.environment_processing import extract_environment_info
from app.utils.file_utils import save_to_csv

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Определяем базовый каталог для временных файлов, доступный для воркеров
# Возможно, стоит вынести это в настройки приложения
TEMP_DIR = '/app/temp_celery_files'
os.makedirs(TEMP_DIR, exist_ok=True)


@celery.task(bind=True) # bind=True позволяет получить доступ к self (инстансу задачи)
def process_event_logs_to_dataframe_task(self, base_path: str) -> str: # <-- Возвращает путь к файлу (строка)
    """
    Celery task to find, parse JSON event log files, create DataFrame,
    and save it to a temporary Parquet file.

    Args:
        base_path: The base file system path where logs are located.

    Returns:
        The absolute path to the temporary Parquet file on success.
        Raises an exception on failure.
    """
    task_id = self.request.id
    logger.info(f"Task {task_id} started: process_event_logs_to_dataframe_task with base_path='{base_path}'")
    self.update_state(state='PROGRESS', meta={'current': 0, 'total': 3, 'status': 'Parsing JSON files'})

    try:
        # Шаг 1: Парсим JSON файлы в объекты Root
        all_roots = parse_json_files_to_roots(base_path)
        logger.info(f"Task {task_id}: Finished parsing JSON files. Got {len(all_roots)} Root objects.")
        self.update_state(state='PROGRESS', meta={'current': 1, 'total': 3, 'status': 'Creating DataFrame'})

        # Шаг 2: Создаем DataFrame из объектов Root
        events_df = create_dataframe(all_roots)
        logger.info(f"Task {task_id}: Finished creating DataFrame. Got {len(events_df)} rows.")
        self.update_state(state='PROGRESS', meta={'current': 2, 'total': 3, 'status': 'Saving DataFrame to temp file'})

        if events_df.empty:
            logger.warning(f"Task {task_id}: No data processed. DataFrame is empty.")
            # Создаем пустой файл-заглушку или возвращаем специальное значение,
            # если следующая задача не может обработать пустой файл
            # Для простоты, сохраним пустой файл.
            temp_file_path = os.path.join(TEMP_DIR, f'events_raw_{task_id}_{uuid.uuid4()}.parquet')
            events_df.to_parquet(temp_file_path, index=False)
            logger.info(f"Task {task_id}: Saved empty DataFrame to {temp_file_path}")
            self.update_state(state='SUCCESS', meta={'current': 3, 'total': 3, 'status': 'Completed (empty data)'})
            return temp_file_path


        # Шаг 3: Сохраняем DataFrame во временный файл (например, Parquet)
        # Используем ID задачи для уникальности имени файла
        temp_file_path = os.path.join(TEMP_DIR, f'events_raw_{task_id}_{uuid.uuid4()}.parquet')
        # Убедимся, что директория существует (уже сделано выше)
        # Сохраняем в Parquet, так как он эффективнее для Pandas
        events_df.to_parquet(temp_file_path, index=False)
        logger.info(f"Task {task_id}: DataFrame saved to temporary file: {temp_file_path}")
        self.update_state(state='PROGRESS', meta={'current': 3, 'total': 3, 'status': 'Completed'})

        # Возвращаем путь к сохраненному файлу
        return temp_file_path

    except Exception as e:
        logger.error(f"Task {task_id}: Failed with error: {e}", exc_info=True)
        # Обновляем состояние задачи на FAILURE
        self.update_state(state='FAILURE', meta={'error': str(e)})
        # Переподнимаем исключение, чтобы Celery пометил задачу как FAILED
        raise


@celery.task(bind=True, name='process_environment_batch')
# Изменяем аргументы: теперь принимаем путь к входному файлу как первый позиционный аргумент
def process_environment_batch_task(self,
                                   input_file_path: str, # <-- Путь к файлу с исходными данными
                                   output_filename: str = 'processed_environment_data.csv',
                                   output_base_path: str = '/app/processed_data'):
    """
    Celery task to read data from a file, process environment data,
    extract window information, and save the result to a CSV file.
    Reads data from input_file_path (usually from a previous task).

    Args:
        input_file_path: Полный путь к файлу (например, Parquet) с исходными данными,
                         содержащими столбец 'environment'. Этот аргумент
                         получается автоматически от предыдущей задачи в цепочке.
        output_filename: Имя CSV файла для сохранения.
        output_base_path: Базовый путь для сохранения файла.

    Returns:
        Словарь с результатом (путь к файлу или сообщение об ошибке).
    """
    task_id = self.request.id
    logger.info(f"Task {task_id} started: process_environment_batch_task")
    logger.info(f"Input file: {input_file_path}")
    logger.info(f"Output path: {os.path.join(output_base_path, output_filename)}")
    self.update_state(state='PROGRESS', meta={'current': 0, 'total': 6, 'status': 'Loading data'})

    try:
        # --- ШАГ 1: ЗАГРУЗКА ДАННЫХ ИЗ ФАЙЛА ---
        if not os.path.exists(input_file_path):
            error_msg = f"Input file not found: {input_file_path}"
            logger.error(f"Task {task_id}: {error_msg}")
            self.update_state(state='FAILURE', meta={'error': error_msg})
            # Не переподнимаем исключение здесь, так как возвращаем словарь ошибки
            return {"status": "error", "message": error_msg}

        logger.info(f"Task {task_id}: Reading data from {input_file_path}...")
        try:
            # Читаем из Parquet файла, созданного предыдущей задачей
            data = pd.read_parquet(input_file_path)
        except Exception as read_error:
             error_msg = f"Failed to read input file {input_file_path}: {read_error}"
             logger.error(f"Task {task_id}: {error_msg}", exc_info=True)
             self.update_state(state='FAILURE', meta={'error': error_msg})
             return {"status": "error", "message": error_msg}

        logger.info(f"Task {task_id}: Successfully loaded {len(data)} records from {input_file_path}.")
        self.update_state(state='PROGRESS', meta={'current': 1, 'total': 6, 'status': 'Applying processing function'})

        # --- ОПЦИОНАЛЬНО: Удаление временного файла после загрузки ---
        # Важно убедиться, что файл больше не нужен другим задачам или процессам
        # try:
        #     os.remove(input_file_path)
        #     logger.info(f"Task {task_id}: Removed temporary input file: {input_file_path}")
        # except Exception as rm_error:
        #     logger.warning(f"Task {task_id}: Failed to remove temporary input file {input_file_path}: {rm_error}")
        # -----------------------------------------------------------

        if 'environment' not in data.columns:
            error_msg = "Column 'environment' not found in the input data. Task aborted."
            logger.error(f"Task {task_id}: {error_msg}")
            self.update_state(state='FAILURE', meta={'error': error_msg})
            return {"status": "error", "message": error_msg}

        if data.empty:
             logger.info(f"Task {task_id}: Input DataFrame is empty. Nothing to process.")
             self.update_state(state='SUCCESS', meta={'current': 6, 'total': 6, 'status': 'Completed (empty input)'})
             return {"status": "success", "message": "Input data was empty, nothing processed."}


        # --- ШАГ 2: ПРИМЕНЕНИЕ ФУНКЦИИ ОБРАБОТКИ ---
        logger.info(f"Task {task_id}: Applying extract_environment_info...")
        data['env_info'] = data['environment'].apply(
            lambda x: extract_environment_info(x) if pd.notna(x) and x else None
        )
        logger.info(f"Task {task_id}: extract_environment_info completed.")
        self.update_state(state='PROGRESS', meta={'current': 2, 'total': 6, 'status': 'Filtering and exploding'})


        # --- ШАГ 3: ФИЛЬТРАЦИЯ И РАЗВОРАЧИВАНИЕ ---
        logger.info(f"Task {task_id}: Filtering and exploding data...")
        data_processed = data[data['env_info'].notna()].copy()

        if data_processed.empty:
            logger.info(f"Task {task_id}: No valid environment data to process after initial filtering. Task finished.")
            self.update_state(state='SUCCESS', meta={'current': 6, 'total': 6, 'status': 'Completed (no valid env data)'})
            return {"status": "success", "message": "No valid environment data to process."}

        data_processed['env_info'] = data_processed['env_info'].apply(lambda x: x if isinstance(x, list) else [])
        data_exploded = data_processed.explode('env_info').copy()

        if data_exploded.empty:
             logger.info(f"Task {task_id}: No window data found after exploding. Task finished.")
             self.update_state(state='SUCCESS', meta={'current': 6, 'total': 6, 'status': 'Completed (no window data)'})
             return {"status": "success", "message": "No window data found after processing."}

        env_info_df = data_exploded['env_info'].apply(lambda x: pd.Series(x) if isinstance(x, dict) else pd.Series({}))

        original_columns = data_processed.drop(columns=['environment', 'env_info'], errors='ignore').columns

        data_exploded = data_exploded.reset_index(drop=True)
        env_info_df = env_info_df.reset_index(drop=True)

        processed_df = pd.concat([data_exploded[original_columns], env_info_df], axis=1)
        logger.info(f"Task {task_id}: Filtering and exploding completed.")
        self.update_state(state='PROGRESS', meta={'current': 3, 'total': 6, 'status': 'Filtering by root_app'})


        # --- ШАГ 4: ФИЛЬТРАЦИЯ ПО root_app ---
        logger.info(f"Task {task_id}: Filtering by root_app...")
        if 'root_app' in processed_df.columns:
            processed_df = processed_df[processed_df['root_app'].notna() & (processed_df['root_app'] != '')].copy()
        else:
            logger.warning(f"Task {task_id}: Column 'root_app' not found after processing. Skipping root_app filtering.")

        if processed_df.empty:
            logger.info(f"Task {task_id}: No data remaining after filtering by root_app. Task finished.")
            self.update_state(state='SUCCESS', meta={'current': 6, 'total': 6, 'status': 'Completed (no root_app data)'})
            return {"status": "success", "message": "No data remaining after filtering by root_app."}

        logger.info(f"Task {task_id}: Remaining {len(processed_df)} records after filtering by root_app.")
        self.update_state(state='PROGRESS', meta={'current': 4, 'total': 6, 'status': 'Dropping columns'})

        # --- ШАГ 5: УДАЛЕНИЕ НЕНУЖНЫХ СТОЛБЦОВ ---
        logger.info(f"Task {task_id}: Dropping unnecessary columns...")
        columns_to_drop = ['batch_id', 'related_file', 'log_record_counter', 'program_titles']
        # Удаляем 'environment' и 'env_info' если они вдруг остались (не должны)
        columns_to_drop.extend(['environment', 'env_info'])
        processed_df = processed_df.drop(columns=columns_to_drop, errors='ignore').copy()
        logger.info(f"Task {task_id}: Column dropping completed.")
        self.update_state(state='PROGRESS', meta={'current': 5, 'total': 6, 'status': 'Saving to CSV'})

        # --- ШАГ 6: СОХРАНЕНИЕ В CSV ---
        output_full_path = os.path.join(output_base_path, output_filename)
        logger.info(f"Task {task_id}: Saving processed data to CSV: {output_full_path}...")
        saved_file_path = save_to_csv(processed_df, output_filename, output_base_path)

        if saved_file_path:
            logger.info(f"Task {task_id}: Successfully saved to {saved_file_path}")
            self.update_state(state='SUCCESS', meta={'current': 6, 'total': 6, 'status': 'Completed'})
            return {"status": "success", "message": "Data processed and saved successfully", "file_path": saved_file_path}
        else:
            error_msg = "Failed to save CSV file."
            logger.error(f"Task {task_id}: {error_msg}")
            self.update_state(state='FAILURE', meta={'error': error_msg})
            return {"status": "error", "message": error_msg}

    except Exception as e:
        error_msg = f"An unexpected error occurred during task execution: {e}"
        logger.error(f"Task {task_id}: {error_msg}", exc_info=True)
        self.update_state(state='FAILURE', meta={'error': error_msg})
        # Не переподнимаем исключение, так как возвращаем словарь ошибки
        return {"status": "error", "message": error_msg}

# ... другие задачи (если есть)