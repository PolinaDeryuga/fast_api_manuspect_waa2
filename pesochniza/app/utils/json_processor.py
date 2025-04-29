# app/utils/json_processor.py
import os
import json
import pandas as pd # <-- Импортируем pandas
from typing import List, Any, Dict # <-- Добавляем Dict
from app.models.event_models import BaseEvent, AudioEvent, Root # Импортируем ваши модели

# 3. Функция для рекурсивного поиска JSON файлов
def find_json_files(root_path: str) -> List[str]:
    """
    Рекурсивно находит все JSON файлы в заданной директории и её поддиректориях,
    соответствующие шаблону "batch-*".

    Args:
        root_path: Путь к корневой директории для поиска.

    Returns:
        Список путей к найденным JSON файлам.
    """
    json_files = []
    if not os.path.isdir(root_path):
        print(f"Error: Directory not found: {root_path}")
        return json_files # Return empty list if path is invalid

    for dirpath, dirnames, filenames in os.walk(root_path):
        # Проверяем, соответствует ли текущая директория шаблону "batch-*"
        # Используем os.path.basename для получения только имени папки
        if os.path.basename(dirpath).startswith("batch-"):
            for filename in filenames:
                if filename.endswith(".json"):
                    full_path = os.path.join(dirpath, filename)
                    # Дополнительная проверка на существование файла (хотя os.walk обычно гарантирует это)
                    if os.path.exists(full_path):
                         json_files.append(full_path)
    return json_files


# 4. Основная логика обработки JSON файлов (парсинг в Root объекты)
def parse_json_files_to_roots(base_path: str) -> List[Root]:
    """
    Находит все JSON файлы в подпапках "batch-*" внутри заданной директории,
    загружает их и парсит с использованием классов BaseEvent и Root.

    Args:
        base_path:  Базовый путь к папке, содержащей "Manuspect/logs/EventLogger".

    Returns:
        Список всех объектов Root, полученных из всех обработанных JSON файлов.
    """

    all_roots = []  # Список для хранения всех объектов Root

    # Формируем полный путь к папке EventLogger
    # Убедитесь, что этот путь верен относительно base_path
    event_logger_path = os.path.join(base_path, "Manuspect", "logs", "EventLogger")
    print(f"Searching for JSON files in: {event_logger_path}")

    # Находим все JSON файлы
    json_file_paths = find_json_files(event_logger_path)

    if not json_file_paths:
        print(f"No JSON files found in {event_logger_path} matching pattern 'batch-*'.")
        return all_roots # Return empty list

    print(f"Found {len(json_file_paths)} JSON files to process.")

    # Обрабатываем каждый JSON файл
    for json_file_path in json_file_paths:
        try:
            # Используем 'with' для автоматического закрытия файла
            # Используем 'rb' и игнорируем ошибки, если есть проблемы с кодировкой или BOM
            # Или просто 'r' с 'utf-8' и try/except
            # Попробуем стандартный подход сначала:
            with open(json_file_path, 'r', encoding='utf-8') as f:
                json_content = f.read()
                # Проверка на пустой файл
                if not json_content.strip():
                     print(f"Warning: File {json_file_path} is empty, skipping.")
                     continue

                json_data = json.loads(json_content)  # Загружаем JSON данные

                # Проверка, что json_data является словарем (ожидаемый формат Root)
                if not isinstance(json_data, dict):
                    print(f"Warning: File {json_file_path} does not contain a JSON object at the root, skipping.")
                    continue

                root = Root.from_dict(json_data)       # Создаем объект Root
                all_roots.append(root)                 # Добавляем в список
                print(f"File {json_file_path} successfully parsed into Root object.")

        except FileNotFoundError:
            print(f"Error: File not found: {json_file_path}")
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON in file {json_file_path}: {e}")
        except (KeyError, TypeError, ValueError) as e: # Ловим ошибки парсинга из from_dict
            print(f"Schema or parsing error in file {json_file_path}: {e}")
        except Exception as e:
             print(f"Unexpected error processing {json_file_path}: {e}")

    print(f"\nTotal Root objects parsed: {len(all_roots)}")

    # Возвращаем список объектов Root
    return all_roots


# 5. Функция для создания DataFrame
def create_dataframe(all_roots: List[Root]) -> pd.DataFrame: # Указываем тип List[Root]
    """
    Создает DataFrame из списка объектов Root, извлекая данные из BaseEvent.

    Args:
        all_roots: Список объектов Root, полученных из JSON файлов.

    Returns:
        pd.DataFrame: DataFrame, содержащий данные из BaseEvent.  Если данных нет,
                      возвращается пустой DataFrame.
    """

    all_events_data = []  # Список для хранения данных всех событий (в формате словарей)

    for root in all_roots:
        # Используем метод to_dict() BaseEvent для получения словаря
        all_events_data.extend([base_event.to_dict() for base_event in root.base_events])

    if not all_events_data:
        print("Нет данных из BaseEvent для создания DataFrame.")
        # Возвращаем пустой DataFrame с ожидаемыми колонками, если возможно
        # (можно определить колонки статически или по первому событию, если оно есть)
        # Для простоты, возвращаем пустой DF без колонок, если данных нет вообще.
        return pd.DataFrame()

    # Создаем DataFrame из списка словарей
    df = pd.DataFrame(all_events_data)

    print(f"\nDataFrame created with {len(df)} rows.")
    return df