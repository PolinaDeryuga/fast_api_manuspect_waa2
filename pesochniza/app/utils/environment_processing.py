# app/utils/environment_processing.py

import json
import re
import os
from urllib.parse import urlparse
import pandas as pd # Добавляем импорт pandas, т.к. extract_environment_info использует dicts, которые pandas.Series может обрабатывать

# --- Константы ---
SEPARATORS = ['::', ' - ', ' | ', ' — ', ' – ']
URL_PATTERN = r'https?://[^\s]+|www\.[^\s]+|[^\s]+\.(com|ru|org|net|edu|gov|io)[^\s]*'
BROWSER_INDICATORS = {
    'Chrome_WidgetWin_1': 'Google Chrome',
    'MozillaWindowClass': 'Firefox',
    'Edge': 'Microsoft Edge',
}
BROWSER_NAMES = [
    'Google Chrome', 'Chrome', 'Firefox', 'Mozilla Firefox',
    'Microsoft Edge', 'Edge', 'Safari', 'Opera'
]

# --- Вспомогательные функции ---

def clean_and_shorten_url(url):
    """
    Очищает и сокращает URL, оставляя только домен и основную часть пути.
    """
    if not url:
        return ''

    try:
        parsed_url = urlparse(url)
        domain = parsed_url.netloc
        path_parts = parsed_url.path.split('/')
        short_path = '/'.join(path_parts[:3]) if path_parts else ''
        if domain:
            domain = re.sub(r'^www\.', '', domain, flags=re.IGNORECASE)
            if short_path and short_path != '/':
                return f"{domain}{short_path[:50]}"
            return domain
        return short_path
    except:
        return url

def clean_and_shorten(text, is_url=False):
    """
    Очищает строку от URL и сокращает её, удаляя лишние детали.
    """
    if not text:
        return ''

    cleaned_text = text.strip()

    if is_url or re.search(URL_PATTERN, cleaned_text, flags=re.IGNORECASE):
         return clean_and_shorten_url(cleaned_text)

    cleaned_text = re.sub(URL_PATTERN, '', cleaned_text, flags=re.IGNORECASE)
    cleaned_text = re.sub(r'\?.*$', '', cleaned_text)
    cleaned_text = cleaned_text.strip()

    if '\\' in cleaned_text or '/' in cleaned_text:
        parts = cleaned_text.replace('\\', '/').split('/')
        if len(parts) > 2 or (len(parts) == 2 and parts[0] != ''):
             cleaned_text = '/'.join(parts[-2:])

    return cleaned_text

def remove_browser_names(text, browser_names):
    """
    Удаляет названия браузеров из строки.
    """
    if not text:
        return ''

    cleaned_text = text
    browser_patterns = [re.escape(name) for name in browser_names]
    pattern = rf'\s*[{re.escape("".join(SEPARATORS))}\s]*({"|".join(browser_patterns)})$'
    cleaned_text = re.sub(pattern, '', cleaned_text, flags=re.IGNORECASE).strip()

    pattern_middle = rf'[{re.escape("".join(SEPARATORS))}]\s*({"|".join(browser_patterns)})\s*[{re.escape("".join(SEPARATORS))}]'
    cleaned_text = re.sub(pattern_middle, ' - ', cleaned_text, flags=re.IGNORECASE).strip()

    cleaned_text = re.sub(rf'[{re.escape("".join(SEPARATORS))}]\s*[{re.escape("".join(SEPARATORS))}]', ' - ', cleaned_text).strip()
    cleaned_text = re.sub(rf'^[{re.escape("".join(SEPARATORS))}\s]+', '', cleaned_text).strip()
    cleaned_text = re.sub(rf'[{re.escape("".join(SEPARATORS))}\s]+$', '', cleaned_text).strip()

    return cleaned_text.strip()

def extract_environment_info(environment_str: str):
    """
    Извлекает информацию из строки environment.
    Возвращает список словарей с информацией об окнах или None при ошибке.
    """
    if not isinstance(environment_str, str) or not environment_str.strip():
        return []

    try:
        environment_data = json.loads(environment_str)
        results = []

        mouse_x = environment_data.get('mouse_x', None)
        mouse_y = environment_data.get('mouse_y', None)
        modifiers = environment_data.get('modifiers', None)
        timestamp = environment_data.get('timestamp', None)

        if isinstance(environment_data, dict) and 'log_windows' in environment_data:
            log_windows = sorted(environment_data['log_windows'], key=lambda x: x.get('z_index', 0), reverse=True)

            for window in log_windows:
                if isinstance(window, dict):
                    program_title = window.get('program_title', '').strip()
                    classname = window.get('classname', '').strip()
                    process_path = window.get('process_path', '').strip()
                    is_active = window.get('is_active', False)
                    z_index = window.get('z_index', 0)

                    root_app = ''
                    tab_title = ''
                    is_browser = False

                    detected_browser_name = None
                    for indicator, app_name in BROWSER_INDICATORS.items():
                        if indicator in classname or (process_path and indicator.lower() in process_path.lower()):
                            is_browser = True
                            detected_browser_name = app_name
                            break

                    if is_browser:
                        root_app = detected_browser_name or 'Браузер'
                        cleaned_title = remove_browser_names(program_title, BROWSER_NAMES)
                        tab_title = clean_and_shorten(cleaned_title, is_url=True)
                    else:
                        if '\\' in program_title or '/' in program_title:
                            file_name = os.path.basename(program_title)
                            specific_part = re.sub(r'\.(exe|bat|dll|py|sh|txt|doc|docx|pdf|xlsx|pptx|jpg|png|gif|mp4|mp3|zip|rar|7z)$', '', file_name, flags=re.IGNORECASE).strip()
                            dir_path = os.path.dirname(program_title)

                            root_app_candidate = dir_path or file_name
                            root_app = clean_and_shorten(root_app_candidate, is_url=False)

                            tab_title = specific_part if specific_part else ''
                        else:
                            last_separator = None
                            last_pos = -1
                            for sep in SEPARATORS:
                                pos = program_title.rfind(sep)
                                if pos > last_pos:
                                    last_pos = pos
                                    last_separator = sep

                            if last_separator and last_pos > 0:
                                tab_title_candidate = program_title[:last_pos].strip()
                                root_app_candidate = program_title[last_pos + len(last_separator):].strip()

                                root_app = clean_and_shorten(root_app_candidate)
                                tab_title = clean_and_shorten(tab_title_candidate)
                            else:
                                root_app = clean_and_shorten(program_title)
                                tab_title = ''

                    cleaned_process_path = clean_and_shorten(process_path, is_url=False)

                    if not root_app and cleaned_process_path:
                         root_app = os.path.basename(cleaned_process_path)
                         root_app = re.sub(r'\.(exe|bat|dll)$', '', root_app, flags=re.IGNORECASE).strip()
                         if not root_app and classname:
                              root_app = classname

                    result = {
                        'program_title': program_title,
                        'root_app': root_app or 'Неизвестно',
                        'tab_title': tab_title,
                        'classname': classname,
                        'process_path': cleaned_process_path,
                        'is_active': is_active,
                        'z_index': z_index,
                        'window_left': window.get('window_left', None),
                        'window_top': window.get('window_top', None),
                        'window_right': window.get('window_right', None),
                        'window_bottom': window.get('window_bottom', None),
                        'mouse_x': mouse_x,
                        'mouse_y': mouse_y,
                        'modifiers': modifiers,
                        'timestamp': timestamp,
                        # Добавьте другие поля из env_info, если они нужны на уровне окна после explode
                        # Например, ID пользователя, ID события и т.п.
                        # Для этого вам нужно будет передать их в эту функцию или объединить позже
                        # Пока предполагаем, что эти поля будут в DataFrame до explode
                    }
                    results.append(result)
        return results

    except json.JSONDecodeError:
        print(f"Ошибка парсинга JSON: {environment_str[:200]}...")
        return None
    except Exception as e:
        print(f"Неожиданная ошибка при обработке environment: {e}")
        print(f"Проблемная строка: {environment_str[:200]}...")
        return None
def save_to_csv(df: pd.DataFrame, filename: str, base_path: str):
    """
    Сохраняет DataFrame в CSV файл на Google Диске.

    Args:
        df: DataFrame для сохранения.
        filename: Имя файла для сохранения.
        base_path: Путь к папке на Google Диске, куда нужно сохранить файл.
    """
    full_path = os.path.join(base_path, filename)  # Полный путь к файлу
    try:
        df.to_csv(full_path, index=False)  # index=False чтобы не сохранять индексы строк
        print(f"DataFrame успешно сохранен в файл: {full_path}")
    except Exception as e:
        print(f"Ошибка при сохранении файла: {e}")
save_to_csv(data, "data2.csv", base_path)