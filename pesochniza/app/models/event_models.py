# app/models/event_models.py
from dataclasses import dataclass
from typing import List, Any, Dict # Добавляем Dict для to_dict

@dataclass
class BaseEvent:
    id: int
    batch_id: int
    user_id: str
    timestamp: str
    event_type: str
    record_id: str
    related_file: str
    log_record_counter: int
    event_context: str
    environment: str

    @staticmethod
    def from_dict(obj: Any) -> 'BaseEvent':
        try:
            # Используем .get с дефолтным значением для безопасности
            _id = int(obj.get("id", -1)) # Default to -1 or some indicator of missing
            _batch_id = int(obj.get("batch_id", -1))
            _user_id = str(obj.get("user_id", ""))
            _timestamp = str(obj.get("timestamp", ""))
            _event_type = str(obj.get("event_type", ""))
            _record_id = str(obj.get("record_id", ""))
            _related_file = str(obj.get("related_file", ""))
            _log_record_counter = int(obj.get("log_record_counter", -1))
            _event_context = str(obj.get("event_context", ""))
            _environment = str(obj.get("environment", ""))
            return BaseEvent(_id, _batch_id, _user_id, _timestamp, _event_type, _record_id, _related_file, _log_record_counter, _event_context, _environment)
        except (ValueError, TypeError) as e:
            # Логируем ошибку и поднимаем исключение, чтобы задача Celery провалилась
            print(f"Error parsing BaseEvent from dict: {obj}. Error: {e}")
            raise ValueError(f"Could not parse BaseEvent from dict: {obj}") from e

    def to_dict(self) -> Dict[str, Any]:
        """Преобразует объект BaseEvent в словарь."""
        return self.__dict__


@dataclass
class AudioEvent:
     # Определите поля AudioEvent здесь, если они есть в вашем JSON.
     # Если AudioEvent может быть пустым объектом или не имеет критически важных полей
     # для вашей логики обработки, оставьте его минимальным, но лучше
     # соответствовать реальной структуре.
     # Пример с одним полем:
     id: int = -1
     # audio_file_path: str = "" # Пример другого поля

     @staticmethod
     def from_dict(obj: Any) -> 'AudioEvent':
         try:
             # Пример парсинга поля id
             _id = int(obj.get("id", -1))
             # ... парсинг других полей AudioEvent
             return AudioEvent(_id) # Верните объект с распарсенными полями
         except (ValueError, TypeError) as e:
             print(f"Error parsing AudioEvent from dict: {obj}. Error: {e}")
             raise ValueError(f"Could not parse AudioEvent from dict: {obj}") from e

     def to_dict(self) -> Dict[str, Any]:
         """Преобразует объект AudioEvent в словарь."""
         return self.__dict__


@dataclass
class Root:
    base_events: List[BaseEvent]
    audio_events: List[AudioEvent]

    @staticmethod
    def from_dict(obj: Any) -> 'Root':
        _base_events = []
        # Проверяем, что ключ существует и является списком перед итерацией
        if isinstance(obj.get("base_events"), list):
             # Используем from_dict для каждого элемента
             _base_events = [BaseEvent.from_dict(y) for y in obj["base_events"] if isinstance(y, dict)]

        _audio_events = []
        # Проверяем, что ключ существует и является списком перед итерацией
        if isinstance(obj.get("audio_events"), list):
             # Используем from_dict для каждого элемента
             _audio_events = [AudioEvent.from_dict(y) for y in obj["audio_events"] if isinstance(y, dict)]

        return Root(_base_events, _audio_events)

    def to_dict(self) -> Dict[str, Any]:
        """Преобразует объект Root в словарь, пригодный для сериализации."""
        return {
            "base_events": [event.to_dict() for event in self.base_events],
            "audio_events": [event.to_dict() for event in self.audio_events],
        }