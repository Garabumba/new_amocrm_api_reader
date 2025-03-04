import json
from datetime import datetime

class FileService:
    def __init__(self, file_path):
        self.file_path = file_path

    def read_json_from_file(self):
        try:
            with open(self.file_path, 'r', encoding='utf-8') as file:
                return json.load(file)
        except FileNotFoundError:
            error_file = self.file_path
            self.file_path = 'FileServiceErrors'
            self.write_log_file(f'Файл {error_file} не найден')
            return None
        except json.JSONDecodeError:
            error_file = self.file_path
            self.file_path = 'FileServiceErrors'
            self.write_log_file(f'Ошибка чтения JSON из файла {error_file}. Проверьте формат')
            return None

    def save_json_in_file(self, data):
        try:
            with open(self.file_path, 'w', encoding='utf-8') as file:
                json.dump(data, file, ensure_ascii=False, indent=4)
        except TypeError as e:
            print(f"Ошибка записи JSON: {e}")

    def write_log_file(self, message):
        original_file_path = self.file_path
        log_file_path = f"logs/{original_file_path}_{datetime.now().strftime('%d%m%Y')}.txt"

        with open(log_file_path, 'a', encoding='utf-8') as log_file:
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            log_file.write(f"[{timestamp}] {message}\n")