from .FileService import FileService
from transliterate import translit
from datetime import date
import re

class LeadService():
    def __init__(self, path_to_config: str = None, fields: dict = None):
        self.config = FileService(path_to_config)
        self.logs_file_service = FileService('LeadService')
        self.fields = fields

    def __get_clickhouse_type_from_amocrm_type(self, amocrm_type: str) -> str:
        types_compiliance = self.config.read_json_from_file()

        return types_compiliance.get(amocrm_type.upper(), 'Nullable(String)')
    
    def __process_column_name(self, column_name: str) -> str:
        transliterated = translit(column_name, 'ru', reversed=True)
        transliterated = transliterated.replace(' ', '_')
        clean_name = re.sub(r'[^\w_]', '', transliterated)

        return clean_name
    
    def __process_common_fields(self, data: dict):
        self.fields['id'] = data.get('id', 0)
        self.fields['name'] = data.get('name', '')
        self.fields['price'] = data.get('price', 0)
        self.fields['responsible_user_id'] = data.get('responsible_user_id', 0)
        try:
            created_at = date.fromtimestamp(int(data.get('created_at', None)))
        except Exception as ex:
            self.logs_file_service.write_log_file(f'Ошибка преобразования {data.get("created_at", None)}: {ex}')
            created_at = None

        try:
            updated_at = date.fromtimestamp(int(data.get('updated_at', None)))
        except Exception as ex:
            self.logs_file_service.write_log_file(f'Ошибка преобразования {data.get("created_at", None)}: {ex}')
            updated_at = None

        try:
            closed_at = date.fromtimestamp(int(data.get('closed_at', None)))
        except Exception as ex:
            self.logs_file_service.write_log_file(f'Ошибка преобразования {data.get("created_at", None)}: {ex}')
            closed_at = None

        self.fields['created_at'] = created_at
        self.fields['created_by'] = data.get('created_by', 0)
        self.fields['updated_at'] = updated_at
        self.fields['updated_by'] = data.get('updated_by', 0)
        self.fields['closed_at'] = closed_at
        self.fields['tags'] = data.get('tags', '')
        self.fields['pipeline'] = data.get('pipeline', '')
        self.fields['status_id'] = data.get('status_id', 0)
        self.fields['etap_sdelki'] = data.get('etap_sdelki', '')
        self.fields['pipeline'] = data.get('pipeline', '')

    def __convert_value(self, values: list[dict], amo_crm_field_type: str, types_compiliance: dict):
        if not values or len(values) < 1:
            return None

        python_type = types_compiliance.get(amo_crm_field_type.upper(), None)
        if not python_type:
            return None
        
        try:
            if python_type == 'str':
                return ','.join(value_structure.get('value', '') for value_structure in values)
            elif python_type == 'int':
                return int(float(values[0].get('value', None)))
            elif python_type == 'bool':
                return bool(values[0].get('value', None))
            elif python_type == 'datetime':
                return date.fromtimestamp(int(values[0].get('value', None)))
        except Exception as ex:
            self.logs_file_service.write_log_file(f'Не смогли преобразовать {values} к {python_type}: {ex}')
            return None

    def __get_custom_fields_ids_with_values(self, data: dict) -> dict:
        custom_fields_ids_with_values = {}
        file_service = FileService('amo_python_fields_compiliance_config.json')
        types_compiliance = file_service.read_json_from_file()

        for custom_field in data:
            value = self.__convert_value(custom_field.get('values', []), custom_field.get('field_type', []), types_compiliance)
            try:
                id = int(custom_field.get('field_id', 0))
            except Exception as ex:
                self.logs_file_service.write_log_file(f'Ошибка получения id кастомного поля: {id}')
                continue

            custom_fields_ids_with_values[id] = value

        return custom_fields_ids_with_values

    def __process_custom_fields(self, data: list):
        custom_fields_ids_with_values = self.__get_custom_fields_ids_with_values(data)

        for custom_field in self.fields.get('custom_fields', []):
            custom_field['value'] = custom_fields_ids_with_values.get(custom_field['id'], None)

    def __extract_data(self, data) -> dict:
        result = {}
        if isinstance(data, dict):
            for key, value in data.items():
                if key == 'custom_fields' and isinstance(value, list):
                    for field in value:
                        field_name = field.get('name', '')
                        field_value = field.get('values', '')
                        if field_name:
                            result[f"{field_name}"] = field_value
                elif isinstance(value, (dict, list)):
                    result.update(self.__extract_data(value))
                else:
                    result[f'{key}'] = value
        
        elif isinstance(data, list):
            for idx, item in enumerate(data):
                result.update(self.__extract_data(item))
        
        return result

    def get_columns_names_and_types(self, data: dict) -> list:
        column_names_and_types = [
            'id Nullable(Int64)',
            'name Nullable(String)',
            'price Nullable(Int64)',
            'responsible_user_id Nullable(Int64)',
            'created_at Nullable(Date)',
            'created_by Nullable(Int64)',
            'updated_at Nullable(Date)',
            'updated_by Nullable(Int64)',
            'closed_at Nullable(Date)',
            'tags Nullable(String)',
            'pipeline Nullable(String)',
            'status_id Nullable(Int64)',
            'etap_sdelki Nullable(String)'
        ]
        custom_fields = data.get('custom_fields', [])
        for custom_field in custom_fields:
            custom_field_name = custom_field.get('name', '')
            custom_field_type = custom_field.get('custom_field_type', '')
            column_names_and_types.append(f'{self.__process_column_name(custom_field_name)} {self.__get_clickhouse_type_from_amocrm_type(custom_field_type)}')

        return column_names_and_types
    
    def process_lead(self, data: dict) -> dict:
        self.__process_common_fields(data)
        self.__process_custom_fields(data.get('custom_fields_values', []))
        
        prepared_custom_fields_data = {
            'id': self.fields['id'],
            'name': self.fields['name'],
            'price': self.fields['price'],
            'responsible_user_id': self.fields['responsible_user_id'],
            'created_at': self.fields['created_at'],
            'created_by': self.fields['created_by'],
            'updated_at': self.fields['updated_at'],
            'updated_by': self.fields['updated_by'],
            'closed_at': self.fields['closed_at'],
            'tags': self.fields['tags'],
            'pipeline': self.fields['pipeline'],
            'status_id': self.fields['status_id'],
            'etap_sdelki': self.fields['etap_sdelki'],
            'pipeline': self.fields['pipeline']
        }
        for custom_field in self.fields.get('custom_fields', []):
            prepared_custom_fields_data[self.__process_column_name(custom_field.get('name', ''))] = custom_field.get('value', None)
        
        return prepared_custom_fields_data
        