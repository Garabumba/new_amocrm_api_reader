from services.CustomFieldsService import CustomFieldsService
from services.LeadService import LeadService
from services.ClickhouseService import ClickhouseService
from services.HTTPService import HTTPService
from services.FileService import FileService
from sys import argv
from datetime import datetime

def get_actual_pipelines(city: str) -> str:
    config_file_service = FileService(f'{city}_config.json')
    json_config = config_file_service.read_json_from_file()
    pipelines = json_config.get('PIPELINES', 'localhost')

    return '&'.join(f'filter[pipeline_id][{position}]={pipeline}' for position, pipeline in enumerate(pipelines.split(',')))

def prepare_data_for_clickhouse(data: list[dict]) -> tuple:
    column_names = list(data[0].keys())
    values = []
    for lead in data:
        values.append(tuple(value for value in lead.values()))

    return column_names, values

def upload_leads(city: str):
    current_time = int(datetime.now().timestamp())
    custom_fields_service = CustomFieldsService(
        '/api/v4/leads/custom_fields', 
        'lead', 
        {
            'id': 0,
            'name': '',
            'price': 0,
            'responsible_user_id': 0,
            'responsible_user': '',
            'created_at': '',
            'created_by': '',
            'updated_at': '',
            'updated_by': '',
            'closed_at': '',
            'tags': '',
            'pipeline': '',
            'status_id': '',
            'etap_sdelki': '',
            'pipeline': '',
            'custom_fields': []
        },
        city,
        'custom_fields_service'
    )
    lead_fields = custom_fields_service.get_fields()
    lead_service = LeadService('amo_clickhouse_fields_compiliance_config.json')
    logs_file_service = FileService(f'{city}_LoadLeadsLogs')
    clickhouse_service = ClickhouseService()
    try:
        logs_file_service.write_log_file(f'Удаляем таблицу: {city}')
        clickhouse_service.drop_table(city)
        logs_file_service.write_log_file(f'Удалили таблицу: {city}')
    except Exception as ex:
        logs_file_service.write_log_file(f'Ошибка удаления таблицы: {city}')

    try:
        logs_file_service.write_log_file(f'Получаем колонки для таблицы: {city}')
        columns_for_db = lead_service.get_columns_names_and_types(lead_fields)
        logs_file_service.write_log_file(f'Получили колонки для таблицы: {city}')
    except Exception as ex:
        logs_file_service.write_log_file(f'Ошибка получения колонок для таблицы: {city}')

    try:
        logs_file_service.write_log_file(f'Создаём таблицу: {city}')
        clickhouse_service.create_table(city, columns_for_db)
        logs_file_service.write_log_file(f'Создали таблицу: {city}')
    except Exception as ex:
        logs_file_service.write_log_file(f'Ошибка создания таблицы: {city}')
    
    http_service = HTTPService(city)
    data_list = []
    page = 1
    while True:
        try:
            pipelines = get_actual_pipelines(city)
            #url = f'/api/v4/leads?limit=250&with=source_id,catalog_elements,contacts,loss_reason&{pipelines}&filter[created_at][to]={current_time}&filter[updated_at][to]={current_time}&page={page}'
            url = f'/api/v4/leads?limit=250&with=source_id,catalog_elements,contacts,loss_reason&filter[created_at][to]={current_time}&filter[updated_at][to]={current_time}&page={page}'
            status_code, response = http_service.execute_request(url, use_cache=False)
            
            if status_code == 200:    
                embedded = response.get('_embedded', {})
                leads = embedded.get('leads', [])
                for lead in leads:
                    lead_service = LeadService(fields=lead_fields)
                    data_list.append(lead_service.process_lead(lead))

                if len(data_list) >= 1000:
                    try:
                        logs_file_service.write_log_file(f'Подготавливаем данные для вставки:\n{data_list} в таблицу: {city}')
                        column_names, data = prepare_data_for_clickhouse(data_list)
                        logs_file_service.write_log_file(f'Подготовили данные для вставки в таблицу: {city}')
                    except Exception as ex:
                        logs_file_service.write_log_file(f'Ошибка подготовки данных для вставки в таблицу {city}: {ex}')
                    
                    clickhouse_service.insert_leads(city, data, column_names)
                    data.clear()
                    data_list.clear()

                page += 1
            elif status_code == 204:
                if len(data_list) > 0:
                    try:
                        logs_file_service.write_log_file(f'Подготавливаем данные для вставки:\n{data_list} в таблицу: {city}')
                        column_names, data = prepare_data_for_clickhouse(data_list)
                        logs_file_service.write_log_file(f'Подготовили данные для вставки в таблицу: {city}')
                    except Exception as ex:
                        logs_file_service.write_log_file(f'Ошибка подготовки данных для вставки в таблицу {city}: {ex}')
                    
                    clickhouse_service.insert_leads(city, data, column_names)
                    data.clear()
                    data_list.clear()
                break
            else:
                break
        except Exception as ex:
            logs_file_service.write_log_file(f'Ошибка запроса "{url}": {ex}')
            break

if __name__ == '__main__':
    try:
        script, city = argv
        #city = 'spb'
        if city in ['msc', 'spb']:
            upload_leads(city)
        else:
            print('Unknown city')
    except:
        print('No city parameter')