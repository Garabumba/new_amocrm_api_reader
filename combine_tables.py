from services.ClickhouseService import ClickhouseService

def start():
    clickhouse_service = ClickhouseService()
    clickhouse_service.drop_table('msc_spb')
    clickhouse_service.create_combined_table('msc', 'spb')

if __name__ == '__main__':
    start()