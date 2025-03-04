from clickhouse_connect import get_client
from .FileService import FileService

class ClickhouseService():
    def __init__(self):
        self.__init_data_for_connection()
        self.client = get_client(host=self.host, port=self.port, username=self.username, password=self.password)
        self.logs_file_service = FileService('ClickhouseService')
    
    def __init_data_for_connection(self):
        config_file_service = FileService('db_config.json')
        config = config_file_service.read_json_from_file()

        self.host = config.get('HOST', 'localhost')
        self.port = config.get('PORT', 8123)
        self.local_port = config.get('LOCAL_PORT', 9000)
        self.username = config.get('USERNAME', 'default')
        self.password = config.get('PASSWORD', '')
        self.db_name = config.get('DB_NAME', 'miatest')

    def __get_table_columns(self, table_name: str):
        query = f"""
            WITH
                (SELECT groupArray(name) 
                FROM 
                (SELECT name 
                    FROM system.columns 
                    WHERE database = '{self.db_name}' AND table = 'msc'
                    UNION DISTINCT
                    SELECT name 
                    FROM system.columns 
                    WHERE database = '{self.db_name}' AND table = 'spb')) AS all_columns,
                (SELECT groupArray(name) 
                FROM 
                (SELECT name 
                    FROM system.columns 
                    WHERE database = '{self.db_name}' AND table = 'msc')) AS msc_columns,
                (SELECT groupArray(name) 
                FROM 
                (SELECT name 
                    FROM system.columns 
                    WHERE database = '{self.db_name}' AND table = 'spb')) AS spb_columns,
                (SELECT arrayMap((x, y) -> (if(has(msc_columns, x), x, concat('NULL AS ', y))), all_columns, all_columns)) as msc,
                (SELECT arrayMap((x, y) -> (if(has(spb_columns, x), x, concat('NULL AS ', y))), all_columns, all_columns)) as spb
            SELECT arrayStringConcat(arrayMap(x -> x, {table_name}), ',')
        """
        
        result = self.client.command(query)
        return result
    
    def __recreate_compinded_table(self, combined_table):
        msc_columns = self.__get_table_columns('msc')
        spb_columns = self.__get_table_columns('spb')
        all_columns = self.__get_table_columns('all_columns')

        msc_columns = msc_columns.split(',')
        spb_columns = spb_columns.split(',')
        all_columns = all_columns.split(',')

        msc_query = 'SELECT '
        spb_query = 'SELECT '
        for column in all_columns:
            if column in msc_columns:
                msc_query += f'{column},'
            else:
                msc_query += f'NULL AS {column},'
        
        for column in all_columns:
            if column in spb_columns:
                spb_query += f'{column},'
            else:
                spb_query += f'NULL AS {column},'
        
        #query = f"""DROP TABLE IF EXISTS {combined_table}"""
        #self.client.command(query)
        query = f"""
            CREATE TABLE {self.db_name}.{combined_table}
            ENGINE=MergeTree()
            ORDER BY coalesce(id, 0)
            AS
            {msc_query[:len(msc_query) - 1]} FROM {self.db_name}.msc
            UNION ALL
            {spb_query[:len(spb_query) - 1]} FROM {self.db_name}.spb
        """
        self.client.command(query)

    def create_table(self, table_name: str, fields_with_types: list, engine: str='MergeTree', primary_keys: list=None):
        if not primary_keys or len(primary_keys) < 1:
            primary_keys = 'tuple()'
        else:
            primary_keys = f'({",".join(primary_keys)})'

        #print(f"""CREATE TABLE IF NOT EXISTS {self.db_name}.{table_name} ({','.join(fields_with_types)}) ENGINE = {engine}() ORDER BY {primary_keys}""")
        self.client.command(f"""CREATE TABLE IF NOT EXISTS {self.db_name}.{table_name} ({','.join(fields_with_types)}) ENGINE = {engine}() ORDER BY {primary_keys}""")

    def insert_leads(self, table_name: str, values: list[tuple], column_names: list):
        batch_size = 1000
        try:
            for i in range(0, len(values), batch_size):
                self.client.insert(f'{self.db_name}.{table_name}', values[i:i+batch_size], column_names=column_names)
        except Exception as ex:
            self.logs_file_service.write_log_file(f'Ошибка вставки данных {values[i:i+batch_size]} в таблицу {table_name}: {ex}')

    def drop_table(self, table_name: str):
        self.client.command(f"""DROP TABLE IF EXISTS {self.db_name}.{table_name}""")

    def create_combined_table(self, table_name1: str, table_name2: str):
        try:
            self.__recreate_compinded_table(f'{table_name1}_{table_name2}')
        except Exception as ex:
            self.logs_file_service.write_log_file(f'Ошибка создания таблицы {table_name1}_{table_name2}: {ex}')

    def get_calculations_for_users(self, payments_count: int):
        query = f"""
            WITH
                all_payments AS ({' UNION ALL '.join(f"""SELECT lh.responsible_user_of_summa_oplaty_{i}, if(ms.lead_Data_oplaty_{i} >= toStartOfMonth(now()) AND ms.lead_Data_oplaty_{i} <= now(), ms.lead_Summa_oplaty_{i}, 0) AS oplata FROM test_db.leads_history AS lh LEFT JOIN test_db.msc_spb AS ms ON lh.lead_id = ms.id AND lh.lead_Klinika = ms.lead_Klinika""" for i in range(1, payments_count + 1))}),
                month_leads_payments AS ({' UNION ALL '.join(f"""SELECT lh.responsible_user_of_summa_oplaty_{i}, if(ms.lead_Data_oplaty_{i} >= toStartOfMonth(now()) AND ms.lead_Data_oplaty_{i} <= now() AND ms.lead_Data_oplaty_{i} >= ms.lead_Data_1oj_konsultatsii, ms.lead_Summa_oplaty_{i}, 0) AS oplata FROM test_db.leads_history AS lh LEFT JOIN test_db.msc_spb AS ms ON lh.lead_id = ms.id AND lh.lead_Klinika = ms.lead_Klinika WHERE ms.lead_Data_1oj_konsultatsii >= toStartOfMonth(now()) AND ms.lead_Data_1oj_konsultatsii <= now()""" for i in range(1, payments_count + 1))}),
                final_all_payments AS (
                    SELECT 
                        ru.name, 
                        SUM(oplata) AS total_sum
                    FROM 
                        all_payments
                        LEFT JOIN
                        test_db.responsible_users AS ru 
                        ON
                            responsible_user_of_summa_oplaty_1 = ru.id
                    GROUP BY ru.name
                ),
                final_month_leads_payments AS (
		            SELECT 
			            ru.name, 
			            SUM(oplata) AS total_sum 
		            FROM 
			            month_leads_payments
			        LEFT JOIN
			            test_db.responsible_users AS ru 
			        ON
				        responsible_user_of_summa_oplaty_1 = ru.id
		            GROUP BY ru.name
	            ),
                monthly_leads_count AS 
                    (SELECT
                        ru.name AS responsible_user,
                        COUNT(ru.name) AS leads_count
                    FROM
                        test_db.msc_spb
                    LEFT JOIN
                        test_db.responsible_users AS ru
                        ON
                            responsible_user_id = ru.id
                    WHERE
                        (lead_Data_1oj_konsultatsii >= toStartOfMonth(now())
                        AND lead_Data_1oj_konsultatsii <= now())
                    GROUP BY
                        ru.name
                ),
                summs_count AS
                    (SELECT
                        ru.name AS responsible_user,
                        COUNT(ru.name) AS s_count
                    FROM
                        test_db.msc_spb
                    LEFT JOIN
                        test_db.responsible_users AS ru 
                        ON
                            responsible_user_id = ru.id
                    WHERE
                        (lead_Summa_oplaty_1 <> 0 AND lead_Summa_oplaty_1 IS NOT NULL)
                        AND
                        (lead_Data_oplaty_1 >= toStartOfMonth(now()) AND lead_Data_oplaty_1 <= now())
                        AND
                        (lead_Data_1oj_konsultatsii >= toStartOfMonth(now()) AND lead_Data_1oj_konsultatsii <= now())
                    GROUP BY
                        ru.name
                )
            SELECT DISTINCT 
                ru.name AS responsible_user,
                ifNull(ap.total_sum, 0) AS total_sum,
                if(ifNull(mlc.leads_count, 0) = 0, 0, FLOOR(ifNull(fmlp.total_sum, 0) / ifNull(mlc.leads_count, 0), 0)) AS two,
                if(ifNull(mlc.leads_count, 0) = 0, 0, FLOOR(ifNull(sc.s_count, 0) / ifNull(mlc.leads_count, 0) * 100, 0)) AS conversion,
                if(ifNull(sc.s_count, 0) = 0, 0, FLOOR(ifNull(fmlp.total_sum, 0) / ifNull(sc.s_count, 0), 0)) AS srednii_check
            FROM
                test_db.responsible_users AS ru
            LEFT JOIN
                final_all_payments AS ap
                ON
                    ru.name = ap.name
            LEFT JOIN
                final_month_leads_payments AS fmlp
                ON
                    ru.name = fmlp.name
            LEFT JOIN
                monthly_leads_count AS mlc
                ON
                    ru.name = mlc.responsible_user
            LEFT JOIN
                summs_count AS sc
                ON
                    ru.name = sc.responsible_user
        """

        return self.client.query(query).result_rows