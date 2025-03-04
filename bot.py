import asyncio
from datetime import datetime, timedelta
from clickhouse_connect import get_async_client
from aiogram import Bot
from aiogram.enums.parse_mode import ParseMode
from services.FileService import FileService
from services.ClickhouseService import ClickhouseService
import os
import csv
import aiofiles

async def read_users_from_csv(file_path):
    clinics_dict = {}

    async with aiofiles.open(file_path, mode='r') as file:
        reader = csv.reader((await file.read()).splitlines(), delimiter=';')
        next(reader)

        for user, clinic, chat_id, thread_id, monthly_plan in reader:
            clinic_data = clinics_dict.setdefault(clinic, {'chat_id': int(chat_id), 'thread_id': int(thread_id) if thread_id != '' else None, 'users': []})
            clinic_data['users'].append({'user': user, 'monthly_plan': float(monthly_plan), 'nakop_viruchka_s_pervichek': 0, 'viruchka_na_pervichky': 0, 'conversion': 0, 'srednii_check': 0})

    return clinics_dict

async def load_config(file_service):
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, file_service.read_json_from_file)

async def create_all_users_query(needed_users):
    query = ''
    query = f"""SELECT '{needed_users[0]}' AS responsible_user UNION ALL SELECT '{"' UNION ALL SELECT '".join(needed_users[1:])}'"""
    return query

def process_query_results(rows, users, rows2):
    data = []

    for row in rows.result_rows:
        for row2 in rows2.result_rows:
            if row[0] == row2[0]:
                user_name = row[0]
                nakop_viruchka_s_pervichek = row2[1]
                viruchka_na_pervichky = row[2]
                conversion = row[3]
                srednii_check = row[4]

                #total_summa = row[7] or 0
                current_user = next((u for u in data if u['name'] == user_name), None)
                if not current_user:
                    current_user = {
                        'name': user_name,
                        'nakop_viruchka_s_pervichek': 0,
                        'viruchka_na_pervichky': 0,
                        'conversion': 0,
                        'srednii_check': 0
                    }
                    data.append(current_user)

                current_user.update({
                    'nakop_viruchka_s_pervichek': nakop_viruchka_s_pervichek,
                    'viruchka_na_pervichky': viruchka_na_pervichky,
                    'conversion': conversion,
                    'srednii_check': srednii_check,
                })

    return data

def format_number(number) -> str:
    try:
        num = int(number) if number else 0

        formatted_number = f"{num:,}".replace(",", " ")
        return formatted_number
    except ValueError:
        return "Ошибка: введено некорректное число"

def format_user_message(user, current_date):
    return (
        f"<b>{user['user']} ({current_date.strftime('%d.%m.%Y')})</b>\n"
        f"<i>Накопительная выручка:</i> {format_number(user['nakop_viruchka_s_pervichek'])} ₽\n"
        f"<i>Выручка на первичку:</i> {format_number(user['viruchka_na_pervichky'])} ₽\n"
        f"<i>Конверсия с первичек:</i> {format_number(user['conversion'])}%\n"
        f"<i>Средний чек с первичек:</i> {format_number(user['srednii_check'])} ₽\n"
    )

async def send_message(bot: Bot, chat_id, message, logs_file_service, thread_id = None):
    try:
        if thread_id is not None:
            await bot.send_message(chat_id, message_thread_id=thread_id, text=message, parse_mode=ParseMode.HTML)
        else:
            await bot.send_message(chat_id, text=message, parse_mode=ParseMode.HTML)
    except Exception as ex:
        logs_file_service.write_log_file(
            f"Ошибка отправки сообщения: {ex}"
        )

def format(users, result):
    for clinic in users:
        for user in users[clinic].get('users', []):
            for item in result:
                if user.get('user', '') == item[0]:
                    user['nakop_viruchka_s_pervichek'] = item[1]
                    user['viruchka_na_pervichky'] = item[2]
                    user['conversion'] = item[3]
                    user['srednii_check'] = item[4]

async def main():
    try:
        parent_dir = os.path.abspath(os.path.join(os.getcwd(), os.pardir))
        csv_file_path = os.path.join(parent_dir, 'leads_csv/users.csv')

        bot_config_file_service = FileService('bot_config.json')
        db_config_file_service = FileService('db_config.json')
        logs_file_service = FileService('BotLogs')
        clickhouse_service = ClickhouseService()

        bot_json_config = await load_config(bot_config_file_service)
        db_config_json = await load_config(db_config_file_service)

        bot_token = bot_json_config['BOT_TOKEN']
        db_config = {
            'host': db_config_json['HOST'],
            'port': db_config_json['PORT'],
            'username': db_config_json['USERNAME'],
            'password': db_config_json['PASSWORD'],
            'database': db_config_json['DB_NAME'],
        }

        users_table = f"{db_config_json['DB_NAME']}.users"
        combined_table = f"{db_config_json['DB_NAME']}.msc_spb"
        client = await get_async_client(**db_config)
        clinic_users = await read_users_from_csv(csv_file_path)
        rows = clickhouse_service.get_calculations_for_users(25)
        current_date = datetime.today().date()
        format(clinic_users, rows)
        
        async with Bot(token=bot_token) as bot:
            tasks = []
            for clinic, value in clinic_users.items():
                users = value['users']
                users_array = [u['user'] for u in users]
                #all_users_query = await create_all_users_query(users_array)
                #rows = clickhouse_service.get_calculations_for_users(25)
                #for user in users_array:
                #    pass
                #data = process_query_results(rows, users)
                for user in users:
                    message = format_user_message(user, current_date)
                    chat_id = value['chat_id']
                    thread_id = value['thread_id']

                    #await send_message(bot, chat_id, message, logs_file_service, thread_id)
                    print(message)
    except Exception as ex:
        logs_file_service.write_log_file(f'Ошибка: {ex}')
    
if __name__ == "__main__":
    asyncio.run(main())