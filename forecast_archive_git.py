#!/usr/bin/python3.9
#!/usr/bin/env python
# coding: utf-8

import datetime
import logging
import pathlib
import urllib.parse
import warnings
from sys import platform

import pandas as pd
import pymysql
import requests
import yaml
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

start_time = datetime.datetime.now()
warnings.filterwarnings("ignore")

# Кол-во дней с прогнозами, которые хранятся в основной базе
STORED_DAYS = 40

# Настройки для логера

if platform == "linux" or platform == "linux2":
    logging.basicConfig(
        filename="/var/log/log-execute/log_forecast_archive.txt",
        level=logging.INFO,
        format=(
            "%(asctime)s - %(levelname)s - %(funcName)s: %(lineno)d -"
            " %(message)s"
        ),
    )
elif platform == "win32":
    logging.basicConfig(
        filename=f"{pathlib.Path(__file__).parent.absolute()}/log_forecast_archive.txt",
        level=logging.INFO,
        format=(
            "%(asctime)s - %(levelname)s - %(funcName)s: %(lineno)d -"
            " %(message)s"
        ),
    )

# Загружаем yaml файл с настройками
logging.info("Архивация прогнозов моделей: Старт задания.")
try:
    with open(
        f"{pathlib.Path(__file__).parent.absolute()}/settings.yaml", "r"
    ) as yaml_file:
        settings = yaml.safe_load(yaml_file)
    telegram_settings = pd.DataFrame(settings["telegram"])
    sql_settings = pd.DataFrame(settings["sql_db"])
    pyodbc_settings = pd.DataFrame(settings["pyodbc_db"])
except Exception as e:
    print(f"Архивация прогнозов моделей: Ошибка загрузки файла настроек: {e}")
    logging.error(
        f"Архивация прогнозов моделей: Ошибка загрузки файла настроек: {e}"
    )
logging.info("Архивация прогнозов моделей: Финиш загрузки файла настроек")

# Функция отправки уведомлений в telegram на любое количество каналов
#  (указать данные в yaml файле настроек)


def telegram(i, text):
    msg = urllib.parse.quote(str(text))
    bot_token = str(telegram_settings.bot_token[i])
    channel_id = str(telegram_settings.channel_id[i])

    retry_strategy = Retry(
        total=3,
        status_forcelist=[101, 429, 500, 502, 503, 504],
        method_whitelist=["GET", "POST"],
        backoff_factor=1,
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    http = requests.Session()
    http.mount("https://", adapter)
    http.mount("http://", adapter)

    http.post(
        f"https://api.telegram.org/bot{bot_token}/sendMessage?chat_id={channel_id}&text={msg}",
        timeout=10,
    )


# Функция коннекта к базе Mysql
# (для выбора базы задать порядковый номер числом !!! начинается с 0 !!!!!)


def connection(i):
    host_yaml = str(sql_settings.host[i])
    user_yaml = str(sql_settings.user[i])
    port_yaml = int(sql_settings.port[i])
    password_yaml = str(sql_settings.password[i])
    database_yaml = str(sql_settings.database[i])
    return pymysql.connect(
        host=host_yaml,
        user=user_yaml,
        port=port_yaml,
        password=password_yaml,
        database=database_yaml,
    )


try:
    telegram(1, "Старт архивации прогнозов моделей.")
except Exception as e:
    print(f"Архивация прогнозов моделей: Ошибка отправки в телеграм: {e}")
    logging.error(
        f"Архивация прогнозов моделей: Ошибка отправки в телеграм: {e}"
    )

logging.info("Старт архивации прогнозов моделей.")

# Первый запрос копирует в архив прогнозы старше 40 дней
# Второй запрос удаляет из основной таблицы прогнозы старше 40 дней
try:
    connection_forecast = connection(0)
    with connection_forecast.cursor() as cursor:
        archive_query = (
            "INSERT INTO treid_03.weather_foreca_archive SELECT * FROM"
            " treid_03.weather_foreca WHERE DATE(load_time) <"
            f" DATE_ADD(CURDATE(), INTERVAL -{STORED_DAYS} DAY);"
        )
        cursor.execute(archive_query)
        delete_query = (
            "DELETE FROM treid_03.weather_foreca WHERE DATE(load_time) <"
            f" DATE_ADD(CURDATE(), INTERVAL -{STORED_DAYS} DAY);"
        )
        cursor.execute(delete_query)
    connection_forecast.commit()
    connection_forecast.close()
except Exception as e:
    print(f"Архивация прогнозов моделей: Ошибка запросов к бд: {e}")
    logging.error(f"Архивация прогнозов моделей: Ошибка запросов к бд: {e}")
    telegram(1, f"Ошибка архивации прогнозов моделей: {e}")

try:
    telegram(1, f"Финиш архивации прогнозов моделей.")
    telegram(0, f"Архивация прогнозов моделей завершена.")
except Exception as e:
    print(f"Архивация прогнозов моделей: Ошибка отправки в телеграм: {e}")
    logging.error(
        f"Архивация прогнозов моделей: Ошибка отправки в телеграм: {e}"
    )
logging.info("Финиш архивации прогнозов моделей.")

print("Время выполнения:", datetime.datetime.now() - start_time)
