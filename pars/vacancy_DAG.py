from clickhouse_driver import Client
import dateparser
from concurrent.futures import ThreadPoolExecutor, as_completed
import csv, json
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.log.logging_mixin import LoggingMixin
import logging
from logging import handlers
# from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable
import time
from datetime import datetime, timedelta
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium import webdriver
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.chrome.options import Options as ChromeOptions
import pandas as pd
import numpy as np
import os

# Connections settings
# Загружаем данные подключений из JSON файла
with open('/opt/airflow/dags/config_connections.json', 'r') as config_file:
    connections_config = json.load(config_file)

# Получаем данные конфигурации подключения и создаем конфиг для клиента
conn_config = connections_config['click_connect']
config = {
    'database': conn_config['database'],
    'user': conn_config['user'],
    'password': conn_config['password'],
    'host': conn_config['host'],
    'port': conn_config['port'],
}

client = Client(**config)

# Variables settings
# Загружаем переменные из JSON файла
with open('/opt/airflow/dags/config_variables.json', 'r') as config_file:
    my_variables = json.load(config_file)

Variable.set("shares_variable", my_variables, serialize_json=True)

dag_variables = Variable.get("shares_variable", deserialize_json=True)

url_sber = dag_variables.get('base_sber')
url_yand = dag_variables.get('base_yand')
url_vk = dag_variables.get('base_vk')
url_tin = dag_variables.get('base_tin')
# url_sber = dag_variables['base_sber']
# url_yand = dag_variables['base_yand']
# url_vk = dag_variables['base_vk']
# url_tin = dag_variables['base_tin']

raw_tables = ['raw_vk', 'raw_sber', 'raw_tin']

options = ChromeOptions()

profs = dag_variables['professions']

logging_level = os.environ.get('LOGGING_LEVEL', 'INFO').upper()
logging.basicConfig(level=logging_level)
log = logging.getLogger(__name__)
log_handler = handlers.RotatingFileHandler('/opt/airflow/logs/airflow.log',
                                           maxBytes=5000,
                                           backupCount=5)
log_handler.setLevel(logging.ERROR)
log_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
log_handler.setFormatter(log_formatter)
log.addHandler(log_handler)

# Параметры по умолчанию
default_args = {
    "owner": "admin_1T",
    # 'start_date': days_ago(1),
    'retry_delay': timedelta(minutes=5),
}

# Создаем DAG ручного запуска (инициализирующий режим).
first_raw_dag = DAG(dag_id='first_raw_dag',
                tags=['admin_1T'],
                start_date=datetime(2023, 10, 15),
                schedule_interval=None,
                default_args=default_args)


class DatabaseManager:
    def __init__(self, client, database):
        self.client = client
        self.database = database
        self.raw_tables = ['raw_vk', 'raw_sber', 'raw_tin']
        self.log = LoggingMixin().log

    def create_raw_tables(self):
        for table_name in self.raw_tables:
            drop_table_query = f"DROP TABLE IF EXISTS {self.database}.{table_name};"
            self.client.execute(drop_table_query)
            self.log.info(f'Удалена таблица {table_name}')

            create_table_query = f"""
            CREATE TABLE {self.database}.{table_name}(
               link String,
               name String,
               location String,
               level String,
               company String,
               salary Nullable(Int32),
               description String,
               date_created Date,
               date_of_download Date, 
               status String,
               date_closed Nullable(Date),
               version UInt32,
               sign Int8
            ) ENGINE = VersionedCollapsingMergeTree(sign, version)
            PARTITION BY toYYYYMM(date_of_download)
            ORDER BY (link);
            """
            self.client.execute(create_table_query)
            self.log.info(f'Таблица {table_name} создана в базе данных {self.database}.')


class BaseJobParser:
    def __init__(self, url, profs, log):
        self.browser = webdriver.Remote(command_executor='http://selenium-router:4444/wd/hub', options=options)
        self.url = url
        self.browser.get(self.url)
        self.browser.maximize_window()
        self.browser.delete_all_cookies()
        time.sleep(2)
        self.profs = profs
        self.log = log

    def scroll_down_page(self, page_height=0):
        """
        Метод прокрутки страницы
        """
        self.browser.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(2)
        new_page_height = self.browser.execute_script('return document.body.scrollHeight')
        if new_page_height > page_height:
            self.scroll_down_page(new_page_height)

    def stop(self):
        """
        Метод для выхода из Selenium Webdriver
        """
        self.browser.quit()

    def find_vacancies(self):
        """
        Метод для парсинга вакансий, должен быть переопределен в наследниках
        """
        raise NotImplementedError("Вы должны определить метод find_vacancies")

    def save_df(self):
        """
        Метод для сохранения данных из pandas DataFrame
        """
        raise NotImplementedError("Вы должны определить метод save_df")


class VKJobParser(BaseJobParser):
    """
    Парсер вакансий с сайта VK, наследованный от BaseJobParser
    """
    def find_vacancies(self):
        """
        Метод для нахождения вакансий с VK
        """
        self.df = pd.DataFrame(columns=['link', 'name', 'location', 'company', 'date_created', 'date_of_download',
                                        'status', 'version', 'sign', 'description'])
        self.log.info('Старт парсинга вакансий')
        self.browser.implicitly_wait(3)
        # Поиск и запись вакансий на поисковой странице
        for prof in self.profs:
            input_button = self.browser.find_element(By.XPATH,
                                                     '/html/body/div/div[1]/div[1]/div/form/div[1]/div[4]/div/div/div/div/input')
            input_button.send_keys(prof['fullName'])
            click_button = self.browser.find_element(By.XPATH,
                                                     '/html/body/div/div[1]/div[1]/div/form/div[1]/div[4]/div/button')
            click_button.click()
            time.sleep(5)

            # Прокрутка вниз до конца страницы
            self.scroll_down_page()

            try:
                vacs_bar = self.browser.find_element(By.XPATH, '/html/body/div/div[1]/div[2]/div/div')
                vacs = vacs_bar.find_elements(By.CLASS_NAME, 'result-item')
                vacs = [div for div in vacs if 'result-item' in str(div.get_attribute('class'))]
                self.log.info(f"Парсим вакансии по запросу: {prof['fullName']}")
                self.log.info(f"Количество: " + str(len(vacs)) + "\n")

                for vac in vacs:
                    vac_info = {}
                    vac_info['link'] = str(vac.get_attribute('href'))
                    vac_info['name'] = str(vac.find_element(By.CLASS_NAME, 'title-block').text)
                    vac_info['location'] = str(vac.find_element(By.CLASS_NAME, 'result-item-place').text)
                    vac_info['company'] = str(vac.find_element(By.CLASS_NAME, 'result-item-unit').text)
                    self.df.loc[len(self.df)] = vac_info

            except Exception as e:
                self.log.error(f"Произошла ошибка: {e}")
                input_button.clear()

        self.df = self.df.drop_duplicates()
        self.df['date_created'] = datetime.now().date()
        self.df['date_of_download'] = datetime.now().date()
        self.df['status'] = 'existing'
        self.df['version'] = 1
        self.df['sign'] = 1

    def find_vacancies_description(self):
        """
        Метод для парсинга описаний вакансий для VKJobParser.
        """
        if len(self.df) > 0:
            for descr in self.df.index:
                try:
                    link = self.df.loc[descr, 'link']
                    self.browser.get(link)
                    self.browser.delete_all_cookies()
                    time.sleep(3)
                    desc = self.browser.find_element(By.CLASS_NAME, 'section').text
                    desc = desc.replace(';', '')
                    self.df.loc[descr, 'description'] = str(desc)

                except Exception as e:
                    self.log.error(f"Произошла ошибка: {e}, ссылка {self.df.loc[descr, 'link']}")
                    pass
        else:
            self.log.info(f"Нет вакансий для парсинга")

    def save_df(self):
        """
        Метод для сохранения данных в базу данных vk
        """
        table_name = 'raw_vk'
        # данные, которые вставляются в таблицу Clickhouse
        data = [tuple(x) for x in self.df.to_records(index=False)]
        # попытка вставить данные в таблицу
        client.execute(
            f"INSERT INTO {config['database']}.{table_name} \
            (link, name, location, company, date_created, date_of_download, status, version, sign, description) \
            VALUES", data)
        # логируем количество обработанных вакансий
        self.log.info("Общее количество вакансий после удаления дубликатов: " + str(len(self.df)) + "\n")


class SberJobParser(BaseJobParser):
    """
    Парсер для вакансий с сайта Sberbank, наследованный от BaseJobParser
    """
    def find_vacancies(self):
        """
        Метод для нахождения вакансий с Sberbank
        """
        self.df = pd.DataFrame(columns=['link', 'name', 'location', 'level', 'company', 'date_created',
                                        'date_of_download', 'status', 'version', 'sign', 'description'])
        self.log.info('Старт парсинга вакансий')
        self.browser.implicitly_wait(1)
        # Поиск и запись вакансий на поисковой странице
        for prof in self.profs:
            input_str = self.browser.find_element(By.XPATH, '/html/body/div/div/div[2]/div[3]/div/div/div[2]'
                                                            '/div/div/div/div/input')

            input_str.send_keys(f"{prof['fullName']}")
            click_button = self.browser.find_element(By.XPATH, '/html/body/div/div/div[2]/div[3]/div/div/div[2]'
                                                               '/div/div/div/div/button')
            click_button.click()
            time.sleep(5)

            # Прокрутка вниз до конца страницы
            self.scroll_down_page()

            try:
                # Подсчет количества предложений
                vacs_bar = self.browser.find_element(By.XPATH, '/html/body/div/div/div[2]/div[3]/div/div'
                                                               '/div[3]/div/div[3]/div[2]')
                vacs = vacs_bar.find_elements(By.TAG_NAME, 'div')

                vacs = [div for div in vacs if 'styled__Card-sc-192d1yv-1' in str(div.get_attribute('class'))]
                self.log.info(f"Парсим вакансии по запросу: {prof['fullName']}")
                self.log.info(f"Количество: " + str(len(vacs)) + "\n")

                for vac in vacs:
                    vac_info = {}
                    vac_info['link'] = vac.find_element(By.TAG_NAME, 'a').get_attribute('href')
                    data = vac.find_elements(By.CLASS_NAME, 'Text-sc-36c35j-0')
                    vac_info['name'] = data[0].text
                    vac_info['location'] = data[2].text
                    vac_info['company'] = data[3].text
                    vac_info['date_created'] = data[4].text
                    self.df.loc[len(self.df)] = vac_info

                input_str.clear()

            except Exception as e:
                self.log.error(f"Произошла ошибка: {e}")
                input_str.clear()

        # Удаление дубликатов в DataFrame
        self.df = self.df.drop_duplicates()
        self.df['level'] = ''

        self.df['date_created'] = self.df['date_created'].apply(lambda x: dateparser.parse(x, languages=['ru']))

        self.df['date_created'] = pd.to_datetime(self.df['date_created']).dt.to_pydatetime()
        self.df['date_of_download'] = datetime.now().date()
        self.df['status'] = 'existing'
        self.df['version'] = 1
        self.df['sign'] = 1

    def find_vacancies_description(self):
        """
        Метод для парсинга описаний вакансий для SberJobParser.
        """
        if len(self.df) > 0:
            for descr in self.df.index:
                try:
                    link = self.df.loc[descr, 'link']
                    self.browser.get(link)
                    self.browser.delete_all_cookies()
                    time.sleep(3)
                    desc = self.browser.find_element(By.XPATH, '/html/body/div[1]/div/div[2]/div[3]'
                                                               '/div/div/div[3]/div[3]').text
                    desc = desc.replace(';', '')
                    self.df.loc[descr, 'description'] = str(desc)

                except Exception as e:
                    self.log.error(f"Произошла ошибка: {e}, ссылка {self.df.loc[descr, 'link']}")
                    pass
        else:
            self.log.info(f"Нет вакансий для парсинга")

    def save_df(self):
        """
        Метод для сохранения данных в базу данных Sber
        """
        self.df['date_created'] = self.df['date_created'].dt.date
        table_name = 'raw_sber'
        data = [tuple(x) for x in self.df.to_records(index=False)]
        client.execute(
            f"INSERT INTO {config['database']}.{table_name} \
            (link, name, location, level, company, date_created, date_of_download, status, version, sign, description) \
            VALUES", data)
        self.log.info("Общее количество вакансий после удаления дубликатов: " + str(len(self.df)) + "\n")


class TinkoffJobParser(BaseJobParser):
    """
    Парсер вакансий с сайта Tinkoff, наследованный от BaseJobParser
    """
    def open_all_pages(self):
        elements = self.browser.find_elements(By.CLASS_NAME, 'fuBQPo')
        for element in elements:
            element.click()

    def all_vacs_parser(self):
        """
        Метод для нахождения вакансий с Tinkoff
        """
        self.df = pd.DataFrame(columns=['link', 'name', 'location', 'level', 'company', 'date_created', 'date_of_download', 'status', 'version', 'sign', 'description'])
        self.browser.implicitly_wait(3)
        vacs = self.browser.find_elements(By.CLASS_NAME, 'eM3bvP')
        for vac in vacs:
            try:
                vac_info = {}
                vac_info['link'] = vac.find_element(By.TAG_NAME, 'a').get_attribute('href')
                data = vac.find_elements(By.CLASS_NAME, 'gM3bvP')
                vac_info['name'] = data[0].text
                vac_info['location'] = data[2].text
                vac_info['level'] = data[1].text
                self.df.loc[len(self.df)] = vac_info
            except Exception as e:
                self.log.error(f"Ошибка при получении данных со страницы поиска: {e}")
                pass
            # Удаление дубликатов в DataFrame
            try:
                self.df = self.df.drop_duplicates()

                self.df['company'] = 'Тинькофф'
                # self.df = self.df.astype(str)
                # Добавление текущей даты в отдельный столбец
                self.df['date_created'] = datetime.now().date()
                self.df['date_of_download'] = datetime.now().date()
                self.df['status'] = 'existing'
                self.df['sign'] = 1
                self.df['version'] = 1

            except Exception as e:
                self.log.error(f"Ошибка при преобразовании DF: {e}")
                pass

    def find_vacancies_description(self):
        """
        Метод для парсинга описаний вакансий для TinkoffJobParser.
        """
        if len(self.df) > 0:
            for descr in self.df.index:
                try:
                    link = self.df.loc[descr, 'link']
                    self.browser.get(link)
                    self.browser.delete_all_cookies()
                    time.sleep(3)
                    desc = self.browser.find_element(By.CLASS_NAME, 'dyzaXu').text
                    desc = desc.replace(';', '')
                    self.df.loc[descr, 'description'] = str(desc)

                except Exception as e:
                    self.log.error(f"Произошла ошибка: {e}, ссылка {self.df.loc[descr, 'link']}")
                    pass
        else:
            self.log.info(f"Нет вакансий для парсинга")

    def save_df(self):
        """
        Метод для сохранения данных в базу данных Tinkoff
        """
        table_name = 'raw_tin'
        data = []
        for x in self.df.to_records(index=False):
            item = []
            for i in x:
                if isinstance(i, np.float64):
                    item.append(str(i))
                    print(f'Значение np.float64: {i} в {x}')
                else:
                    item.append(i)
            data.append(tuple(item))
        data = [tuple(x) for x in self.df.to_records(index=False)]
        client.execute(
            f"INSERT INTO {config['database']}.{table_name} \
                (link, name, location, level, company, date_created, date_of_download, status, version, sign, description) \
                VALUES", data)
        self.log.info("Общее количество вакансий после удаления дубликатов: " + str(len(self.df)) + "\n")


db_manager = DatabaseManager(client=client, database=config['database'])


def run_vk_parser(**context):
    """
    Основной вид задачи для запуска парсера для вакансий VK
    """
    log = context['ti'].log
    log.info('Запуск парсера ВК')
    try:
        parser = VKJobParser(url_vk, profs, log)
        parser.find_vacancies()
        parser.find_vacancies_description()
        parser.save_df()
        parser.stop()
        log.info('Парсер ВК успешно провел работу')
    except Exception as e:
        log.error(f'Ошибка во время работы парсера ВК: {e}')


def run_sber_parser(**context):
    """
    Основной вид задачи для запуска парсера для вакансий Sber
    """
    log = context['ti'].log
    log.info('Запуск парсера Сбербанка')
    try:
        parser = SberJobParser(url_sber, profs, log)
        parser.find_vacancies()
        parser.find_vacancies_description()
        parser.save_df()
        parser.stop()
        log.info('Парсер Сбербанка успешно провел работу')
    except Exception as e:
        log.error(f'Ошибка во время работы парсера Сбербанка: {e}')


def run_tin_parser(**context):
    """
    Основной вид задачи для запуска парсера для вакансий Tinkoff
    """
    log = context['ti'].log
    log.info('Запуск парсера Тинькофф')
    try:
        parser = TinkoffJobParser(url_tin, profs, log)
        parser.open_all_pages()
        parser.all_vacs_parser()
        parser.save_df()
        parser.stop()
        log.info('Парсер Тинькофф успешно провел работу')
    except Exception as e:
        log.error(f'Ошибка во время работы парсера Тинькофф: {e}')


hello_bash_task = BashOperator(
    task_id='hello_task',
    bash_command='echo "Желаю удачного парсинга! Да прибудет с нами безотказный интернет!"')

# Определение задачи
create_raw_tables = PythonOperator(
    task_id='create_raw_tables',
    python_callable=db_manager.create_raw_tables,
    provide_context=True,
    dag=first_raw_dag
)

parse_vkjobs = PythonOperator(
    task_id='parse_vkjobs',
    python_callable=run_vk_parser,
    provide_context=True,
    dag=first_raw_dag
)

parse_sber = PythonOperator(
    task_id='parse_sber',
    python_callable=run_sber_parser,
    provide_context=True,
    dag=first_raw_dag
)

parse_tink = PythonOperator(
    task_id='parse_tink',
    python_callable=run_tin_parser,
    provide_context=True,
    dag=first_raw_dag
)


end_task = DummyOperator(
    task_id="end_task"
)

hello_bash_task >> create_raw_tables >> parse_vkjobs >> parse_sber >> parse_tink >> end_task
