from clickhouse_driver import Client
from datetime import datetime
import csv, json
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
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
import dateparser
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

url_sber = dag_variables['base_sber']
url_yand = dag_variables['base_yand']
raw_tables = ['raw_vk', 'raw_sber']

options = ChromeOptions()

profs = dag_variables['professions']
df = pd.DataFrame(columns=['link', 'name', 'location',
                           'company', 'date'])

# Параметры по умолчанию
default_args = {
    "owner": "admin_1T",
    # 'start_date': days_ago(1),
    'retry_delay': timedelta(minutes=5),
}

# Создаем DAG ручного запуска (инициализирующий режим).
scrap_dag = DAG(dag_id='scrap_dag',
                tags=['admin_1T'],
                start_date=datetime(2023, 10, 2),
                schedule_interval=None,
                default_args=default_args
                )

url_vk = dag_variables.get('base_vk')
cur_date = datetime.now().strftime('%Y-%m-%d')
cur_year = datetime.now().year


def create_raw_tables():
    for table_name in raw_tables:
        drop_table_query = f"DROP TABLE IF EXISTS {config['database']}.{table_name};"

        client.execute(drop_table_query)

        create_table_query = f"""
        CREATE TABLE {config['database']}.{table_name}(
            link String,
            name String,
            location String,
            company String,
            date String,
            date_of_download Date
        ) ENGINE = MergeTree()
        ORDER BY link
        """
        # ReplacingMergeTree
        # VersionedCollapsingMergeTree
        client.execute(create_table_query)
        print(f'Таблица {table_name} создана в базе данных {config["database"]}.')


class SberJobParser:

    def __init__(self, url_sber, profs):

        # Передаем название вакансии
        self.browser = webdriver.Remote(command_executor='http://selenium-router:4444/wd/hub', options=options)
        self.url_sber = url_sber
        self.browser.get(self.url_sber)
        self.browser.maximize_window()
        self.browser.delete_all_cookies()
        time.sleep(2)
        self.profs = profs
        self.df = pd.DataFrame(columns=['link', 'name', 'location', 'company', 'date', 'description'])

    def scroll_down_page(self, page_height=0):

        # Скролл до конца страницы
        self.browser.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(2)

        new_page_height = self.browser.execute_script('return document.body.scrollHeight')
        if new_page_height > page_height:
            self.scroll_down_page(new_page_height)

    def stop(self):

        # Выход из Webdriver selenium
        self.browser.quit()

    def find_vacancies(self):

        # Поиск и запись вакансий на поисковой странице
        for prof in self.profs:
            input_str = self.browser.find_element(By.XPATH,
                                                  '/html/body/div/div/div[2]/div[3]/div/div/div[2]/div/div/div/div/input')

            input_str.send_keys(f"{prof['fullName']}")
            click_button = self.browser.find_element(By.XPATH,
                                                     '/html/body/div/div/div[2]/div[3]/div/div/div[2]/div/div/div/div/button')
            click_button.click()
            time.sleep(5)

            # Прокрутка вниз до конца страницы
            self.scroll_down_page()

            try:
                # Подсчет количества предложений
                vacs_bar = self.browser.find_element(By.XPATH,
                                                     '/html/body/div/div/div[2]/div[3]/div/div/div[3]/div/div[3]/div[2]')
                vacs = vacs_bar.find_elements(By.TAG_NAME, 'div')

                vacs = [div for div in vacs if 'styled__Card-sc-192d1yv-1' in str(div.get_attribute('class'))]
                print(f"Парсим вакансии по запросу: {prof['fullName']}")
                print(f"Количество: " + str(len(vacs)) + "\n")

                for vac in vacs:
                    vac_info = {}
                    vac_info['link'] = vac.find_element(By.TAG_NAME, 'a').get_attribute('href')
                    data = vac.find_elements(By.CLASS_NAME, 'Text-sc-36c35j-0')
                    vac_info['name'] = data[0].text
                    vac_info['location'] = data[2].text
                    vac_info['company'] = data[3].text
                    vac_info['date'] = data[4].text
                    self.df.loc[len(self.df)] = vac_info

            except Exception as e:
                print(f"Произошла ошибка: {e}")
                input_str.clear()

        self.df = self.df.drop_duplicates(subset=['link'], keep='last')
        self.df['date'] = pd.to_datetime(self.df['date'].apply(lambda x: dateparser.parse(x).strftime('%Y-%m-%d')))
        self.df['date_of_download'] = datetime.now().date()

        # Нужно изменить тип данных в таблиуе и убрать
        self.df = self.df.astype(str)

    def save_df(self):
        # Загрузка данные в базу данных
        table_name = 'raw_sber'
        data = [tuple(x) for x in self.df.to_records(index=False)]
        insert_query = f"INSERT INTO {config['database']}.{table_name} VALUES"
        client.execute(insert_query, data)
        # Вывод результатов парсинга
        print("Общее количество вакансий после удаления дубликатов: " + str(len(self.df)) + "\n")


class VKJobParser(SberJobParser):

    def __init__(self, url_vk, profs):
        # Передаем название вакансии
        self.browser = webdriver.Remote(command_executor='http://selenium-router:4444/wd/hub', options=options)
        self.url_vk = url_vk
        self.browser.get(self.url_vk)
        self.browser.maximize_window()
        self.browser.delete_all_cookies()
        time.sleep(2)
        self.profs = profs
        self.df = pd.DataFrame(columns=['link', 'name', 'location', 'company', 'date'])


    def find_vacancies(self):
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
                print(f"Парсим вакансии по запросу: {prof['fullName']}")
                print(f"Количество: " + str(len(vacs)) + "\n")

                for vac in vacs:
                    vac_info = {}
                    vac_info['link'] = str(vac.get_attribute('href'))
                    vac_info['name'] = str(vac.find_element(By.CLASS_NAME, 'title-block').text)
                    vac_info['location'] = str(vac.find_element(By.CLASS_NAME, 'result-item-place').text)
                    vac_info['company'] = str(vac.find_element(By.CLASS_NAME, 'result-item-unit').text)
                    self.df.loc[len(self.df)] = vac_info

            except Exception as e:
                print(f"Произошла ошибка: {e}")
                input_button.clear()

        self.df = self.df.drop_duplicates()
        self.df = self.df.astype(str)
        self.df['date_of_download'] = datetime.now().date()

    def save_df(self):
        # Загружать данные в базу данных
        table_name = 'raw_vk'
        data = [tuple(x) for x in self.df.to_records(index=False)]
        insert_query = f"INSERT INTO {config['database']}.{table_name} VALUES"
        client.execute(insert_query, data)
        print("Общее количество вакансий после удаления дубликатов: " + str(len(self.df)) + "\n")


def run_vk_parser():
    parser = VKJobParser(url_vk, profs)
    parser.find_vacancies()
    parser.save_df()
    parser.stop()


def run_sber_parser():
    parser = SberJobParser(url_sber, profs)
    parser.find_vacancies()
    parser.save_df()
    parser.stop()


hello_bash_task = BashOperator(
    task_id='hello_task',
    bash_command='echo "Желаю удачного парсинга! Да прибудет с нами безотказный интернет!"')

# Определение задачи
create_raw_tables = PythonOperator(
    task_id='create_raw_tables',
    python_callable=create_raw_tables,
    dag=scrap_dag
)

parse_vkjobs = PythonOperator(
    task_id='parse_vkjobs',
    python_callable=run_vk_parser,
    dag=scrap_dag
)

parse_sber = PythonOperator(
    task_id='parse_sber',
    python_callable=run_sber_parser,
    dag=scrap_dag
)

end_task = DummyOperator(
    task_id="end_task"
)

hello_bash_task >> create_raw_tables >> [parse_vkjobs, parse_sber] >> end_task
