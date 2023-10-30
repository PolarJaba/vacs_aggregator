import config as c

import time
import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By


import pandas as pd

url_tin = c.tin_base_link
profs = pd.read_csv('profs.txt', sep=',', header=None, names=['fullName'])
raw_tables = ['raw_vk', 'raw_sber', 'raw_tinkoff', 'raw_yandex']


class BaseJobParser:
    def __init__(self, url, profs, df=pd.DataFrame()):
        self.browser = webdriver.Chrome()
        self.url = url
        self.browser.get(self.url)
        self.browser.maximize_window()
        self.browser.delete_all_cookies()
        time.sleep(2)
        self.profs = profs
        self.df = df

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

    def find_vacancies_description(self):
        """
        Метод для парсинга вакансий, должен быть дополнен в наследниках
        """
        if len(self.df) > 0:
            for descr in self.df.index:
                try:
                    link = self.df.loc[descr, 'link']
                    self.browser.get(link)
                    self.browser.delete_all_cookies()
                    self.browser.implicitly_wait(5)
                    # Этот парсер разрабатывался для общего для 3х источников DAG'a
                    if isinstance(self, TinkoffJobParser):
                        desc = self.browser.find_element(By.CLASS_NAME, 'dyzaXu').text
                        desc = desc.replace(';', '')
                        self.df.loc[:, (descr, 'description')] = str(desc)
                except Exception as e:
                    print(f"Произошла ошибка: {e}, ссылка {self.df.loc[descr, 'link']}")
                    pass
        else:
            print("Нет вакансий для парсинга")

    def save_df(self, table):
        """
        Метод для сохранения данных из pandas DataFrame
        """
        self.df.to_csv(f"loaded_data/{table}.txt", index=False, sep=';')
        print("Общее количество вакансий после удаления дубликатов: " + str(len(self.df)) + "\n")


class TinkoffJobParser(BaseJobParser):

    def open_all_pages(self):
        elements = self.browser.find_elements(By.CLASS_NAME, 'fuBQPo')
        for element in elements:
            element.click()

    def all_vacs_parser(self):
        self.df = pd.DataFrame(
            columns=['link', 'name', 'location', 'level', 'company', 'vacancy_date', 'date_of_download'])
        try:
            vacs = self.browser.find_elements(By.CLASS_NAME, 'eM3bvP')
            for vac in vacs:
                vac_info = {}
                vac_info['link'] = vac.find_element(By.TAG_NAME, 'a').get_attribute('href')
                data = vac.find_elements(By.CLASS_NAME, 'gM3bvP')
                vac_info['name'] = data[0].text
                vac_info['level'] = data[1].text
                vac_info['location'] = data[2].text
                self.df.loc[len(self.df)] = vac_info

            self.df['company'] = 'Тинькофф'
            self.df['vacancy_date'] = pd.to_datetime('1970-01-01').date()
            self.df['date_of_download'] = datetime.datetime.now().strftime('%Y-%m-%d')

        except Exception as e:
            print(f"Произошла ошибка: {e}")


parser = TinkoffJobParser(url_tin, profs)
parser.open_all_pages()
parser.all_vacs_parser()
parser.find_vacancies_description()
parser.save_df(raw_tables[2])
parser.stop()

