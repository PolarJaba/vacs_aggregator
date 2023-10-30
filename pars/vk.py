import config as c

import time
import datetime
import dateparser
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC

import pandas as pd

profs = pd.read_csv('profs.txt', sep=',', header=None, names=['fullName'])
url_vk = c.vk_base_link
raw_tables = ['raw_vk', 'raw_sber', 'raw_tinkoff', 'raw_yandex']

start_time = time.time()


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
                    if isinstance(self, VKJobParser):
                        desc = self.browser.find_element(By.CLASS_NAME, 'section').text
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


class VKJobParser(BaseJobParser):
    """
    Парсер вакансий с сайта VK, наследованный от BaseJobParser
    """
    def find_vacancies(self):
        """
        Метод для нахождения вакансий с VK
        """
        print('Старт парсинга вакансий VK')

        self.df = pd.DataFrame(columns=['link', 'name', 'location', 'company', 'vacancy_date', 'date_of_download'])
        self.browser.implicitly_wait(3)
        # Поиск и запись вакансий на поисковой странице
        for prof in self.profs['fullName']:
            input_button = self.browser.find_element(By.XPATH, '/html/body/div/div[1]/div[1]/div/form'
                                                               '/div[1]/div[4]/div/div/div/div/input')
            input_button.send_keys(f"{prof}")
            click_button = self.browser.find_element(By.XPATH, '/html/body/div/div[1]/div[1]/div'
                                                               '/form/div[1]/div[4]/div/button')
            click_button.click()
            time.sleep(5)

            # Прокрутка вниз до конца страницы
            self.scroll_down_page()

            try:
                vacs_bar = self.browser.find_element(By.XPATH, '/html/body/div/div[1]/div[2]/div/div')
                vacs = vacs_bar.find_elements(By.CLASS_NAME, 'result-item')
                vacs = [div for div in vacs if 'result-item' in str(div.get_attribute('class'))]
                print(f"Парсим вакансии по запросу: {prof}")
                print(f"Количество: " + str(len(vacs)) + "\n")

                for vac in vacs:
                    vac_info = {}
                    vac_info['link'] = vac.get_attribute('href')
                    vac_info['name'] = vac.find_element(By.CLASS_NAME, 'title-block').text
                    vac_info['location'] = vac.find_element(By.CLASS_NAME, 'result-item-place').text
                    vac_info['company'] = vac.find_element(By.CLASS_NAME, 'result-item-unit').text
                    self.df.loc[len(self.df)] = vac_info

                input_button.clear()
            except Exception as e:
                print(f"Произошла ошибка: {e}")
                input_button.clear()

        self.df = self.df.drop_duplicates()
        self.df['vacancy_date'] = pd.to_datetime('1970-01-01').date()
        self.df['date_of_download'] = datetime.datetime.now().date()


parser = VKJobParser(url_vk, profs)
parser.find_vacancies()
parser.find_vacancies_description()
parser.save_df(raw_tables[0])
parser.stop()