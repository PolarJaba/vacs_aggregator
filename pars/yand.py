import config as c

import time
import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys

import pandas as pd

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
                    if isinstance(self, YandJobParser):
                        desc = self.browser.find_element(By.CLASS_NAME, 'lc-jobs-vacancy-mvp__description').text
                        desc = desc.replace(';', '')
                        self.df.loc[:, (desc, 'description')] = str(desc)
                        tags = self.browser.find_element(By.CLASS_NAME, 'lc-jobs-tags-block').text
                        self.df.loc[:, (desc, 'tags')] = str(tags)
                        header = self.browser.find_element(By.CLASS_NAME, 'lc-jobs-content-header')
                        name = header.find_element(By.CLASS_NAME, 'lc-styled-text__text').text
                        self.df.loc[:, (desc, 'name')] = str(name)
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


class YandJobParser(BaseJobParser):
    def find_vacancies(self):
        print('Старт парсинга вакансий Yandex')

        self.df = pd.DataFrame(columns=['link', 'name', 'company', 'tags', 'description', 'date_of_download'])
        self.browser.implicitly_wait(3)
        # Поиск и запись вакансий на поисковой странице
        for prof in self.profs['fullName']:
            input_str = self.browser.find_element(By.XPATH, '/html/body/div[3]/div/div/span/section/div[1]'
                                                            '/div[1]/div[2]/section/div/div/div/div[3]/div'
                                                            '/div[2]/div/div/span/input')

            input_str.send_keys(f"{prof}")
            click_button = self.browser.find_element(By.XPATH, '/html/body/div[3]/div/div/span/section/div[1]'
                                                               '/div[1]/div[2]/section/div/div/div/div[3]/div'
                                                               '/div[2]/div/button')
            click_button.click()
            time.sleep(3)

            # Прокрутка вниз до конца страницы
            self.scroll_down_page()

            try:
                # Подсчет количества предложений
                vacs_bar = self.browser.find_element(By.CLASS_NAME, 'lc-jobs-vacancies-list')
                vacs = vacs_bar.find_elements(By.CLASS_NAME, 'lc-jobs-vacancy-card')

                for vac in vacs:
                    vac_info = {}
                    find_link = vac.find_element(By.CLASS_NAME, 'lc-jobs-vacancy-card__link')
                    vac_info['link'] = find_link.get_attribute('href')
                    vac_info['company'] = vac.find_elements(By.CLASS_NAME, 'lc-styled-text')[0].text
                    self.df.loc[len(self.df)] = vac_info

            except Exception as e:
                print(f"Ошибка {e}")

            clear_button = self.browser.find_element(By.XPATH, '/html/body/div[3]/div/div/span/section/div[1]/div[1]'
                                                               '/div[2]/section/div/div/div/div[3]/div/div[2]/div'
                                                               '/div/span/span[1]')
            clear_button.click()

        self.df = self.df.drop_duplicates()
        self.df['date_of_download'] = datetime.datetime.now().date()


parser = YandJobParser(c.yand_base_link, profs)
parser.find_vacancies()
parser.find_vacancies_description()
parser.save_df(raw_tables[3])
parser.stop()




