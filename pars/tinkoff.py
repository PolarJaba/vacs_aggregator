import config as c

import time
import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By


import pandas as pd

url_tin = c.tin_base_link
df = pd.DataFrame(columns=['link', 'name', 'location', 'level', 'company'])


class TinkoffJobParser:
    def __init__(self, url, df):
        self.browser = webdriver.Chrome()
        self.url = url
        self.browser.get(self.url)
        self.browser.maximize_window()
        self.browser.delete_all_cookies()
        time.sleep(2)
        self.df = df

    def open_all_page(self):
        elements = self.browser.find_elements(By.CLASS_NAME, 'fuBQPo')
        for element in elements:
            element.click()

    def all_vacs_parser(self):
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
        self.df['date_of_download'] = datetime.datetime.now().strftime('%Y-%m-%d')

    def find_vacancies_description(self):
        for descr in self.df.index:
            link = self.df.loc[descr, 'link']
            self.browser.get(link)
            self.browser.delete_all_cookies()
            time.sleep(3)
            self.df.loc[descr, 'description'] = self.browser.find_element(By.CLASS_NAME, 'dyzaXu').text.replace(';', '')

    def save_df(self):
        """
        Метод для сохранения данных из pandas DataFrame
        """
        self.df.to_csv(f"tin.txt", index=False, sep=';')
        print("Общее количество вакансий: " + str(len(self.df)) + "\n")


parser = TinkoffJobParser(url_tin, df)
parser.open_all_page()
parser.all_vacs_parser()
parser.find_vacancies_description()
parser.save_df()

