# import requests
# from bs4 import BeautifulSoup
import config as c

import time
import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC

import pandas as pd

cur_date = datetime.datetime.now().strftime('%Y-%m-%d')
cur_year = datetime.datetime.now().year
profs = pd.read_csv('profs.txt', sep=',', header=None, names=['profs_names'])
df = pd.DataFrame(columns=['link', 'name', 'location',
                           'company', 'date', 'date_of_download'])


# ООП версия

class SberJobParser:

    def __init__(self, vac_type):

        # Передаем название вакансии
        self.vac_type = vac_type
        self.browser = webdriver.Chrome()
        self.browser.get(self.url)
        self.browser.maximize_window()
        self.browser.delete_all_cookies()
        time.sleep(2)

    def scroll_down_page(self, page_height=0):

        # Скролл до конца страницы
        self.browser.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(2)

        new_page_height = self.browser.execute_script('return document.body.scrollHeight')
        if new_page_height > page_height:
            self.scroll_down_page(new_page_height)

    url = c.sber_base_link

    def stop(self):

        # Выход из Webdriver selenium
        self.browser.quit()

    def find_vacancies(self, profs, df):

        # Поиск и запись вакансий на поисковой странице
        for prof in profs['profs_names']:
            input_str = self.browser.find_element(By.XPATH,
                                                  '/html/body/div/div/div[2]/div[3]/div/div/div[2]/div/div/div/div/input')

            input_str.send_keys(f"{prof}")
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
                print(f"Количество вакансий по запросу: '{prof}': " + str(len(vacs)) + "\n")

                for vac in vacs:
                    vac_info = {}
                    vac_info['link'] = vac.find_element(By.TAG_NAME, 'a').get_attribute('href')
                    # vac_info['name'] = vac.find_element(By.CLASS_NAME, 'Text-sc-36c35j-0')
                    data = vac.find_elements(By.CLASS_NAME, 'Text-sc-36c35j-0')
                    vac_info['name'] = data[0].text
                    vac_info['location'] = data[2].text
                    vac_info['company'] = data[3].text
                    vac_info['date'] = data[4].text
                    vac_info['date_of_download'] = datetime.datetime.now().strftime('%Y-%m-%d')
                    df.loc[len(df)] = vac_info
                input_str.clear()

            except:
                input_str.clear()
                pass

        self.df = df.drop_duplicates()

    def save_df(self):
        self.df.to_csv(f"all_{cur_date}.txt", index=False, sep=';')


    """    def find_descriptions(self, df):

        # Парсинг описаний
        for descr in range(len(df)):
            link = df.loc[descr, 'link']
            self.browser.get(link)
            self.browser.delete_all_cookies()
            time.sleep(3)

            df.loc[descr, 'description'] = self.browser.find_element(By.CLASS_NAME, 'Box-sc-159i47a-0').text

        time.sleep(600)"""


parser = SberJobParser(profs)
parser.find_vacancies(profs, df)
# parser.find_descriptions(df)
parser.save_df()
parser.stop()

print("Общее количество вакансий: " + str(len(df)) + "\n")
