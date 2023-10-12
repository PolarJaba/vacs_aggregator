import config as c

import time
import datetime
import dateparser
import concurrent.futures

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC

import pandas as pd

profs = pd.read_csv('profs.txt', sep=',', header=None, names=['profs_names'])
url_sber = c.sber_base_link
url_vk = c.vk_base_link


class SberJobParser:

    def __init__(self, url_sber, profs):

        # Передаем название вакансии
        self.browser = webdriver.Chrome()
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

    def find_vacancies(self, prof):

        # Поиск и запись вакансий на поисковой странице

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
            print(f"Парсим вакансии по запросу: {prof}")
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


    def parallel_find_vacancies(self):
        futures = []
        with concurrent.futures.ThreadPoolExecutor() as executor:
            for prof in self.profs['profs_names']:
                futures.append(executor.submit(self.find_vacancies(prof)))
            for future in concurrent.futures.as_completed(futures):
                

    def change_df(self, find_vacancies):
        self.df = self.df.drop_duplicates(subset=['link'], keep='last')
        self.df['date'] = pd.to_datetime(
            self.df['date'].apply(lambda x: dateparser.parse(x).strftime('%Y-%m-%d'))).dt.date()
        self.df['date_of_download'] = datetime.datetime.now().date()


    def save_df(self):
        self.df.to_csv(f"all_sber.txt", index=False, sep=';')
        # Вывод результатов парсинга
        print("Общее количество вакансий: " + str(len(self.df)) + "\n")



    """class VKJobParser(SberJobParser):

    def __init__(self, url_vk, profs):
        # Передаем название вакансии
        self.browser = webdriver.Chrome()
        self.url_vk = url_vk
        self.browser.get(self.url_vk)
        self.browser.maximize_window()
        self.browser.delete_all_cookies()
        time.sleep(2)
        self.profs = profs
        self.df = pd.DataFrame(columns=['link', 'name', 'location', 'company', 'date'])

    def find_vacancies(self, prof):
        # Поиск и запись вакансий на поисковой странице

        input_button = self.browser.find_element(By.XPATH,
                                                     '/html/body/div/div[1]/div[1]/div/form/div[1]/div[4]/div/div/div/div/input')
        input_button.send_keys(prof)
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
            print(f"Парсим вакансии по запросу: {prof}")
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
            input_button.clear()"""

"""    def save_df(self):
        self.df.to_csv(f"all_vk.txt", index=False, sep=';')
        # Вывод результатов парсинга
        print("Общее количество вакансий: " + str(len(self.df)) + "\n")"""




"""def run_vk_parser():
    parser = VKJobParser(url_vk, profs)
    parser.find_vacancies(prof)
    parser.save_df()
    parser.stop()"""

def run_sber_parser():
    parser = SberJobParser(url_sber, profs)
    parser.find_vacancies()
    parser.save_df()
    parser.stop()

# run_vk_parser()
run_sber_parser()

