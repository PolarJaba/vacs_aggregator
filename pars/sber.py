import config as c

import time
import datetime
import dateparser
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC

import pandas as pd


profs = pd.read_csv('profs.txt', sep=',', header=None, names=['profs_names'])
url_sber = c.sber_base_link


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

    def find_vacancies(self):

        # Поиск и запись вакансий на поисковой странице
        for prof in self.profs['profs_names']:
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
                    data = vac.find_elements(By.CLASS_NAME, 'Text-sc-36c35j-0')
                    vac_info['name'] = data[0].text
                    vac_info['location'] = data[2].text
                    vac_info['company'] = data[3].text
                    vac_info['date'] = data[4].text
                    vac_info['date_of_download'] = datetime.datetime.now().strftime('%Y-%m-%d')
                    self.df.loc[len(self.df)] = vac_info
                input_str.clear()


            except Exception as e:
                print(f"Произошла ошибка: {e}")
                input_str.clear()

        self.df = self.df.drop_duplicates(subset=['link'], keep='last')
        self.df['date'] = self.df['date'].apply(lambda x: dateparser.parse(x).strftime('%Y-%m-%d'))
        print('Всего:', len(self.df.drop_duplicates(subset=['link'], keep='last')))
        self.df.to_csv(f"all_shorts.txt", index=False, sep=';')

    def find_descriptions(self):

        # Парсинг описаний
        for descr in self.df.index:
            link = self.df.loc[descr, 'link']
            self.browser.get(link)
            self.browser.delete_all_cookies()
            time.sleep(3)
            #texts_elems = self.browser.find_elements(By.CLASS_NAME, 'Box-sc-159i47a-0')
            try:
                self.df.loc[descr, 'description'] = str(self.browser.find_element(By.XPATH, '/html/body/div[1]/div/div[2]/div[3]/div/div/div[3]/div[3]').text.replace(';', ''))
            except:
                self.df.loc[descr, 'description'] = None


    def save_df(self):

        self.df.to_csv(f"loaded_data/all_3.txt", index=False, sep=';')
        # Вывод результатов парсинга
        print("Общее количество вакансий: " + str(len(self.df)) + "\n")


parser = SberJobParser(url_sber, profs)
parser.find_vacancies()
# parser.find_descriptions()
parser.save_df()
parser.stop()
