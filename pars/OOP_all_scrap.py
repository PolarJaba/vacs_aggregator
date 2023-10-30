import config as c

import time
import datetime
import dateparser
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC

import pandas as pd

profs = pd.read_csv('profs.txt', sep=',', header=None, names=['fullName'])
url_sber = c.sber_base_link
url_vk = c.vk_base_link
url_tin = c.tin_base_link
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
                    if isinstance(self, SberJobParser):
                        desc = self.browser.find_element(By.XPATH, '/html/body/div[1]/div/div[2]/div[3]'
                                                                   '/div/div/div[3]/div[3]').text
                        desc = desc.replace(';', '')
                        self.df.loc[:, (descr, 'description')] = str(desc)
                    if isinstance(self, VKJobParser):
                        desc = self.browser.find_element(By.CLASS_NAME, 'section').text
                        desc = desc.replace(';', '')
                        self.df.loc[:, (descr, 'description')] = str(desc)
                    if isinstance(self, TinkoffJobParser):
                        desc = self.browser.find_element(By.CLASS_NAME, 'dyzaXu').text
                        desc = desc.replace(';', '')
                        self.df.loc[:, (descr, 'description')] = str(desc)
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


class SberJobParser(BaseJobParser):
    """
    Парсер для вакансий с сайта Sberbank, наследованный от BaseJobParser
    """
    def find_vacancies(self):
        """Метод для нахождения вакансий с Sberbank"""
        print('Старт парсинга вакансий Sberbank')

        self.df = pd.DataFrame(columns=['link', 'name', 'location', 'company', 'vacancy_date', 'date_of_download'])
        self.browser.implicitly_wait(1)

        # Поиск и запись вакансий на поисковой странице
        for prof in self.profs['fullName']:
            input_str = self.browser.find_element(By.XPATH, '/html/body/div/div/div[2]/div[3]/div'
                                                            '/div/div[2]/div/div/div/div/input')

            input_str.send_keys(f"{prof}")
            click_button = self.browser.find_element(By.XPATH, '/html/body/div/div/div[2]/div[3]/div/div'
                                                               '/div[2]/div/div/div/div/button')
            click_button.click()
            time.sleep(3)

            # Прокрутка вниз до конца страницы
            self.scroll_down_page()

            try:
                # Подсчет количества предложений
                vacs_bar = self.browser.find_element(By.XPATH, '/html/body/div/div/div[2]/div[3]/div/div'
                                                               '/div[3]/div/div[3]/div[2]')
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
                    vac_info['vacancy_date'] = data[4].text
                    self.df.loc[len(self.df)] = vac_info

                input_str.clear()

            except Exception as e:
                print(f"Произошла ошибка: {e}")
                input_str.clear()

        # Удаление дубликатов в DataFrame
        self.df = self.df.drop_duplicates()

        # конвертация строки в объект datetime используя dateparser и приведение его к DateTime pandas
        self.df['vacancy_date'] = self.df['vacancy_date'].apply(lambda x: dateparser.parse(x).strftime('%Y-%m-%d'))

        # Добавление текущей даты в отдельный столбец
        self.df['date_of_download'] = datetime.datetime.now().date()


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


"""parser = VKJobParser(url_vk, profs)
parser.find_vacancies()
parser.find_vacancies_description()
parser.save_df(raw_tables[0])
parser.stop()"""

"""parser = SberJobParser(url_sber, profs)
parser.find_vacancies()
parser.find_vacancies_description()
parser.save_df(raw_tables[1])
parser.stop()

parser = TinkoffJobParser(url_tin, profs)
parser.open_all_pages()
parser.all_vacs_parser()
parser.find_vacancies_description()
parser.save_df(raw_tables[2])
parser.stop()"""

parser = YandJobParser(c.yand_base_link, profs)
parser.find_vacancies()
parser.find_vacancies_description()
parser.save_df(raw_tables[3])
parser.stop()

end_time = time.time()

print('Scraping time: ', end_time - start_time)



