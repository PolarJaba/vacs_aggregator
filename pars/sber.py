# import requests
# from bs4 import BeautifulSoup
import config as c

import time
import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys

import pandas as pd

profs = pd.read_csv('profs.txt', sep=',', header=None, names=['profs_names', 'short_name'])
df = pd.DataFrame(columns=['link', 'name', 'location',
                           'company', 'date'])

cur_date = datetime.datetime.now().strftime('%Y-%m-%d')
cur_year = datetime.datetime.now().year

with webdriver.Chrome() as browser:

    # заход на сайт
    browser.get(c.sber_base_link)
    browser.maximize_window()
    browser.delete_all_cookies()
    time.sleep(2)

    for i in range(0, len(profs)):
        input_str = browser.find_element(By.XPATH,
                                         '/html/body/div/div/div[2]/div[3]/div/div/div[2]/div/div/div/div/input')

        input_str.send_keys(f"{profs['profs_names'][i]}")
        click_button = browser.find_element(By.XPATH,
                                            '/html/body/div/div/div[2]/div[3]/div/div/div[2]/div/div/div/div/button')
        click_button.click()
        time.sleep(5)

        # Прокрутка вниз до конца страницы
        page_height = browser.execute_script('return document.body.scrollHeight')

        while True:
            browser.find_element(By.TAG_NAME, 'body').send_keys(Keys.END)
            time.sleep(2)

            new_page_height = browser.execute_script('return document.body.scrollHeight')

            if new_page_height == page_height:
                break
            else:
                page_height = new_page_height

        # Подсчет количества предложений
        vacs_bar = browser.find_element(By.XPATH, '/html/body/div/div/div[2]/div[3]/div/div/div[3]/div/div[3]/div[2]')
        vacs = vacs_bar.find_elements(By.TAG_NAME, 'div')

        vacs = [div for div in vacs if 'styled__Card-sc-192d1yv-1' in str(div.get_attribute('class'))]
        print(f"Парсим вакансии по запросу: {profs['profs_names'][i]}")
        print(f"Количество: " + str(len(vacs)) + "\n")

        for vac in vacs:
            vac_info = {}
            vac_info['link'] = vac.find_element(By.TAG_NAME, 'a').get_attribute('href')
            # vac_info['name'] = vac.find_element(By.CLASS_NAME, 'Text-sc-36c35j-0')
            data = vac.find_elements(By.CLASS_NAME, 'Text-sc-36c35j-0')
            vac_info['name'] = data[0].text
            vac_info['location'] = data[2].text
            vac_info['company'] = data[3].text
            vac_info['date'] = data[4].text
            df.loc[len(df)] = vac_info

        input_str.clear()

        # Раскомментировать для загрузки данных в файлы по запросам
        # df.to_csv(f"loaded_data/{cur_year}/sber/{profs['short_name'][i]}-{cur_date}.txt", index=False, sep=';')
        # df = df.drop(df.index[0:])

df = df.drop_duplicates()
df.to_csv(f"loaded_data/{cur_year}/sber/all_{cur_date}.txt", index=False, sep=';')
print("Общее количество вакансий: " + str(len(df)) + "\n")

