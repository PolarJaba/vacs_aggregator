import config as c

import time
import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys

import pandas as pd

profs = pd.read_csv('profs.txt', sep=',', header=None, names=['profs_names', 'short_name'])
df = pd.DataFrame(columns=['link', 'department'])
                           # 'location', 'format',
                           # 'name', 'describe'])

cur_date = datetime.datetime.now().strftime('%Y-%m-%d')
cur_year = datetime.datetime.now().year

with webdriver.Chrome() as browser:
    browser.get(c.yand_base_link)
    browser.maximize_window()
    browser.delete_all_cookies()
    time.sleep(2)

    for i in range(0, len(profs)):
        input_str = browser.find_element(By.XPATH, '/html/body/div[3]/div/div/span/section/div[1]/div[1]/div[2]/section/div/div/div/div[3]/div/div[2]/div/div/span/input')
        input_str.send_keys(f"{profs['profs_names'][i]}")
        time.sleep(5)
        click_button = browser.find_element(By.XPATH, '/html/body/div[3]/div/div/span/section/div[1]/div[1]/div[2]/section/div/div/div/div[3]/div/div[2]/div/button')
        click_button.click()
        time.sleep(5)

        # Прокрутка вниз до конца страницы
        page_height = browser.execute_script('return document.body.scrollHeight')

        while True:
            browser.find_element(By.TAG_NAME, 'body').send_keys(Keys.END)
            time.sleep(3)

            new_page_height = browser.execute_script('return document.body.scrollHeight')

            if new_page_height == page_height:
                break
            else:
                page_height = new_page_height

        # Подсчет количества предложений
        vacs_bar = browser.find_element(By.XPATH, '/html/body/div[3]/div/div/span/section/div[1]/div[1]/div[2]/section/div/div/div/div[3]/div/div[5]')
        vacs = vacs_bar.find_elements(By.TAG_NAME, 'span')

        vacs = [span for span in vacs if 'lc-jobs-vacancy-card' in str(span.get_attribute('class'))]
        print(f"Парсим вакансии по запросу: {profs['profs_names'][i]}")
        print(f"Количество: " + str(len(vacs)) + "\n")
        # 'id', 'link', 'department',
        # 'location', 'format',
        # 'name', 'describe'
        for vac in vacs:
            vac_info = {}
            # vac_info['id'] = vac.find_element(By.TAG_NAME, 'span').get_attribute('data-vacancy-id')
            vac_info['link'] = vac.find_element(By.TAG_NAME, 'a').get_attribute('href')
            data = vac.find_elements(By.CLASS_NAME, 'lc-styled-text__text')
            vac_info['department'] = data[0].text
            # vac_info['location'] = data[1].text
            # vac_info['format'] = data[2].text
            # vac_info['name'] = data[3].text
            # vac_info['describe'] = data[4].text
            df.loc[len(df)] = vac_info

        input_clear = browser.find_element(By.XPATH, '/html/body/div[3]/div/div/span/section/div[1]/div[1]/div[2]/section/div/div/div/div[3]/div/div[2]/div/div/span/span[1]')
        input_clear.click()

df.to_csv(f"loaded_data/test.txt", index=False, sep=';')
