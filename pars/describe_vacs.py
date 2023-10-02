import pandas as pd

import config as c

import pandas as py
import time
import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys

cur_date = datetime.datetime.now().strftime('%Y-%m-%d')

# Получение id вакансий за день
ids = pd.read_csv(f'loaded_data/id_list-{cur_date}.txt')


