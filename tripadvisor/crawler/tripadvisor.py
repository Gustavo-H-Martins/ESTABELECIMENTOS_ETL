"""libs"""
import time
import datetime
import re
import os
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import logging

# obteendo o caminho do diretório atual e construindo o caminho do arquivo a partir dele
current_dir = os.path.dirname(os.path.abspath(__file__))
file_logs = current_dir.replace(r'tripadvisor\crawler',r'logs\tripadvisor.log')

# gerando log
logging.basicConfig(level=logging.INFO, filename=file_logs ,encoding='utf-8', format="%(asctime)s - %(levelname)s - %(message)s")
#search = str(input('Onde vamos procurar? \n'))


"""
   Omite o Navegador na Execução
"""
chrome_options = Options()
chrome_options.add_experimental_option('excludeSwitches', ['enable-logging'])
chrome_options.add_argument('--allow-insecure-localhost')
#chrome_options.add_argument('--headless')
chrome_options.add_argument('--no-sandbox')
chrome_options.add_argument('--disable-dev-shm-usage')
chrome_options.add_argument('--disable-gpu')
chrome_options.add_argument('--disable-software-rasterizer')
chrome_options.add_argument('--window-size=1366,768')
chrome_options.add_argument('--log-level=3')


url_base = 'https://www.tripadvisor.com.br/'

# Abre o navegador
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
driver.get(url=url_base)
actions = ActionChains(driver)
os.system('cls')
driver.maximize_window()
time.sleep(1)

# Aceitando o cache
actions.move_to_element(driver.find_element(By.ID,'onetrust-accept-btn-handler')).click().perform()
time.sleep(30)