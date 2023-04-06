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
current_dir = os.getcwd()
file_logs = current_dir.replace(r'benvisavale\api',r'logs\benvisavale.log')
# gerando log
logging.basicConfig(level=logging.DEBUG, filename=file_logs,encoding='utf-8', format="%(asctime)s - %(levelname)s - %(message)s")

hoje = datetime.datetime.today().strftime("%B")
arquivocsv = current_dir.replace(r'crawler', r'dados\src_BASE_BENVISAVALE.csv')


#Omite o Navegador na Execução
chrome_options = Options()
chrome_options.add_experimental_option('excludeSwitches', ['enable-logging'])
chrome_options.add_argument('--allow-insecure-localhost')
chrome_options.add_argument('--headless')
chrome_options.add_argument('--no-sandbox')
chrome_options.add_argument('--disable-dev-shm-usage')
chrome_options.add_argument('--disable-gpu')
chrome_options.add_argument('--disable-software-rasterizer')
chrome_options.add_argument('--window-size=1366,768')
chrome_options.add_argument('--log-level=3')

def base_estabelecimentos_benvisavale(listacidades_txt:str = 'teste.txt'):

    """
        Coleta informações de estabelecimentos que aceitam cartões Bem Visa Vale do tipo Refeição 
        Parâmetros : 
            Cidade & UF - Região em que iremos buscar as informações do painel Credenciados Bem Visa Vale

        Retorno:
            Retorna um dataframe ou csv com as observações
                - Estabelecimento
                - Endereço
                - Cidade/UF
                - Telefone
                - Latitude
                - Longitude

    """
    # Montando o dicionário que vai receber os dados que posteriormente vira o arquivo csv
    EstabelecimentosBen = {
                "Estabelecimento" : [],
                "Endereço" : [],
                "Cidade_UF" : [],
                "Telefone" : [],
                "Latitude" : [],
                "Longitude" : []}

    municipios = open(listacidades_txt,'r', encoding='UTF-8')
    next(municipios)
    for municipio in municipios.readlines()[:]:
        n_municipio = re.sub('\t', ',', municipio)

        
        
        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
        driver.get("https://bensite.conductor.com.br/usuario/#rede-credenciada")
        
        # Teste
        driver.maximize_window()
        actions = ActionChains(driver)
        
        # Aceitando Cache
        actions.move_to_element(driver.find_element(By.XPATH,'//*[@id="__next"]/div/div/div[2]/div/div/button')).click().perform()
        
        # Encontrando e preenchendo o Formulário de pesquisa por endereço
        formulario = driver.find_element(By.CSS_SELECTOR,'#search-input')  
        actions.move_to_element(formulario).click().send_keys(n_municipio).perform()
        actions.click(formulario).send_keys(Keys.ARROW_DOWN).send_keys(Keys.ENTER)
        time.sleep(1)
        actions.perform()
        time.sleep(3) 
        #botao = driver.find_element(By.CLASS_NAME,'maps__search-submit.js-search-submit')
        #actions.move_to_element(botao).click().perform()
        # Encontrando o tipo de cartão
        
        actions.move_to_element(driver.find_element(By.NAME,'x-food')).click().perform()
        time.sleep(3)

        # Encontrando e expandindo a área de busca
        slide = driver.find_element(By.CLASS_NAME,'input-range__slider')
        actions.click_and_hold(slide).move_by_offset(300,0).release().perform()
        time.sleep(5)

        # Tentando extrair dados da pesquisa
        try:
            driver.find_element(By.CLASS_NAME,'maps__list-item.js-list-item')
            check = 'ok'
        except:
            check = 'nok'
        if check == 'ok':
            lista_estabelecimentos = driver.find_elements(By.CLASS_NAME,'maps__list-item.js-list-item')
            itens = len(lista_estabelecimentos)
            for estabelecimento in lista_estabelecimentos:
                link_maps = estabelecimento.find_element(By.TAG_NAME,'a').get_attribute("href").split('&daddr=')
                point = link_maps[-1]
                rota = point.split(',')
                detalhes = estabelecimento.text.splitlines()
                endereco = detalhes[1].split(',')

                try:
                    EstabelecimentosBen['Estabelecimento'].append(detalhes[0])
                    EstabelecimentosBen['Endereço'].append(detalhes[1])
                    EstabelecimentosBen['Cidade_UF'].append(endereco[-1])
                    EstabelecimentosBen['Telefone'].append(detalhes[2])
                    #EstabelecimentosBen['Telefone'].append('Sem contato informado')
                    EstabelecimentosBen['Latitude'].append(rota[0])
                    EstabelecimentosBen['Longitude'].append(rota[1])

                except:
                    continue
            dados = pd.DataFrame(EstabelecimentosBen)                    
            dados.to_csv(arquivocsv, mode='a', index=False, header=False, encoding='utf-8')
            dados = pd.DataFrame()
            EstabelecimentosBen = {
                                "Estabelecimento" : [],
                                "Endereço" : [],
                                "Cidade_UF" : [],
                                "Telefone" : [],
                                "Latitude" : [],
                                "Longitude" : []}
            logging.info(f'Extração de {itens} estabelecimentos em {endereco[-1]}')
            print(f'Extração de {itens} estabelecimentos em {endereco[-1]} \n na tentativa de extração em {n_municipio}')
        else:
            logging.warning(f'Não encontrado dados em {n_municipio}')    

    driver.close()
    driver.quit()