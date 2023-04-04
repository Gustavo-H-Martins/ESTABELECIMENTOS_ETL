"""libs"""
import time
import datetime
from fake_useragent import UserAgent
import ctypes
import gc
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

# gerando log
logging.basicConfig(level=logging.INFO, filename="src_Alelo.log",encoding='utf-8', format="%(asctime)s - %(levelname)s - %(message)s")

# pegando a resolução da tela

user32 = ctypes.windll.user32
resolucao = user32.GetSystemMetrics(0), user32.GetSystemMetrics(1)


hoje = datetime.datetime.today().strftime("%B")
arquivocsv = f'{hoje}_Estabelecimentos_Alelo.csv'

"""
   Omite o Navegador na Execução
"""
chrome_options = Options()
chrome_options.add_experimental_option('excludeSwitches', ['enable-logging'])
chrome_options.add_argument('--allow-insecure-localhost')
chrome_options.add_argument('--headless')
chrome_options.add_argument('--no-sandbox')
chrome_options.add_argument('--disable-dev-shm-usage')
chrome_options.add_argument('--disable-gpu')
chrome_options.add_argument('--disable-software-rasterizer')
chrome_options.add_argument(f'--window-size={resolucao[0]},{resolucao[1]}')
chrome_options.add_argument('--log-level=3')

ua = UserAgent()
userAgent = ua.chrome

#def base_estabelecimentos_alelo(listacidades_txt:str = 'teste.txt'):
def base_estabelecimentos_alelo(municipio:str = 'Lagoa Santa	MG'):
    #listacidades_txt = 'teste.txt'

    """
        Coleta informações de estabelecimentos que aceitam cartões Alelo do tipo Refeição
        Parâmetros : 
            Cidade & UF - Região em que iremos buscar as informações do painel Credenciados Alelo

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
    EstabelecimentosAlelo = {
                "Estabelecimento" : [],
                "Endereço" : [],
                "Cidade_UF" : [],
                "Telefone" : [],
                "Latitude" : [],
                "Longitude" : []}

    #municipios = open(listacidades_txt,'r', encoding='UTF-8')
    #next(municipios)
    #for municipio in municipios.readlines():
    municipio = municipio.split('\t')
    cidade = municipio[0]
    uf = municipio[1]


    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
    driver.implicitly_wait(15)
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    driver.execute_cdp_cmd('Network.setUserAgentOverride', {"userAgent": userAgent})
    driver.maximize_window()
    driver.get("https://redeaceitacao.alelo.com.br/")

    actions = ActionChains(driver, duration=100)
    time.sleep(3)

    # encontra e preenche o formulário
    try:
        formulario = driver.find_element(By.CLASS_NAME,'container-filter-autocomplete.pac-target-input')

    except:
        formulario = driver.find_element(By.XPATH,'//*[@id="divMyMap"]/div[2]/div[1]/input')

    actions.move_to_element(formulario).click()
    #actions.send_keys_to_element(formulario,f'{cidade}, {uf.upper()}').send_keys_to_element(formulario,Keys.ARROW_DOWN).send_keys_to_element(formulario,Keys.ENTER).perform()
    actions.send_keys_to_element(formulario,f'{cidade}, {uf.upper()}, Brasil').move_by_offset(0,15).pause(0.5).click().perform()
    time.sleep(0.5)

    # encontra e seleciona o cartão
    cartoes = driver.find_element(By.CLASS_NAME,'container-filter-combo-product-desc')
    actions.move_to_element(cartoes).click().click(driver.find_element(By.CSS_SELECTOR,'#divMyMap > div.container-filter > div.container-filter-out-autocomplete > div.container-filter-combo-product > div.container-filter-combo-product-multi > ul > li:nth-child(1)')).perform()
    time.sleep(0.5)

    # encontra a lista tipos estabelecimentos
    categoria_filtro = driver.find_element(By.CLASS_NAME,'container-filter-combo-segment-desc-general')

    # encontra o botão buscar
    buscar = driver.find_element(By.CLASS_NAME,'container-filter-button-search')


    for i in [7, 1, 4, 6]:
        # aciona o botão de tipo de estabelecimento
        try:
            actions.move_to_element(categoria_filtro).click(categoria_filtro).perform()
        except:
            actions.move_to_element(cartoes).click().click(driver.find_element(By.CSS_SELECTOR,'#divMyMap > div.container-filter > div.container-filter-out-autocomplete > div.container-filter-combo-product > div.container-filter-combo-product-multi > ul > li:nth-child(1)')).perform()
            actions.move_to_element(categoria_filtro).click(categoria_filtro).perform()
        time.sleep(0.5)

        # Seleciona o tipo de estabelecimento 
        str_opcao = f'#divMyMap > div.container-filter > div.container-filter-out-autocomplete > div.container-filter-combo-segment > div.container-filter-combo-segment-multi > ul > li > ul > li:nth-child({i}) > label'
        time.sleep(0.5)
        opcao = driver.find_element(By.CSS_SELECTOR,str_opcao)
        
        #print(f'{opcao.text}\n{opcao.location}\n{opcao.location_once_scrolled_into_view}')
        txt_opcao = opcao.text
        actions.move_to_element(categoria_filtro)
        driver.execute_script("arguments[0].click();", opcao)
        actions.pause(0.25).perform()
        time.sleep(0.5)

        # aciona o botão buscar
        actions.move_to_element(buscar).click().perform()
        time.sleep(1)

        # extração dos dados aqui
        
        lista_nome, lista_endereco, lista_estabelecimento  = driver.find_elements(By.CLASS_NAME,'container-stores-box-title') , driver.find_elements(By.CLASS_NAME,'container-stores-box-icon-location-label'), driver.find_elements(By.CLASS_NAME,'container-stores-box-item')
        logging.info(f'Tentando extrair {len(lista_estabelecimento)} dados de {txt_opcao} em {cidade}')
        print(f'Tentando extrair {len(lista_estabelecimento)} dados de {txt_opcao} em {cidade}')
        if len(lista_estabelecimento) > 0: 
            #print(type(len(lista_estabelecimentos)))  
            time.sleep(2) 
            for i in range(0, len(lista_estabelecimento)-1):
                # pegando informações para cada estabelecimento
                actions.move_to_element(lista_estabelecimento[i]).click()
                try:
                    actions.perform()
                except:
                    actions.reset_actions()
                    #actions = ActionChains(driver, duration=100)
                # o telefone é flutuante mó loucura bicho
                try:
                    telefone_atual = driver.find_element(By.CLASS_NAME,'info-box-phone').text
                except:
                    telefone_atual = "indisponível"
                
                # pegando latitude e longitude
                try:
                    latitude = lista_estabelecimento[i].get_attribute("data-lat")
                    longitude = lista_estabelecimento[i].get_attribute("data-lng")
                except:
                    latitude = '-0'
                    longitude = '-0'
                try:
                    CAMADA1 = lista_endereco[i].text.split(',')
                        
                    cidade_uf = CAMADA1[-2]
                    EstabelecimentosAlelo['Estabelecimento'].append(lista_nome[i].text)
                    
                    EstabelecimentosAlelo["Endereço"].append(lista_endereco[i].text)
                    EstabelecimentosAlelo["Cidade_UF"].append(cidade_uf)
                    EstabelecimentosAlelo["Telefone"].append(telefone_atual)  
                    EstabelecimentosAlelo["Latitude"].append(latitude)
                    EstabelecimentosAlelo["Longitude"].append(longitude)
                    # Transforma tudo em um dataframe
                    dados = pd.DataFrame(data=EstabelecimentosAlelo)
                    # Converte tudo em um CSV bonitinho
                    dados.to_csv(f'BaseCsv/{arquivocsv}', mode='a', index=False, header=False, encoding='utf-8')
                    
                    # Limpando o df
                    dados = pd.DataFrame()
                    # Limpando o dicionário
                    EstabelecimentosAlelo = {
                                "Estabelecimento" : [],
                                "Endereço" : [],
                                "Cidade_UF" : [],
                                "Telefone" : [],
                                "Latitude" : [],
                                "Longitude" : []}
                #time.sleep(0.5)
                except:
                    continue
        else:
            #print(type(len(lista_estabelecimentos)))
            #print(len(lista_estabelecimentos))
            continue

        # Transforma tudo em um dataframe
        dados = pd.DataFrame(data=EstabelecimentosAlelo)
        # Converte tudo em um CSV bonitinho
        dados.to_csv(f'BaseCsv/{arquivocsv}', mode='a', index=False, header=False, encoding='utf-8')

    # limpando dados da memória
    
    gc.collect()    

    # Finaliza o processo
    driver.close()