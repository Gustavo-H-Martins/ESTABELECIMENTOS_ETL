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
# gerando log
logging.basicConfig(level=logging.INFO, filename="src_Sodexo.log",encoding='utf-8', format="%(asctime)s - %(levelname)s - %(message)s")

hoje = datetime.datetime.today().strftime("%B")
arquivocsv = f'{hoje}_Estabelecimentos_Sodexo.csv'

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
chrome_options.add_argument('--window-size=1366,768')
chrome_options.add_argument('--log-level=3')

def base_estabelecimentos_sodexo(listacidades_txt:str = 'teste.txt'):
    #listacidades_txt = 'teste.txt'

    """
        Coleta informações de estabelecimentos que aceitam cartões sodexo do tipo Refeição Pass
        Parâmetros : 
            Cidade & UF - Região em que iremos buscar as informações do painel Credenciados Sodexo

        Retorno:
            Retorna um dataframe ou csv com as observações
                - Estabelecimento
                - Endereço
                - Cidade/UF
                - Telefone
                - Latitude
                - Longitude

    """

    #driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)

    # Chamando o navegador
    #driver.get("https://www.sodexobeneficios.com.br/sodexo-club/rede-credenciada/")
    #driver.maximize_window()
    #time.sleep(1)
    # Instanciando operacional
    #actions = ActionChains(driver)
    #actions.move_to_element(driver.find_element(By.XPATH,'//*[@id="onetrust-accept-btn-handler"]')).click().perform()
    # Selecionando o tipo de cartão aceito
    #tipo_cartao = driver.find_element(By.XPATH, '//*[@id="516"]')
    #actions.move_to_element(tipo_cartao).click(tipo_cartao).perform()

    # Montando o dicionário que vai receber os dados que posteriormente vira o arquivo csv
    EstabelecimentosSodexo = {
                "Estabelecimento" : [],
                "Endereço" : [],
                "Cidade_UF" : [],
                "Telefone" : [],
                "Latitude" : [],
                "Longitude" : []}

    municipios = open(listacidades_txt,'r', encoding='UTF-8')
    next(municipios)
    for municipio in municipios.readlines()[745:]:
        municipio = municipio.split('\t')
        cidade = municipio[0]
        uf = municipio[1]

        
        
        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
        driver.get("https://www.sodexobeneficios.com.br/sodexo-club/rede-credenciada/")
        # limpando as saídas do terminal
        os.system('cls')
        actions = ActionChains(driver)
        time.sleep(1)
        # Aceitando o cache
        actions.move_to_element(driver.find_element(By.XPATH,'//*[@id="onetrust-accept-btn-handler"]')).click().perform()

        # Selecionando o tipo de cartão aceito
        tipo_cartao = driver.find_element(By.XPATH, '//*[@id="516"]')
        actions.move_to_element(tipo_cartao).click(tipo_cartao).perform()
        time.sleep(0.5)


        # Encontrando e preenchendo o Formulário de pesquisa por endereço
        formulario = driver.find_element(By.CSS_SELECTOR, 'input#endereco')
        actions.move_to_element(formulario)
        formulario.send_keys(cidade)
        formulario.send_keys(f', {uf.upper()}')
        actions.perform()
        formulario.send_keys(Keys.RETURN)
        time.sleep(0.5)
        #formulario.send_keys('\ue015')
        
        formulario.send_keys(Keys.RETURN)
        #actions.send_keys('\ue00f')
        actions.perform()
        
        # Botão Buscar
        buscar = driver.find_element(By.CLASS_NAME, 'primary-button')
        actions.move_to_element(buscar).click(buscar).perform()

        # se o elemento aparecer bem, se não amém
        if driver.find_element(By.CLASS_NAME, 'estabcount').is_displayed():

            # Coleta dos dados
            estabcount = driver.find_element(By.CLASS_NAME, 'estabcount').text
            search_item = int(estabcount) # contagem de estabelecimentos
            logging.info(f'Extração de {search_item} dados de estabelecimentos : {cidade} {uf}')

            # Definindo e verificando o número de estabelecimentos na barra de rolagem.
            try:
                pagination = driver.find_element(By.ID, 'pagination').text
                item = pagination.split() # contagem de estabelecimentos na página
                last_item = item[-1]
                page_item = int(last_item)
            except:
                continue

            time.sleep(0.5)
            # Loop que vai clicando e mudando as tabelas de cada formulário na página
            while page_item <= search_item:

                enderecos = driver.find_elements(By.CLASS_NAME, "info-estab")


                # Extraindo dados estabelecimentos
                #enderecos.find_elements(By.CLASS_NAME,'info-estab')
                for endereco in enderecos:
                    ROTA = endereco.find_element(By.TAG_NAME,'a').get_attribute("onclick").split(',')
                    parte = endereco.text.split('\n')
                    
                    #print(parte[0])
                    #print(parte[1])
                    #print(parte[2])
                    #print(parte[3])
                    #print(len(parte))
                    try:
                        # preenchendo endereços
                        EstabelecimentosSodexo['Estabelecimento'].append(parte[0])
                        EstabelecimentosSodexo['Endereço'].append(parte[1])
                        EstabelecimentosSodexo['Cidade_UF'].append(parte[2])
                        EstabelecimentosSodexo['Telefone'].append(parte[3])
                        # Preenchendo geolocalização
                        EstabelecimentosSodexo['Latitude'].append(ROTA[-2])
                        EstabelecimentosSodexo['Longitude'].append(re.sub("[)]","", ROTA[-1]))
                    except:
                        dados = pd.DataFrame(EstabelecimentosSodexo)                    
                        dados.to_csv(arquivocsv, mode='a', index=False, header=False, encoding='utf-8')
                        
                        # Esvaziando o dataframe e dicionário
                        dados = pd.DataFrame()
                        EstabelecimentosSodexo = {
                                            "Estabelecimento" : [],
                                            "Endereço" : [],
                                            "Cidade_UF" : [],
                                            "Telefone" : [],
                                            "Latitude" : [],
                                            "Longitude" : []}
                        try:
                            actions.move_to_element(driver.find_element(By.ID,'nextPage')).click().perform()
                        except:
                            print(f'Total de informações de estabelecimentos extraídas: {search_item}')
                            break

                # Mudando de página no formulário lateral
                if driver.find_element(By.ID,'nextPage').is_displayed():
                    actions.move_to_element(driver.find_element(By.ID,'nextPage')).click().perform()
                else:
                    logging.info(f'Total de informações de estabelecimentos extraídas: {search_item}')
                    # Montando o dataframe e convertendo em arquivo CSV
                    dados = pd.DataFrame(EstabelecimentosSodexo)                    
                    dados.to_csv(arquivocsv, mode='a', index=False, header=False)
                    
                    # Esvaziando o dataframe e dicionário
                    dados = pd.DataFrame()
                    EstabelecimentosSodexo = {
                                        "Estabelecimento" : [],
                                        "Endereço" : [],
                                        "Cidade_UF" : [],
                                        "Telefone" : [],
                                        "Latitude" : [],
                                        "Longitude" : []}
                    #actions.send_keys(Keys.PAGE_UP)
                    #time.sleep(1)
                    break
 
        else:
            continue 
    # Fechando o navegador
    time.sleep(2)
    driver.close()

    # Retorno 
    return print(f'Extração de {search_item} dados de {municipio}')

