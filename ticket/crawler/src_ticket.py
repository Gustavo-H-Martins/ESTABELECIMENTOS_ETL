"""libs"""
import time
import datetime
from fake_useragent import UserAgent
import ctypes
import gc
import re
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import logging

# gerando log
logging.basicConfig(level=logging.INFO, filename="src_ticket.log",
                    encoding='utf-8', format="%(asctime)s - %(levelname)s - %(message)s")
hoje = datetime.datetime.today().strftime("%B")
arquivocsv = f'{hoje}_Estabelecimentos_Ticket.csv'

# pegando a resolução da tela

user32 = ctypes.windll.user32
resolucao = user32.GetSystemMetrics(0), user32.GetSystemMetrics(1)

"""
   Omite o Navegador na Execução
"""
chrome_options = Options()
#chrome_options.add_argument('--headless')
#chrome_options.add_experimental_option(
#    "prefs", {"profile.default_content_setting_values.geolocation": 1})
chrome_options.add_experimental_option("excludeSwitches", ["enable-automation","enable-logging"])
chrome_options.add_experimental_option('useAutomationExtension', False)
chrome_options.add_argument(f'--window-size={resolucao[0]},{resolucao[1]}')
chrome_options.add_argument("start-maximized")
prefs = {
        'profile.default_content_setting_values':
            {
                'notifications': 1,
                'geolocation': 1
            },
        'devtools.preferences': {
            'emulation.geolocationOverride': "\"11.111698@-122.222954:\"",
        },
        'profile.content_settings.exceptions.geolocation':{
            'BaseUrls.Root.AbsoluteUri': {
                'last_modified': '13160237885099795',
                'setting': '1'
            }
        },
        'profile.geolocation.default_content_setting': 1

    }

chrome_options.add_experimental_option('prefs', prefs)
    # Limpando saída do terminal
    #os.system('cls')

def base_estabelecimentos_ticket(municipio:str = 'Lagoa Santa	MG'):
    
    # Montando o dicionário que vai receber os dados que posteriormente vira o arquivo csv
    EstabelecimentosTicket = {
                    "Estabelecimento" : [],
                    "Endereço" : [],
                    "Cidade_UF" : [],
                    "Telefone" : []}

    # Lenndo cada linha
    municipio = municipio.split('\t')
    cidade = re.sub("\n","",municipio[0])
    uf = re.sub("\n","",municipio[1])

       # Parâmetros do navegador
    
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)

    driver.get("https://www.ticket.com.br/portal-usuario/rede-credenciada")
    driver.execute_cdp_cmd("Page.setBypassCSP", {"enabled": True})
    driver.implicitly_wait(15)
    driver.maximize_window()
    actions = ActionChains(driver, duration=100)
    #driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    #driver.execute_cdp_cmd('Network.setUserAgentOverride', {"userAgent": userAgent})

    # Aceita usar minha localização
    WebDriverWait(driver,10).until(EC.presence_of_element_located((By.XPATH,"//button"))).click()
    time.sleep(2)
    #location = driver.find_element(By.CSS_SELECTOR,'button.btn.btn-geolocation.text-left.mt-3')
    #print(location.location)
    #location.click()
    #actions.move_to_element(location).click(location).perform()

    
    # Botão Filtro
    driver.find_element(By.CLASS_NAME,"btn-filter").click()
    #driver.find_element(By.XPATH,"//*/text()[normalize-space(.)='Filtro']/parent::*").click()
    time.sleep(0.1)
    
    # Mostrar Mais
    driver.find_element(By.XPATH,"//a[contains(.,' Mostrar mais')]").click()
    time.sleep(0.1)
    
    # Aumentando a área de busca
    area = driver.find_element(By.CLASS_NAME, 'ngx-slider-span.ngx-slider-bar')
    actions.move_to_element(area).move_by_offset(150, 0).pause(0.25).click().perform()
    
    time.sleep(0.1)
    # Encontrando o Formulário
    formulario = driver.find_element(By.XPATH,"//input[@type='search']")
    time.sleep(0.1)    
    # Preenchendo Formulário
    actions.move_to_element(formulario).click()
    time.sleep(0.1)    
    """
    actions.send_keys_to_element(formulario,f'{cidade}, {uf.upper()}, Brasil').move_by_offset(0,15).pause(0.5).click().perform()
    time.sleep(0.5)
    """
    
    actions.send_keys_to_element(formulario, f'{cidade}, {uf.upper()}, Brasil').pause(
        0.25).send_keys(Keys.ARROW_DOWN).send_keys(Keys.ENTER).perform()
    time.sleep(1)

    # Botão Filtro novamente
    #driver.find_element(By.XPATH,"//*/text()[normalize-space(.)='Filtro']/parent::*").click()
    driver.find_element(By.CLASS_NAME,"active-red").click()
    
    time.sleep(0.5)
    count_restaurants = driver.find_element(By.CLASS_NAME,'count-restaurants').text.split()
    #print(type(count_restaurants))
    time.sleep(0.5)
    count_restaurants = int(count_restaurants[3])
    #print(type(count_restaurants))
    dados_coletados = 0
    continua = 'SIM'
    while continua == 'SIM':
        # Listando dados da pagina
        Lista_Estabelecimentos = driver.find_elements(By.CLASS_NAME,'list-group-item')
        contador_estabelecimentos = 1
        for i in range(contador_estabelecimentos, len(Lista_Estabelecimentos)+1):
            
            estabelecimento = driver.find_element(By.XPATH,f'//li[{i}]/div')
            #print(estabelecimento.text.splitlines())
            info = estabelecimento.text.splitlines()
            
                
            # Adicionando valor ao contador
            contador_estabelecimentos += 1
            #print(contador_estabelecimentos)
            if len(info) > 1:
                EstabelecimentosTicket['Estabelecimento'].append(info[0])
                EstabelecimentosTicket['Endereço'].append(info[1])
                EstabelecimentosTicket['Cidade_UF'].append(f'{cidade} - {uf}')
                # Detalhes do estabelecimento
                estabelecimento.click()

                detalhes = driver.find_elements(By.CLASS_NAME,'text-small')
                if len(detalhes) >= 2:
                    EstabelecimentosTicket['Telefone'].append(detalhes[1].text)
                else:
                    EstabelecimentosTicket['Telefone'].append('Indisponível')
                
                # Volta para lista de estabelecimentos
                driver.find_element(By.LINK_TEXT,"Voltar para a lista de resultados").click()
            else:
                dados_coletados = dados_coletados + 1
                continue
        dados_coletados = dados_coletados + len(EstabelecimentosTicket['Estabelecimento']) 
        #print(dados_coletados)
        # Transforma tudo em um dataframe
        dados = pd.DataFrame(data=EstabelecimentosTicket)
        # Converte tudo em um CSV bonitinho
        dados.to_csv(f'BaseCsv/{arquivocsv}', mode='a', index=False, header=False, encoding='utf-8')
        
        # Limpando o df
        dados = pd.DataFrame()
        # Limpando o dicionário
        EstabelecimentosTicket = {
                    "Estabelecimento" : [],
                    "Endereço" : [],
                    "Cidade_UF" : [],
                    "Telefone" : []}
        if count_restaurants > dados_coletados +1: #len(EstabelecimentosTicket['Estabelecimento'])+1:
            if count_restaurants > 50:
                driver.find_element(By.CSS_SELECTOR,'li.pagination-next > a').click()
            else:
                continua = 'NÃO'
        else:
            continua = 'NÃO'

    print(f"Extraídos {dados_coletados} de {cidade} - {uf} {datetime.datetime.today().strftime('%X')}")
    logging.info(f"Extraídos {dados_coletados} de {cidade} - {uf}")
    gc.collect()
    driver.close()
    #return f'Extraídos {dados_coletados} de {cidade} - {uf}'
    