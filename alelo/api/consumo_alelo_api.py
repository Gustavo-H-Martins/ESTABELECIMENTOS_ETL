# Modulos e libs
from request_alelo_api import get_token, get_establishments
import pandas as pd
import polars as pl
import multiprocessing
import time
import os
import logging

# obteendo o caminho do diretório atual e construindo o caminho do arquivo a partir dele
current_dir = os.getcwd()
file_localidades = current_dir.replace(r'alelo\api', r'localidades\localidades.txt')
file_dados = current_dir.replace(r'api', r'dados\BASE_ALELO.csv')

# configurando o registro de logs
logging.basicConfig(level=logging.DEBUG, filename="consumo_alelo.log",encoding='utf-8', format="%(asctime)s - %(levelname)s - %(message)s")

# Abrindo, lendo e salvando o arquivo com os municípios
with open(file_localidades,'r', encoding='UTF-8') as municipios:
    next(municipios)
    # salvando em uma lista todas as linhas
    localidades = municipios.readlines()

# Obetendo a chave de acesso
token = get_token()
print(token['access_token'])

# O for as vezes é melhor que o paralelismo
def processo(municipio):
    try:
        localidade = municipio.split('\t')
        logging.info(f'pegando dados de {localidade[0]}')
        base = get_establishments(token=token['access_token'],latitude=localidade[3].replace('\n', ''), longitude=localidade[2],raio=5)
        dados = pd.json_normalize(base)
        dados = dados[['establishmentName','address','district',
                    'cityName','stateName', 'zip','phoneAreaCode',
                    'phoneNumber','latitude', 'longitude']]
        dados.rename(columns={'establishmentName':'ESTABELECIMENTOS',
                                    'address':'ENDERECO', 'district': 'BAIRRO',
                                    'cityName':'MUNICIPIO', 'stateName': 'UF',
                                    'zip':'CEP','phoneAreaCode':'DDD','phoneNumber':'TELEFONE', 
                                    'latitude':'LATITUDE','longitude':'LONGITUDE' }, inplace=True)
        dados.to_csv(file_dados,header=False,sep=';', mode='a', encoding='utf-8', index=False)
    except Exception as e:
        logging.warning(f"deu erro aqui : [{e}] mas vams continuar a brincadeira")
        pass
if __name__ == '__main__':
    with multiprocessing.Pool(processes=3) as pool:
        time.sleep(5)
        pool.map(processo, localidades)