from request_alelo_api import get_token, get_establishments
import pandas as pd
import polars as pl
import multiprocessing
import time
municipios = open(r'./localidades/localidades.txt','r', encoding='UTF-8')
next(municipios)
municipios
token = get_token()
def processar(municipio):
    try:
        municipio = municipio.split('\t')
        dados = get_establishments(token=token['access_token'],latitude=municipio[3].replace('\n', ''), longitude=municipio[2],raio=5)
        dados.write_csv(r'../dados/BASE_ALELO.csv',has_header=False,separator=';', mode='a', encoding='utf-8')
    except Exception as e:
        # registra a posição do loop em que o erro ocorreu
        print(f"Erro na posição {municipio}: {e}")
        # ignora o erro e continua para o próximo item do loop
if __name__ == '__main__':
    with multiprocessing.Pool(processes=3) as pool:
        time.sleep(5)
        pool.map(processar, municipios)