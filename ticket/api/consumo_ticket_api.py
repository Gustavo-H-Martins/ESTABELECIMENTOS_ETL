# Modulos e libs
from resquest_ticket_api import get_ticket
import polars as pl
import multiprocessing
import time
import os
import logging
from datetime import datetime
from backup_limpeza import backup_limpeza_simples

# obteendo o caminho do diretório atual e construindo o caminho do arquivo a partir dele
current_dir = os.path.dirname(os.path.abspath(__file__))
file_localidades = current_dir.replace(r'ticket\api', r'localidades\localidades.txt')
file_dados = current_dir.replace(r'api', r'dados\BASE_TICKET.csv')
file_logs = current_dir.replace(r'ticket\api',r'logs\ticket.log')
folder_dados = file_dados.replace(r'BASE_TICKET.csv', '')

# configurando o registro de logs
logging.basicConfig(level=logging.DEBUG, filename=file_logs,encoding='utf-8', format="%(asctime)s - %(levelname)s - %(message)s")

# difinindo datazip
datazip = datazip = f'{datetime.now().month}-{datetime.now().year}'

# Filtra todos os arquivos csv da pasta
arquivos_csv = list(filter(lambda x: '.csv' in x, os.listdir(folder_dados)))
# Se a pasta não estiver vazia faz o backup dos arquivos e limpa ela.
if len(arquivos_csv) >= 1:
        backup_limpeza_simples(pasta=folder_dados, nome_zipado=f'{folder_dados}ticket_{datazip}.zip')

# Abrindo, lendo e salvando o arquivo com os municípios
with open(file_localidades,'r', encoding='UTF-8') as municipios:
    next(municipios)
    # salvando em uma lista todas as linhas
    localidades = municipios.readlines()
# aplicando o paralelismo no processo de busca
def processo(municipio):
    try:
        localidade = municipio.split('\t')
        logging.info(f'pegando dados de {localidade[0]}')
        base = get_ticket(latitude=localidade[3].replace('\n', ''), longitude=localidade[2])
        dados = pl.DataFrame(base)
        # Salvar em csv em modo incremental
        if os.path.isfile(file_dados) and os.path.getsize(file_dados) > 0:
            """
                Se o arquivo já existir e tiver algum tamanho,
                escrever o dataframe sem cabeçalho
            """
            with open(file_dados, mode='ab') as f:
                dados.write_csv(f, has_header=False, separator=';')

        else:
            """
                Se o arquivo não existir ou estiver vazio,
                escrever o dataframe com cabeçalho
                Adicionar o dataframe ao arquivo CSV existente
            """
            dados.write_csv(file_dados, has_header=True, separator=';')

    except Exception as e:
        logging.warning(f"deu erro aqui : [{e}] mas vamos continuar a brincadeira")
        pass
if __name__ == '__main__':
    with multiprocessing.Pool(processes=3) as pool:
        pool.map(processo, localidades)
    