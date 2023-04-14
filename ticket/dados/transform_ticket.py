# libs
import pandas as pd
import sqlite3
import warnings
import os
from unidecode import unidecode 
import logging
warnings.filterwarnings('ignore')
pd.option_context(10,5)

#define o caminho do diretório atual
current_dir = os.path.dirname(os.path.abspath(__file__))
file_logs = current_dir.replace(r'ticket\dados',r'logs\ticket.log')
# configurando o registro de logs
logging.basicConfig(level=logging.DEBUG, filename=file_logs,encoding='utf-8', format="%(asctime)s - %(levelname)s - %(message)s")

# pega o arquivo gerado
base_ticket = current_dir + r'\BASE_TICKET.csv'
base_ticket

# qual cabeçalho nós usamos mesmo?
cabecalho = ["CNPJ", "RAZAO_SOCIAL", 
            "ESTABELECIMENTO", "ENDERECO", 
            "BAIRRO", "CIDADE", 
            "UF", "CEP", "TELEFONE",
            "LATITUDE", "LONGITUDE"]

# carregada os dados no dataframe pandas aqui, simples né?
dados  = pd.read_csv(base_ticket, sep=';',names=cabecalho, dtype='object')


# a parte de transform de fato está toda aqui, bem simples:
# com quaanto de dadps começou?
logging.info(f'Tinham: {dados.shape[0]} dados')
# Remove os dados duplicados, estranho que sempre aparecem
dados.drop_duplicates(inplace=True, ignore_index=True)
# coloca tudo em uppercase
dados['ESTABELECIMENTOS'] = dados['ESTABELECIMENTO'].str.upper()
dados['ENDERECO'] = dados['ENDERECO'].str.upper()
dados['BAIRRO'] = dados['BAIRRO'].str.upper()
dados['CIDADE'] = dados['CIDADE'].str.upper()
dados['EMAIL'] = None
dados['BANDEIRA'] = 'TICKET'

# filtrando as colunas que vamos usar depois de toda a brincadeira
dados = dados[['CNPJ', 'ESTABELECIMENTOS', 'ENDERECO', 'BAIRRO', 'CIDADE', 'UF', 'CEP', 'TELEFONE', 'EMAIL', 'LATITUDE','LONGITUDE', 'BANDEIRA']]

# conta quando de dados sobrou
logging.info(f'ficaram: {dados.shape[0]} dados')

# tirando os telefones fakes ou sem valor interessante
telefone = []
for i in dados['TELEFONE']:
    if len(str(i)) < 7:
        telefone.append('Indisponível')
    else:
        telefone.append(str(i))
dados['TELEFONE'] = telefone


# conta quantos de dados tinham antes de tirar os telefones nulos
logging.info(f'ficaram: {dados.shape[0]} dados')
dados.drop(dados[dados['TELEFONE'] == 'Indisponível'].index, inplace=True)
# contando quantos ficaram depois de tirar os nulos
logging.info(f'ficaram: {dados.shape[0]} dados')

# Salva tudo novamente desta vez com um csv e no banco de dados, a galera gosta de "variedades"
dados.to_csv(base_ticket,sep=';', index=False, encoding='utf-8')
#Criar uma conexão com o banco de dados sqlite
db_file = current_dir.replace('ticket\dados', r'app\files\database.db')
conn = sqlite3.connect(database=db_file)

#Converter o dataframe em uma tabela no banco de dados
"""
O parâmetro if_exists=`append` verifica se a tabela já existe e incrementa os dados
O parâmetro index=False evita que o índice do dataframe seja inserido na tabela
O parâmetro dtype define o tipo de cada coluna na tabela
"""
dados.to_sql('tb_ticket', conn, 
             if_exists='append', index=False, 
             dtype={'CNPJ': 'TEXT PRIMARY KEY', 
                    'ESTABELECIMENTOS': 'TEXT', 'ENDERECO': 'TEXT', 
                    'BAIRRO': 'TEXT', 'CIDADE': 'TEXT', 'UF': 'TEXT', 
                    'CEP': 'TEXT', 'TELEFONE': 'TEXT', 
                    'EMAIL': 'TEXT', 'LATITUDE': 'TEXT', 
                    'LONGITUDE': 'TEXT', 'BANDEIRA': 'TEXT'})
# Finaliza a transação
conn.commit()

# Executa o comando VACUUM para compactar o banco de dados
conn.execute('VACUUM')

# Fechar a conexão com o banco de dados
conn.close()