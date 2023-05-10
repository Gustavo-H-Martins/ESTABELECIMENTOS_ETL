# libs
import pandas as pd
import numpy as np
import datetime
import warnings
import os
import sqlite3
import logging
warnings.filterwarnings("ignore")
pd.option_context(10,5)

#define o caminho do diretório atual
current_dir = os.path.dirname(os.path.abspath(__file__))
file_logs = current_dir.replace(r"rfb\dados\csv",r"logs\rfb.log")
# configurando o registro de logs
logging.basicConfig(level=logging.DEBUG, filename=file_logs,encoding="utf-8", format="%(asctime)s - %(levelname)s - %(message)s")

# pega o arquivo gerado
base_rfb = current_dir + r"\BASE_RFB.csv"

# qual cabeçalho nós usamos mesmo?
cabecalho = ["CNPJ","RAZAO_SOCIAL","NOME_FANTASIA",
             "SITUACAO_CADASTRAL","DATA_SITUACAO_CADASTRAL",
             "DATA_INICIO_ATIVIDADE","CNAE_PRINCIPAL","ENDERECO",
             "BAIRRO","CIDADE","UF","CEP","TELEFONE","CNAE_DESCRICAO", "EMAIL"]

# carregada os dados no dataframe pandas aqui, simples né?
dados  = pd.read_csv(base_rfb, sep=";",usecols=cabecalho, dtype="string")

# a parte de transform de fato está toda aqui, bem simples:
# com quaanto de dadps começou?
logging.info(f"Tinham: {dados.shape[0]} dados")
# Remove os dados duplicados, estranho que sempre aparecem
dados.drop_duplicates(inplace=True, ignore_index=True)
dados.drop_duplicates(subset=["CNPJ"])
# coloca tudo em uppercase
dados["CNAE_DESCRICAO"] = dados["CNAE_DESCRICAO"].str.upper()
dados["ENDERECO"] = dados["ENDERECO"].str.strip()

# filtrando as colunas que vamos usar depois de toda a brincadeira
dados = dados[["CNPJ", "RAZAO_SOCIAL","NOME_FANTASIA", 
               "ENDERECO", "BAIRRO", "CIDADE", "UF", 
               "CEP", "TELEFONE", "EMAIL", "CNAE_PRINCIPAL",
               "CNAE_DESCRICAO","SITUACAO_CADASTRAL",
               "DATA_SITUACAO_CADASTRAL", "DATA_INICIO_ATIVIDADE"]]

# conta quando de dados sobrou
logging.info(f"ficaram: {dados.shape[0]} dados")


# Salva tudo novamente desta vez com um csv e no banco de dados, a galera gosta de "variedades"
dados.to_csv(base_rfb,sep=";", index=False, encoding="utf-8")
#Criar uma conexão com o banco de dados sqlite
db_file = current_dir.replace(r"rfb\dados\csv", r"app\files\database.db")
conn = sqlite3.connect(database=db_file)

#Converter o dataframe em uma tabela no banco de dados
"""
O parâmetro if_exists=`append` verifica se a tabela já existe e incrementa os dados
O parâmetro index=False evita que o índice do dataframe seja inserido na tabela
O parâmetro dtype define o tipo de cada coluna na tabela
"""
dados.to_sql("tb_rfb", conn, 
             if_exists="replace", index=False, 
             dtype={"CNPJ": "TEXT PRIMARY KEY", 
                    "RAZAO_SOCIAL": "TEXT", "NOME_FANTASIA": "TEXT", 
                    "ENDERECO": "TEXT", "BAIRRO": "TEXT", "CIDADE": "TEXT", 
                    "UF": "TEXT", "CEP": "TEXT", 
                    "TELEFONE": "TEXT", "EMAIL": "TEXT", 
                    "CNAE_PRINCIPAL": "TEXT", "CNAE_DESCRICAO": "TEXT",
                    "SITUACAO_CADASTRAL" : "TEXT", "DATA_SITUACAO_CADASTRAL" : "TEXT",
                    "DATA_INICIO_ATIVIDADE" : "TEXT"})
# Finaliza a transação
conn.commit()
# Executa o comando VACUUM para compactar o banco de dados
conn.execute("VACUUM")

# Fechar a conexão com o banco de dados
conn.close()