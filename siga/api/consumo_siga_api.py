# Modulos e libs
from request_siga_api import get_siga
import pandas as pd
import os
import logging
import sqlite3
# obteendo o caminho do diretório atual e construindo o caminho do arquivo a partir dele
"""current_dir = os.path.dirname(os.path.abspath(__file__))
current_dir = current_dir.replace("siga/api", "app/files/database.db")
db_dir = current_dir.replace("siga/api", "app/iles")
file_dados = current_dir.replace("api", "dados/base_siga.csv")
file_logs = current_dir.replace("siga/api","logs/siga.log")
file_dir = current_dir.replace("siga/api","logs")
"""

current_dir = os.path.normpath(os.path.dirname(os.path.abspath(__file__)))
db_dir = os.path.normpath(os.path.join(current_dir, '..', '..', 'app', 'files'))
db_file = os.path.normpath(os.path.join(db_dir, 'database.db'))
file_dados = os.path.normpath(os.path.join(current_dir, '..', 'dados', 'base_siga.csv'))
file_logs = os.path.normpath(os.path.join(current_dir, '..', '..', 'logs', 'siga.log'))
file_dir = os.path.normpath(os.path.join(current_dir, '..', '..', 'logs'))

def touch_file(filename):
    with open(filename, 'a'):
        os.utime(filename, None)


if not os.path.exists(db_dir):
                    os.makedirs(db_dir)

if not os.path.exists(file_dir):
                    os.makedirs(file_dir)

touch_file(db_file)

conn = sqlite3.connect(database=db_file)

# configurando o registro de logs
logging.basicConfig(level=logging.DEBUG, filename=file_logs,encoding="utf-8", format="%(asctime)s - %(levelname)s - %(message)s")
base = get_siga()
dados = pd.DataFrame(base)
dados["CNPJ"].sort_values(ascending=False)
dados.drop_duplicates(subset=["CNPJ"], inplace=True)
dados.to_sql("tb_siga", conn, 
    if_exists="replace", index=False,
    dtype={
        "CNPJ": "TEXT PRIMARY KEY", "RAZAO_SOCIAL" : "TEXT",
        "NOME_FANTASIA" : "TEXT", "SEC_REG": "TEXT",
        "ENDERECO": "TEXT", "BAIRRO": "TEXT", 
        "CEP": "TEXT", "CIDADE" : "TEXT", "UF":"TEXT", 
        "ASSOCIADO":"TEXT", "SOU_ABRASEL":"TEXT"
    })

dados.to_csv(file_dados, header=True, index=False, encoding="utf-8", sep=";")