# Modulos e libs
from request_api_cadastur import get_cadastur
import pandas as pd
import os
import logging
import sqlite3

# obteendo o caminho do diretório atual e construindo o caminho do arquivo a partir dele
current_dir = os.path.dirname(os.path.abspath(__file__))
db_file = current_dir.replace(r"cadastur/api", r"app/files/database.db")
db_dir = current_dir.replace(r"cadastur/api", r"app/files")
file_dados = current_dir.replace(r"api", r"dados/base_cadastur.csv")
file_logs = current_dir.replace(r"cadastur/api",r"logs/cadastur.log")
file_dir = current_dir.replace(r"cadastur/api",r"logs")


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
base = get_cadastur()
dados = pd.DataFrame(base)
dados["CNPJ"].sort_values(ascending=False)
dados.drop_duplicates(subset=["CNPJ"], inplace=True)

# Verificar se a coluna SITE tem "@" e mover o valor para a coluna INSTAGRAM
dados["SITE"] = dados["SITE"].fillna("")
dados["BAIRRO"] = dados["BAIRRO"].fillna("")
dados.loc[dados["SITE"].str.contains("@"), "INSTAGRAM"] = dados["SITE"].str.lower()
dados.loc[dados["SITE"].str.contains("@"), "SITE"] = ""

# Verificar se a coluna SITE tem "facebook" e mover o valor para a coluna FACEBOOK
dados.loc[dados["SITE"].str.contains("facebook"), "FACEBOOK"] = dados["SITE"].str.lower()
dados.loc[dados["SITE"].str.contains("facebook"), "SITE"] = ""

# Preencher as colunas INSTAGRAM e FACEBOOK com uma string vazia onde não houver um valor compatível na coluna SITE
dados["INSTAGRAM"] = dados["INSTAGRAM"].fillna("")
dados["FACEBOOK"] = dados["FACEBOOK"].fillna("")

# Mostrar o resultado final
dados.columns = dados.columns.str.strip()
dados = dados[['CNPJ', 'CNPJ_FORMATADO', 'NOME_FANTASIA', 'RAZAO_SOCIAL',
       'INICIO_VIGENCIA', 'FIM_VIGENCIA', 'SITE','INSTAGRAM', 'FACEBOOK', 
       'TELEFONE', 'CEP', 'ENDERECO', 'BAIRRO', 'CIDADE', 'UF', 'ATIVIDADE',
       'COD_SITUACAO_CADASTRAL', 'SITUACAO_CADASTRAL', 'ID_PRESTADOR',
       'URL_DETALHES_PRESTADOR']]

dados.to_sql("tb_cadastur", conn, 
    if_exists="replace", index=False,
    dtype={
        "CNPJ": "TEXT PRIMARY KEY",'CNPJ_FORMATADO': "TEXT", "NOME_FANTASIA" : "TEXT",
        "RAZAO_SOCIAL" : "TEXT", "INICIO_VIGENCIA" : "TEXT", "FIM_VIGENCIA" : "TEXT", 
        "SITE": "TEXT", "TELEFONE" : "TEXT","CEP": "TEXT",
        "ENDERECO": "TEXT", "BAIRRO": "TEXT", 
        "CIDADE" : "TEXT", "UF":"TEXT", 
        "ATIVIDADE":"TEXT", "COD_SITUACAO_CADASTRAL":"INT",
        "SITUACAO_CADASTRAL": "TEXT", "ID_PRESTADOR":"INT",
        "URL_DETALHES_PRESTADOR": "TEXT"
    })

dados.to_csv(file_dados, header=True, index=False, encoding="utf-8", sep=";")