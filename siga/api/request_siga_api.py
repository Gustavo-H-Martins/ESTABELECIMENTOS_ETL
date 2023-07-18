# libs 
import os
from dotenv import load_dotenv

env = os.path.dirname(os.path.abspath(__file__)).replace(r"siga\api", ".env")

# carrega variáveis de ambiente no script
load_dotenv(env)

# carrega a variável da chave de api no script
API_KEY_LEADS_SIGA = os.getenv("API_KEY_LEADS_SIGA")
URL_SIGA = os.getenv("URL_SIGA")
def get_siga(chave:str = API_KEY_LEADS_SIGA):
    import requests
    #Conexão com api siga.
    params={
        "chave_abrasel":chave,
        "accept" : "*",
        }
    response = requests.get(URL_SIGA,headers=params)
    data = response.json()
    base = []
    for d in data:
        base.append({
        "SEC_REG" : d["S/R"].upper() if d["S/R"] else "",
        "NOME_FANTASIA" : d["Nome Fantasia"].upper() if d["Nome Fantasia"] else "",
        "RAZAO_SOCIAL" : d["Razão Social"].upper() if d["Razão Social"] else "",
        "CNPJ" : d["CNPJ"] if d["CNPJ"] else "",
        "ENDERECO" : d["Logradouro"].upper() if d["Logradouro"] else "" + ", " + d["Numero"].upper() if d["Numero"] else "" + "" + d["Comp."].upper() if d["Comp."] else "",
        "BAIRRO" : d["Bairro"].upper() if d["Bairro"] else "",
        "CEP" : d["CEP"] if d["CEP"] else "" ,
        "CIDADE" : d["Cidade"].upper() if d["Cidade"] else "",
        "UF" : d["UF"].upper() if d["UF"] else "",
        "ASSOCIADO": d["Status"].upper(),
        "SOU_ABRASEL": d["Status_SouAbrasel"].upper()
        })
    return base


#Visualizar dados.
def visualizar_associados_ativos():
    import pandas as pd
    import duckdb as db
    associados=pd.DataFrame(get_siga())
    return db.query("""select * from associados """).to_df()