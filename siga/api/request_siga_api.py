# libs 
import os
from dotenv import load_dotenv

# carrega variáveis de ambiente no script
load_dotenv()

# carrega a variável da chave de api no script
API_KEY_LEADS_SIGA = os.environ.get("API_KEY_LEADS_SIGA")
URL_SIGA = os.environ.get("URL_SIGA")
def get_siga(chave:str = API_KEY_LEADS_SIGA):
    import requests
    #Conexão com api siga.
    params={
        "chave_abrasel":chave,
        "accept" : "*",
        }
    response = requests.get(URL_SIGA,headers=params, verify=False)
    data = response.json()
    base = []
    for d in data:
        base.append({
            "SEC_REG": d.get("S/R", "não informado").upper(),
            "NOME_FANTASIA": d.get("Nome Fantasia", ""),
            "CNPJ": d.get("CNPJ", "não informado"),
            "ENDERECO": f"{d.get('Logradouro', '')}, {d.get('Numero', 'SN')} {d.get('Comp.', '')}".upper(),
            "BAIRRO": d.get("Bairro", "não informado"),
            "CEP": d.get("CEP", "não informado"),
            "CIDADE": d.get("Cidade", "não informado"),
            "UF": d.get("UF", "não informado"),
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