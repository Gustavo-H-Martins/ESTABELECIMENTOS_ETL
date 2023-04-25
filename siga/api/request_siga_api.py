


def get_siga(chave:str = '8FBC66AB932E18D3AD244A87948EF144226eb1f08beac449f08bc8eb2353001fd426FB844AEDFFE131AA47ADB34EA6995a9216fa79d4d04a40643bfd85904f8'):
    import requests
    #Conexão com api siga.
    chave={
        'chave_abrasel':chave,
        'accept' : '*',
        }
    response = requests.get('https://siga.abrasel.com.br/tools/wsv/associados.jwsv',headers=chave)
    base = response.json()
    return base


#Visualizar dados.
def visualizar_associados_ativos():
    import pandas as pd
    import duckdb as db
    associados=pd.DataFrame(get_siga())
    return db.query("""select * from associados """).to_df()