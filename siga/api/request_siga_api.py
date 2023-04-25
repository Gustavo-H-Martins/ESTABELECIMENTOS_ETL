


def get_siga(chave:str = '8FBC66AB932E18D3AD244A87948EF144226eb1f08beac449f08bc8eb2353001fd426FB844AEDFFE131AA47ADB34EA6995a9216fa79d4d04a40643bfd85904f8'):
    import requests
    #Conexão com api siga.
    chave={
        'chave_abrasel':chave,
        'accept' : '*',
        }
    response = requests.get('https://siga.abrasel.com.br/tools/wsv/associados.jwsv',headers=chave)
    data = response.json()
    base = []
    for d in data:
        base.append({
        "SEC_REG" : d['S/R'].upper() if d['S/R'] else "",
        "NOME_FANTASIA" : d['Nome Fantasia'].upper() if d['Nome Fantasia'] else "",
        "RAZAO_SOCIAL" : d['Razão Social'].upper() if d["Razão Social"] else "",
        "CNPJ" : d['CNPJ'],
        "ENDERECO" : d['Logradouro'].upper() if d["Logradouro"] else "" + ', ' + d['Numero'].upper() if d["Numero"] else "" + '' + d['Comp.'].upper() if d["Comp."] else "",
        "BAIRRO" : d['Bairro'].upper() if d["Bairro"] else "",
        "CEP" : d['CEP'],
        "CIDADE" : d['Cidade'].upper() if d["Cidade"] else "",
        "UF" : d['UF'].upper() if d["UF"] else "",
        "ASSOCIADO": d['Status'].upper(),
        "SOU_ABRASEL": d['Status_SouAbrasel'].upper()
        })
    return base


#Visualizar dados.
def visualizar_associados_ativos():
    import pandas as pd
    import duckdb as db
    associados=pd.DataFrame(get_siga())
    return db.query("""select * from associados """).to_df()