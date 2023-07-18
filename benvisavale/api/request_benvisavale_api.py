import os
import requests
from dotenv import load_dotenv

# carrega as variáveis de ambientes no script
load_dotenv(".env")

# carrega a variável de ambiente no script
CODE_BENVISAVALE = os.getenv('CODE_BENVISAVALE')
URL_BENVISAVALE = os.getenv("URL_BENVISAVALE")
ORIGIN_BENVISAVALE = os.getenv("ORIGIN_BENVISAVALE")

def get_estabelecimentos(latitude:str = "-23.5673865", longitude:str = "-46.5703831821127", raio:float = 5):
    """Resumo para a função get_estabelecimentos

    Args:
        latitude (float, optional): número float negativo representando o ponto de latitude para a  geolocalização
        longitude (float, optional): número float negativo representando o ponto de longitude para a geolocalização.
        raio (float, optional): número float positivo representando o raio de busca dos dados na api.
    """
    url = f"{URL_BENVISAVALE}{CODE_BENVISAVALE}"

    payload = {
        "resourcePath": "estabelecimentos",
        "parameters": {
            "geoinfo.lat": latitude,
            "geoinfo.lng": longitude,
            "geoinfo.rad": raio,
            "size": 10000,
            "produtos": "2",
            "page": 0
        }
    }

    headers = {
        "Content-type": "application/json; charset=UTF-8",
        "Origin": ORIGIN_BENVISAVALE,
        "Referer": f"{ORIGIN_BENVISAVALE}/",
    }

    response = requests.post(url, headers=headers, json=payload)
    # Retorna um json que é aqui vou reparar e buscar somente o campo "content"
    data = response.json()["content"]
    base = []
    for d in data:
        base.append({
            "CNPJ" : d.get('cnpj', None),
            "RAZAO_SOCIAL" : d['nome'],
            "ESTABELECIMENTOS": d["nomeFantasia"],
            "ENDERECO": d["logradouro"] + ", " + d["numero"],
            "BAIRRO": d["bairro"],
            "CIDADE": d["cidade"],
            "UF": d["uf"],
            "CEP": d["cep"],
            "EMAIL": d.get("email", ""),
            "TELEFONE": d["telefones"][0]["ddd"]+ " " + d["telefones"][0]["telefone"] if d["telefones"] else "",
            "LATITUDE": d["geoinfo"]["lat"],
            "LONGITUDE": d["geoinfo"]["lng"],
            "BANDEIRA": "BENVISAVALE"
        })
    return base
