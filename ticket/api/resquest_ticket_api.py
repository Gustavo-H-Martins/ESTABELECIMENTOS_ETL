import os
import requests
import json
from dotenv import load_dotenv

# carrega as variáveis de ambientes no script
load_dotenv(".env")

# carrega a variável de ambiente no script
URL_TICKET = os.getenv("URL_TICKET")
ORIGIN_TICKET = os.getenv("ORIGIN_TICKET")

def get_ticket(longitude:str = "-43.9185698", latitude:str = "-19.9360307", tipo:list= ["tre"], raio_metros:str = "10000"):
    url = URL_TICKET

    params = json.dumps({
        "nomeEstabelecimento": None,
        "categoriaId": None,
        "raio": int(raio_metros),
        "qtdRegistro": 4000,
        "longitude": float(longitude),
        "latitude": float(latitude),
        "produtos": tipo,
        "qtdPularRegistro": 0
    })

    headers = {
        "Content-Type": "application/json",
        "Origin": ORIGIN_TICKET,
        "Referer": f"{ORIGIN_TICKET}/",
    }

    response = requests.post(url, headers=headers, data=params)
    values = response.json()["value"]
    base = []
    for value in values:
        estabelecimentos = {
            "CNPJ" : value["id"],
            "RAZAO_SOCIAL" : value["razaoSocial"], 
            "ESTABELECIMENTO" : value["nomeEstabelecimento"], 
            "ENDERECO" : value["endereco"], 
            "BAIRRO" : value["bairro"], 
            "CIDADE" : value["cidade"], 
            "UF" : value["estado"], 
            "CEP" : value["cep"], 
            "TELEFONE" : value["telefone"], 
            "LATITUDE" : value["latitude"], 
            "LONGITUDE" : value["longitude"]
        }
        base.append(estabelecimentos)
    return base
