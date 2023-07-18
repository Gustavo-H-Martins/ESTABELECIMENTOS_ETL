import os
import requests
from dotenv import load_dotenv

# carrega as variáveis de ambientes no script
load_dotenv(".env")

# carrega a variável de ambiente no script
URL_VR = os.getenv("URL_VR")
ORIGIN_VR = os.getenv("ORIGIN_VR")

def get_vr(latitude:str="-19.919052", longitude:str="-43.9386685", raio:str="10" ,distancia:str="10"):
    url = URL_VR
    payload={}
    # parâmetros da consulta    
    params = {
        "lat": latitude,
        "lon": longitude,
        "raio": raio,
        "distancia": distancia,
        "produto": "31",
        "termo":""
    }
    headers = {
        "origin": ORIGIN_VR,
        "referer": f"{ORIGIN_VR}/",
    }

    response = requests.get(url, headers=headers, params=params)
    data = response.json()["vr_estabelecimentos"]
    base = []
    for d in data:
        base.append({
            "CNPJ" : d["cnpj"],
            "RAZAO_SOCIAL" : d.get("razaoSocial", None),
            "ESTABELECIMENTOS" : d["nome"], 
            "ENDERECO" : d["endereco"], 
            "BAIRRO" : d["bairro"], 
            "CIDADE" : d["cidade"], 
            "UF" : d["estado"], 
            "CEP" : d["cep"], 
            "TELEFONE" : d["telefone"], 
            "EMAIL": d.get("email", None),
            "LATITUDE" : d["localizacao"].split(",")[0], 
            "LONGITUDE" : d["localizacao"].split(",")[1],
            "BANDEIRA": "VR"
        })
    return base
