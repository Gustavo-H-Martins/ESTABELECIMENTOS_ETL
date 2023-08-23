import os
import requests
from dotenv import load_dotenv

# carrega as variáveis de ambientes no script
load_dotenv(".env")

# carrega a variável de ambiente no script
URL_VR = os.environ.get("URL_VR")
ORIGIN_VR = os.environ.get("ORIGIN_VR")

def get_vr(latitude:str="-19.919052", longitude:str="-43.9386685", raio:str="10" ,distancia:str="10"):
    url = URL_VR
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

    data = requests.get(url, headers=headers, params=params)
    base = []
    
    for d in data.json():
        base.append({
            "CNPJ" : d["cnpj"],
            "RAZAO_SOCIAL" : d.get("razaoSocial", None),
            "ESTABELECIMENTOS" : d["nomeFantasia"], 
            "ENDERECO" : d.get("tipoEnd", "").upper() +  d.get("endereco", "").upper() + " " + d.get("Numero", ""),
            "BAIRRO" : d["bairro"], 
            "CIDADE" : d["cidade"], 
            "UF" : d["estado"], 
            "CEP" : d["cep"], 
            "TELEFONE": d.get("ddd", "") + ", " + d.get("telefone",""),
            "EMAIL": d.get("email", None),
            "LATITUDE" : d.get("latitude", None), 
            "LONGITUDE" : d.get("longitude", None),
            "BANDEIRA": "VR"
        })
    return base
