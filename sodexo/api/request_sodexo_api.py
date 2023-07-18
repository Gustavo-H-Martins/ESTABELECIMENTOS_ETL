import os
import requests
import json
from dotenv import load_dotenv

# carrega as variáveis de ambientes no script
load_dotenv(".env")

# carrega a variável de ambiente no script
URL_SODEXO = os.getenv("URL_SODEXO")
ORIGIN_SODEXO = os.getenv("ORIGIN_SODEXO")

def get_establishments(latitude:str = "-19.6447323", longitude:str = "-43.9044951", raio:str = "15", delivery:str = "false", cartao:str = "516"):
    """RESUMO DA FUNÇÃO get_establishments

    ARGUMENTOS:
        latitude (str, optional): LATITUDE PARA BUSCAR DADOS NA API. Defaults to "-19.6447323".
        longitde (str, optional): LONGITUDE PARA BUSCAR DADOS NA API. Defaults to "-43.9044951".
        raio (str, optional): RAIO quadrado onde vamos mapear os estabelecimentos. Defaults to "15".
        delivery (str, optional): trabalha com delivery sim ou não ?. Defaults to "false".
        cartao (str, optional): tipo de produto utilizado em uma lista que é [516, 512, 526, 5267 etc]. Defaults to "516".

    RETORNO:
        results (json): retorna um json com as observações 
        ["ESTABELECIMENTOS" , "RAZAO_SOCIAL",  "TELEFONE",
        "EMAIL" , "SITE"  ,"TIPO_LOGRADOURO",  "LOGRADOURO",
        "NUMERO" , "COMPLEMENTO" , "BAIRRO",  "MUNICIPIO",  "UF",
        "CEP" , "LATITUDE" , "LONGITUDE" , "BANDEIRA" , "LISTA_NEGRA"]
    """
    url = URL_SODEXO

    # parâmetros da consulta    
    params ={
        "product": f"{cartao}",
        "hasDelivery": f"{delivery}",
        "proximity": f"{raio}km",
        "lat": f"{latitude}",
        "lon": f"{longitude}",
        "startAt": "0",
        "limit": "25"
    }
    # cabeçalho da requisição por algum motivo precisa de tudo isso
    headers = {
        "accept": "application/json, text/javascript, */*; q=0.01",
        "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
        "origin": ORIGIN_SODEXO,
        "referer": f"{ORIGIN_SODEXO}/",
        "x-requested-with": "XMLHttpRequest",
    }

    results = []
    total = -1
    startAt = 0
    limit = 25
    while total == -1 or startAt < total:
        # a chamada em si
        response = requests.post(url, headers=headers, data=params)
        try:
            # retorna uma zoeira de dados, então vamos manipular e pegar só o que podemos usar
            data = json.loads(response.json()["responseData"])
            hits = data["hits"]["hits"]
            dados = []
            for hit in hits:
                source = hit["_source"]
                establishment = {
                    "CNPJ" : source.get("cnpj", None),
                    "RAZAO_SOCIAL": source["socialname"],
                    "ESTABELECIMENTOS": source["fantasyname"],
                    "ENDERECO":  source["place"] + " " + source["address"] + " " + source["number"] + " " + source["complement"],
                    "BAIRRO": source["town"],
                    "CIDADE": source["city"],
                    "UF": source["state"],
                    "CEP": source["zipcode"],
                    "TELEFONE": source["phones"],
                    "EMAIL": source["email"],
                    "LATITUDE": source["location"]["lat"],
                    "LONGITUDE":source["location"]["lon"],
                    "BANDEIRA": hit["_index"].upper(),  
                }
                dados.append(establishment)
            # Retorna um json que é aqui vou reparar e buscar somente o campo "hits"
            results.extend(dados)
            total = data["hits"]["total"]
            startAt += limit
            params["startAt"] = str(startAt)
        except Exception as e:
            print(f"deu erro aqui : [{e}: {response.text}]")
            pass

    return results