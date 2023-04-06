def  get_estabelecimentos(latitude:float = -23.5673865, longitde:float = -46.5703831821127, raio:float = 5):
    """Resumo para a função get_estabelecimentos

    Args:
        latitude (float, optional): número float negativo representando o ponto de latitude para a  geolocalização
        longitde (float, optional): número float negativo representando o ponto de longitude para a geolocalização.
        raio (float, optional): número float positivo representando o raio de busca dos dados na api.
    """
    # libs utilizadas
    import requests
    import pandas as pd
    url = "https://ben-institucional-prd.azurewebsites.net/api/request?code=MqRNZcyopVyFNtUQ1BWNxnIHAiQDXRSnUC4k9xKNuDs7vabqkKi3eQ=="

    payload = {
        "resourcePath": "estabelecimentos",
        "parameters": {
            "geoinfo.lat": latitude,
            "geoinfo.lng": longitde,
            "geoinfo.rad": raio,
            "size": 10000,
            "produtos": "2",
            "page": 0
        }
    }

    headers = {
        "Accept": "*/*",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
        "Connection": "keep-alive",
        "Content-type": "application/json; charset=UTF-8",
        "Host": "ben-institucional-prd.azurewebsites.net",
        "Origin": "https://bensite.conductor.com.br",
        "Referer": "https://bensite.conductor.com.br/",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "cross-site",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36 Edg/114.0.0.0",
        "sec-ch-ua": "\"Not.A/Brand\";v=\"8\", \"Chromium\";v=\"113\", \"Microsoft Edge\";v=\"114\"",
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": "\"Windows\""
    }

    response = requests.post(url, headers=headers, json=payload)
    # Retorna um json que é aqui vou reparar e buscar somente o campo "content"
    base = response.json()["content"]
    return base
