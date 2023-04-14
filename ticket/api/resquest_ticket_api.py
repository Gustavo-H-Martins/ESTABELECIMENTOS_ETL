def get_ticket(longitude:str = '-43.9185698', latitude:str = '-19.9360307', tipo:list= ["tre"], raio_metros:str = '10000'):
    import requests
    import json

    url = "https://api.ticket.com.br/digital_redecredenciada/v2/estabelecimentos"

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
    'Accept': 'application/json, text/plain, */*',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
    'Authorization': 'Bearer',
    'Connection': 'keep-alive',
    'Content-Type': 'application/json',
    'Host': 'api.ticket.com.br',
    'Origin': 'https://www.ticket.com.br',
    'Referer': 'https://www.ticket.com.br/',
    'Request-Id': '8c6ca81a-a1d3-4f67-af9a-e1ac76fe078c',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-site',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36',
    'sec-ch-ua': '"Google Chrome";v="111", "Not(A:Brand";v="8", "Chromium";v="111"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"'
    }

    response = requests.post(url, headers=headers, data=params)
    values = response.json()['value']
    base = []
    for value in values:
        estabelecimentos = {
            "CNPJ" : value['id'],
            "RAZAO_SOCIAL" : value['razaoSocial'], 
            "ESTABELECIMENTO" : value['nomeEstabelecimento'], 
            "ENDERECO" : value['endereco'], 
            "BAIRRO" : value['bairro'], 
            "CIDADE" : value['cidade'], 
            "UF" : value['estado'], 
            "CEP" : value['cep'], 
            "TELEFONE" : value['telefone'], 
            "LATITUDE" : value['latitude'], 
            "LONGITUDE" : value['longitude']
        }
        base.append(estabelecimentos)
    return base
    
