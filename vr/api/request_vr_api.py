def get_vr(latitude:str='-19.919052', longitude:str='-43.9386685', raio:str='10' ,distancia:str='10'):
    import requests
    import json
    url = "https://mapaec.vrbeneficios.io/search/buscaec?"

    payload={}
    # parâmetros da consulta    
    params = {
        'lat': latitude,
        'lon': longitude,
        'raio': raio,
        'distancia': distancia,
        'produto': '00031',
        'termo':''
        }
    headers = {
    'accept': 'application/json, text/plain, */*',
    'accept-encoding': 'gzip, deflate, br',
    'accept-language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
    'origin': 'https://portal.vr.com.br',
    'referer': 'https://portal.vr.com.br/',
    'sec-ch-ua': '"Chromium";v="112", "Google Chrome";v="112", "Not:A-Brand";v="99"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'cross-site',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36'
    }

    response = requests.get(url, headers=headers, params=params)
    data = response.json()['vr_estabelecimentos']
    base = []
    for d in data:
        base.append({
            "CNPJ" : d['cnpj'],
            "RAZAO_SOCIAL" : d.get('razaoSocial', None),
            "ESTABELECIMENTOS" : d['nome'], 
            "ENDERECO" : d['endereco'], 
            "BAIRRO" : d['bairro'], 
            "CIDADE" : d['cidade'], 
            "UF" : d['estado'], 
            "CEP" : d['cep'], 
            "TELEFONE" : d['telefone'], 
            "EMAIL": d.get('email', None),
            "LATITUDE" : d['localizacao'].split(',')[0], 
            "LONGITUDE" : d['localizacao'].split(',')[1],
            "BANDEIRA": "VR"
        })
    return base
