def  get_establishments(latitude:str = '-19.6447323', longitde:str = '-43.9044951', raio:str = '15', delivery:str = 'false', cartao:str = '516'):
    # libs utilizadas
    import requests
    import json
    url = "https://www.sodexobeneficios.com.br/sodexo/rest/accreditedNetwork/ws.accreditedNetwork.searchByProductAndAddress"

    # parâmetros da consulta    
    params ={
    'product': f'{cartao}',
    'hasDelivery': f'{delivery}',
    'proximity': f'{raio}km',
    'lat': f'{latitude}',
    'lon': f'{longitde}',
    'startAt': '0',
    'limit': '25'
    }
    # cabeçalho da requisição por algum motivo precisa de tudo isso
    headers = {
    'authority': 'www.sodexobeneficios.com.br',
    'method': 'POST',
    'path': '/sodexo/rest/accreditedNetwork/ws.accreditedNetwork.searchByProductAndAddress',
    'scheme': 'https',
    'accept': 'application/json, text/javascript, */*; q=0.01',
    'accept-encoding': 'gzip, deflate, br',
    'accept-language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
    'content-length': '86',
    'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
    'origin': 'https://www.sodexobeneficios.com.br',
    'referer': 'https://www.sodexobeneficios.com.br/',
    'sec-ch-ua': '"Google Chrome";v="111", "Not(A:Brand";v="8", "Chromium";v="111"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36',
    'x-requested-with': 'XMLHttpRequest',
    'Cookie': 'JSESSIONID=84F7489EF41724D9811208C3ED0DA96B.lumis1; incap_ses_1615_2051948=JzcbFpqhUXS4Jm5nMKJpFgZsNWQAAAAAlyFU2XpUVTBwSwTQTCqDXA==; lumClientId=2C9FB039875DF62401877093EF4A2DFA; lumIsLoggedUser=false; lumUserLocale=pt_BR; lumUserName=Guest; lumUserSessionId=5orjJqolZqs9QIK_vkMK1sCOKsIGW3n3; nlbi_2051948=cN/dVBXu81Xb8KZr8iXQLgAAAACEKF9bI7ozizjlufK2CFAI; visid_incap_2051948=XXjYRmlFQ+6tmGtxJed75xplNWQAAAAAQUIPAAAAAABTAZX/PGXPNqAxpDuPEA8h; AWSALB=78HqhwGMK2U2L9lBe3LO/pVOXp4NY7z5QgV6rN+0dROyQjjFzDFGvE5CiOfnG9MstHFZJgwu01YKeN1bLRkvJacnQWiAItOMyENkPyhZXu/YbXQBOjVlRLD8ogV6; AWSALBCORS=78HqhwGMK2U2L9lBe3LO/pVOXp4NY7z5QgV6rN+0dROyQjjFzDFGvE5CiOfnG9MstHFZJgwu01YKeN1bLRkvJacnQWiAItOMyENkPyhZXu/YbXQBOjVlRLD8ogV6'
    }

    results = []
    total = -1
    startAt = 0
    limit = 25
    while total == -1 or startAt < total:
        # a chamada em si
        response = requests.post(url, headers=headers, data=params)
        # retorna uma zoeira de dados, então vamos manipular e pegar só o que podemos usar
        data = json.loads(response.json()['responseData'])
        hits = data['hits']['hits']
        dados = []
        for hit in hits:
            source = hit["_source"]
            establishment = {
                "ESTABELECIMENTO": source["fantasyname"],
                "RAZAO_SOCIAL": source["socialname"],
                "TELEFONE": source["phones"],
                "EMAIL": source["email"],
                "SITE": source["webpage"],
                "TIPO_LOGRADOURO": source['place'],
                "LOGRADOURO": source["address"],
                "NUMERO": source["number"],
                "COMPLEMENTO": source["complement"],
                "BAIRRO": source["town"],
                "CIDADE": source["city"],
                "ESTADO": source["state"],
                "CEP": source["zipcode"],
                "LATITUDE": source['location']['lat'],
                "LONGITUDE":source['location']['lon'],
                "BANDEIRA": hit['_index'],
                "LISTA_NEGRA":source['blacklist']
            }
            dados.append(establishment)
        # Retorna um json que é aqui vou reparar e buscar somente o campo "hits"
        results.extend(dados)
        total = data['hits']['total']
        startAt += limit
        params['startAt'] = str(startAt)
    #print(f'Total de dados: {len(results)}')
    return results
