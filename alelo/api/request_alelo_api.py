def post_establishments(longitude:str ='-46.6395571', latitude:str= '-23.5557714', raio:str = '10', page:str = '1', pageSize:str = '10000', product:str = '100'):
    """resumo para função get_establishments

    Args:
        longitude (str): passe longitude da localização onde vamos buscar os dados exemplo: -46.6395571
        latitude (str): passe a latitude da localização onde vamos buscar os dados exemplo: -23.5557714
        raio (str): distancia da localização onde vamos procurar os dados exemplo 10 = 10km de raio
        page (str, optional): o número da página que vamos acessar os dados . padrão é '1'.
        pageSize (str, optional): tamanho da página, quantidade máximo de dados retornados. padrão é '10000'.
        product (str, optional): aceita o parâmetro tipo de produto que é o tipo de cartão aceito. padrão é '100' que representa refeição.

    Returns:
        json: retorna um json com dados de estabelecimentos da base da provedora de cartão voucher Alelo
    """
    import requests

    url = f"https://api.alelo.com.br/alelo/prd/acceptance-network/establishments?longitude={longitude}&latitude={latitude}&distance={raio}&pageNumber={page}&pageSize={pageSize}&type=POSITION&product={product}"

    payload={}
    headers = {
    'Accept': '*/*',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
    'Access-Control-Request-Headers': 'authorization,x-ibm-client-id',
    'Access-Control-Request-Method': 'GET',
    'Connection': 'keep-alive',
    'Host': 'api.alelo.com.br',
    'Origin': 'https://redeaceitacao.alelo.com.br',
    'Referer': 'https://redeaceitacao.alelo.com.br/',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-site',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36',
    'Cookie': 'dtCookie=v_4_srv_3_sn_9BC5D4CCED2DEE026B6B076E754DC9C9_perc_100000_ol_0_mul_1_app-3Aea7c4b59f27d43eb_0; a7e2b1664582e63453352cdc14747b5e=149fc99cb62a2dfda246e6f3fc75b1f0; b4a8c8bd365a01af1ce16d38b368df35=320400dd5f58d59b1432b4a3d1135c04'
    }

    response = requests.request("OPTIONS", url, headers=headers, data=payload)

    print(response.text)

def get_establishments(token:str ,longitude:str ='-46.6395571', latitude:str= '-23.5557714', raio:str = '10', page:str = '1', pageSize:str = '10000', product:str = '100'):
    """resumo para função get_establishments

    Args:
        longitude (str): passe longitude da localização onde vamos buscar os dados exemplo: -46.6395571
        latitude (str): passe a latitude da localização onde vamos buscar os dados exemplo: -23.5557714
        raio (str): distancia da localização onde vamos procurar os dados exemplo 10 = 10km de raio
        page (str, optional): o número da página que vamos acessar os dados . padrão é '1'.
        pageSize (str, optional): tamanho da página, quantidade máximo de dados retornados. padrão é '10000'.
        product (str, optional): aceita o parâmetro tipo de produto que é o tipo de cartão aceito. padrão é '100' que representa refeição.

    Returns:
        json: retorna um json com dados de estabelecimentos da base da provedora de cartão voucher Alelo
    """
    import requests
    import pandas as pd
    import polars as pl
    url = f"https://api.alelo.com.br/alelo/prd/acceptance-network/establishments?longitude={longitude}&latitude={latitude}&distance={raio}&pageNumber={page}&pageSize={pageSize}&type=POSITION&product={product}"

    payload={}
    headers = {
    'Accept': 'application/json, text/plain, */*',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
    'Authorization': f'Bearer {token}',
    'Connection': 'keep-alive',
    'Host': 'api.alelo.com.br',
    'Origin': 'https://redeaceitacao.alelo.com.br',
    'Referer': 'https://redeaceitacao.alelo.com.br/',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-site',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36',
    'sec-ch-ua': '"Google Chrome";v="111", "Not(A:Brand";v="8", "Chromium";v="111"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'x-ibm-client-id': '76a775cd-8b2c-4ce8-b7a5-b321c66223f7',
    'Cookie': 'dtCookie=v_4_srv_3_sn_9BC5D4CCED2DEE026B6B076E754DC9C9_perc_100000_ol_0_mul_1_app-3Aea7c4b59f27d43eb_0; a7e2b1664582e63453352cdc14747b5e=149fc99cb62a2dfda246e6f3fc75b1f0; b4a8c8bd365a01af1ce16d38b368df35=320400dd5f58d59b1432b4a3d1135c04'
    }

    response = requests.request("GET", url, headers=headers, data=payload)

    base =  response.json()['establishments']
    dados = pl.from_pandas(pd.json_normalize(base))
    dados = dados.select(['establishmentName','address','district','cityName','stateName', 'zip','phoneAreaCode','phoneNumber','latitude', 'longitude'])
    dados = dados.select([
    pl.col('establishmentName').alias('ESTABELECIMENTOS'),
    pl.col('address').alias('ENDERECO'),
    pl.col('district').alias('BAIRRO'),
    pl.col('cityName').alias('MUNICIPIO'),
    pl.col('stateName').alias('UF'),
    pl.col('zip').alias('CEP'),
    pl.col('phoneAreaCode').alias('DDD'),
    pl.col('phoneNumber').alias('TELEFONE'),
    pl.col('latitude').alias('LATITUDE'),
    pl.col('longitude').alias('LONGITUDE')
    ])
    return dados
def get_token():
    import requests

    url = "https://api.alelo.com.br/alelo/prd/cardholders/oauth2/token"

    payload='grant_type=client_credentials&client_id=76a775cd-8b2c-4ce8-b7a5-b321c66223f7&client_secret=V2jC1qG2dN2nQ2sK3gE7hR0tI1oO3yT4oF0tA3iI5qK8gD7fX7&scope=acceptance-network'
    headers = {
    'Accept': 'application/json, text/plain, */*, application/json',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
    'Connection': 'keep-alive',
    'Content-Length': '166',
    'Content-Type': 'application/x-www-form-urlencoded',
    'Host': 'api.alelo.com.br',
    'Origin': 'https://redeaceitacao.alelo.com.br',
    'Referer': 'https://redeaceitacao.alelo.com.br/',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-site',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36',
    'sec-ch-ua': '"Google Chrome";v="111", "Not(A:Brand";v="8", "Chromium";v="111"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'Cookie': 'dtCookie=v_4_srv_3_sn_9BC5D4CCED2DEE026B6B076E754DC9C9_perc_100000_ol_0_mul_1_app-3Aea7c4b59f27d43eb_0; a7e2b1664582e63453352cdc14747b5e=149fc99cb62a2dfda246e6f3fc75b1f0; b4a8c8bd365a01af1ce16d38b368df35=320400dd5f58d59b1432b4a3d1135c04'
    }

    response = requests.request("POST", url, headers=headers, data=payload)

    return response.json()
