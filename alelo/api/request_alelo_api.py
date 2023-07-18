import requests
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
    url = f"https://api.alelo.com.br/alelo/prd/acceptance-network/establishments?longitude={longitude}&latitude={latitude}&distance={raio}&pageNumber={page}&pageSize={pageSize}&type=POSITION&product={product}"

    payload={}
    headers = {
        'Origin': 'https://redeaceitacao.alelo.com.br',
        'Referer': 'https://redeaceitacao.alelo.com.br/',
    }

    response = requests.options(url, headers=headers, data=payload)

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
    url = f"https://api.alelo.com.br/alelo/prd/acceptance-network/establishments?longitude={longitude}&latitude={latitude}&distance={raio}&pageNumber={page}&pageSize={pageSize}&type=POSITION&product={product}"

    payload={}
    headers = {
        'Accept': 'application/json, text/plain, */*',
        'Authorization': f'Bearer {token}',
        'Origin': 'https://redeaceitacao.alelo.com.br',
        'Referer': 'https://redeaceitacao.alelo.com.br/',
        'x-ibm-client-id': '76a775cd-8b2c-4ce8-b7a5-b321c66223f7'
    }

    response = requests.get(url, headers=headers, data=payload)

    data =  response.json()['establishments']
    base = []
    for d in data:
        base.append({
            "CNPJ" : d.get('cnpj', None),
            "RAZAO_SOCIAL" : d['razao_establishmentSocialReason'],
            "ESTABELECIMENTOS" : d['establishmentName'], 
            "ENDERECO" : d['address'], 
            "BAIRRO" : d['district'], 
            "CIDADE" : d['cityName'], 
            "UF" : d['stateName'], 
            "CEP" : d['zip'], 
            "TELEFONE" : d['phoneAreaCode'] + " "+ d['phoneNumber'] , 
            "EMAIL": d.get('email', None),
            "LATITUDE" : d['latitude'], 
            "LONGITUDE" : d['longitude'],
            "BANDEIRA": "ALELO"
        })
    return base

def get_token():
    url = "https://api.alelo.com.br/alelo/prd/cardholders/oauth2/token"

    payload='grant_type=client_credentials&client_id=76a775cd-8b2c-4ce8-b7a5-b321c66223f7&client_secret=V2jC1qG2dN2nQ2sK3gE7hR0tI1oO3yT4oF0tA3iI5qK8gD7fX7&scope=acceptance-network'
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
        'Origin': 'https://redeaceitacao.alelo.com.br',
        'Referer': 'https://redeaceitacao.alelo.com.br/',
}

    response = requests.post(url, headers=headers, data=payload)

    return response.json()
