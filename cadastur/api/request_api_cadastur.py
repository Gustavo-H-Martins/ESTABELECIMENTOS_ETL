import os
import requests
from dotenv import load_dotenv

# carrega as variáveis de ambientes no script
load_dotenv(".env")

# carrega a variável de ambiente no script
URL_CADASTUR = os.getenv('URL_CADASTUR')

def get_cadastur():
    """
    Filtros:

    - `noPrestador`: Nome do prestador de serviço.
    - `localidade`: Código da localidade.
    - `nuAtividadeTuristica`: Tipo de atividade turística, como "Restaurante, Cafeteria, Bar e Similares".
    - `souPrestador`: Indica se o usuário é um prestador de serviços.
    - `souTurista`: Indica se o usuário é um turista.
    - `localidadesUfs`: Localidades e unidades federativas, como "São Paulo, SP".
    - `localidadeNuUf`: Número da unidade federativa.
    - `flPossuiVeiculo`: Indica se o prestador possui veículo.

    Você pode usar esses campos para filtrar os resultados da sua chamada de API. Por exemplo, se você quiser apenas resultados de prestadores de serviços em São Paulo que possuem veículos, você pode definir os campos `localidadesUfs` e `flPossuiVeiculo` de acordo. 😊
    """

    url = URL_CADASTUR

    payload = {
        "currentPage": 1,
        "pageSize": 10000,
        "sortFields": "noBairro",
        "sortDirections": "ASC",
        "filtros": {
            "noPrestador": "",
            "localidade": 9668,
            "nuAtividadeTuristica": "Restaurante, Cafeteria, Bar e Similares",
            "souPrestador": False,
            "souTurista": True,
            "localidadesUfs": "São Paulo, SP",
            "localidadeNuUf": 25,
            "flPossuiVeiculo": ""
        }
    }

    headers = {
        'accept': 'application/json, text/plain, */*',
        'content-type': 'application/json;charset=UTF-8',
        'origin': 'https://cadastur.turismo.gov.br',
        'referer': 'https://cadastur.turismo.gov.br/hotsite/',
    }

    response = requests.post(url, headers=headers, json=payload)

    data = response.json()["list"]
    base = []
    for d in data:
        base.append({

        })
    return base