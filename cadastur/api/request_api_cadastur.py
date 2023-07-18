import os
import requests
from datetime import datetime
from dotenv import load_dotenv

# carrega as variáveis de ambientes no script
load_dotenv(".env")

# carrega a variável de ambiente no script
URL_CADASTUR = os.getenv("URL_CADASTUR")
ORIGIN_CADASTUR = os.getenv("ORIGIN_CADASTUR")

def formata_cnpj(cnpj:str=None):
    """Retorna o CNPJ Formatado no formato Brasileiro"""
    cnpj = str(cnpj)
    if len(cnpj) == 14:
        return f"{cnpj[:2]}.{cnpj[2:5]}.{cnpj[5:8]}/{cnpj[8:12]}-{cnpj[12:]}"
    else:
        return cnpj

def formata_data(int_data:int):
    """Retorna a data no formato Brasileiro"""
    if int_data:
        parse_data = datetime.fromtimestamp(int_data / 1e3)
        format_data = parse_data.strftime("%d-%m-%Y")
    else:
        format_data = int_data
    return format_data

def formata_telefone(telefone):
    """Retorna o telefone Formatado no formato Brasileiro"""
    telefone = str(telefone)
    if len(telefone) == 10:
        return f"({telefone[:2]}) {telefone[2:6]}-{telefone[6:]}"
    elif len(telefone) == 11:
        return f"({telefone[:2]}) {telefone[2:7]}-{telefone[7:]}"
    else:
        return telefone

def formata_url_prestador(id):
    """Formata a url para api no cadastur"""
    id = str(id)
    if id:
        url = f"https://cadastur.turismo.gov.br/cadastur-backend/rest/portal/prestador/{id}/tipoPrestador/PJ"
    else:
        url = id
    return url

def get_cadastur(page:int=1, page_size:int=30000, campo_ordenacao:str="noBairro", 
                 orderby:str="ASC", prestador:str="", cod_local:int=None, 
                 atividade_turistica:str="Restaurante, Cafeteria, Bar e Similares",
                 sou_prestador:bool=False, sou_turista:bool=True, cidade_uf:str="Lagoa Santa, MG",
                 id_uf:int=None, possui_veiculo:str=""):
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
    # URL no .env
    url = URL_CADASTUR

    # Parâmetros e filtros
    params = {
        "currentPage": page,
        "pageSize": page_size,
        "sortFields": campo_ordenacao,
        "sortDirections": orderby,
        "filtros": {
            "noPrestador": prestador,
            "localidade": cod_local,
            "nuAtividadeTuristica": atividade_turistica,
            "souPrestador": sou_prestador,
            "souTurista": sou_turista,
            "localidadesUfs": cidade_uf,
            "localidadeNuUf": id_uf,
            "flPossuiVeiculo": possui_veiculo
        }
    }

    # Cabeçalho da requisição
    headers = {
        "accept": "application/json, text/plain, */*",
        "content-type": "application/json;charset=UTF-8",
        "origin": ORIGIN_CADASTUR,
        "referer": f"{ORIGIN_CADASTUR}/",
    }

    response = requests.post(url, headers=headers, json=params)

    # Formata o json de retorno
    data = response.json()["list"]
    base = []
    for d in data:
        base.append({
            "CNPJ": str(d.get("numeroCadastro", "")),
            "CNPJ_FORMATADO": formata_cnpj(d.get("numeroCadastro", "")),
            "NOME_FANTASIA" : str(d.get("nomePrestador", "")),
            "RAZAO_SOCIAL" : str(d.get("registroRf", "")),
            "INICIO_VIGENCIA": formata_data(d.get("dtInicioVigencia", 0)),
            "FIM_VIGENCIA": formata_data(d.get("dtFimVigencia", 0)),
            "SITE": d.get("noWebSite", ""),
            "TELEFONE": formata_telefone(d.get("nuTelefone", "")),
            "CEP": d.get("nuCep", ""),
            "ENDERECO": str(d.get("noLogradouro", "")).capitalize() + ", " + str(d.get("complemento","")).capitalize(),
            "BAIRRO": d.get("noBairro",""),
            "CIDADE" : str(d.get("municipio", "")).capitalize(),
            "UF": d.get("sguf", ""),
            "ATIVIDADE": str(d.get("atividade","")).capitalize(),
            "COD_SITUACAO_CADASTRAL": d.get("nuSituacaoCadastral", ""),
            "SITUACAO_CADASTRAL": d.get("situacao",""),
            "ID_PRESTADOR": d.get("id",""),
            "URL_DETALHES_PRESTADOR" : formata_url_prestador(d.get("id",""))
        })
    return base