import os
import logging

current_dir = os.path.dirname(os.path.abspath(__file__))
file_log = current_dir + r"/logs/"
if not os.path.exists(file_log):
    os.makedirs(file_log)
# gerando log
logging.basicConfig(
    level=logging.DEBUG,
    filename=f"{file_log}extract_transform_cnpj.log",
    format="%(asctime)s - %(levelname)s - %(message)s",
)

# Warnings: Possui uma série de funções e comandos para tratamento de mensagens de avisos e alertas do Python
import warnings

warnings.filterwarnings("ignore")


class EXTRATOR_CNPJ:
    def __init__(
        self,
        url: str = "https://dadosabertos.rfb.gov.br/CNPJ/",
        nome_arquivo: str = "",
        extensao: str = "zip",
    ):
        self.url = url
        self.nome_arquivo = nome_arquivo
        self.extensao = extensao
        self.CNPJ_ESQUEMA = {
            "EMPRESAS": {
                "_c0": "CNPJ_BASE",
                "_c1": "RAZAO_SOCIAL",
                "_c2": "CODIGO_NATUREZA_JURIDICA",
                "_c3": "CODIGO_QUALIFICACAO",
                "_c4": "CAPITAL_SOCIAL",
                "_c5": "PORTE_EMPRESA",
                "_c6": "ENTE_FEDERATIVO",
            },
            "ESTABELECIMENTOS": {
                "_c0": "CNPJ_BASE",
                "_c1": "CNPJ_ORDEM",
                "_c2": "CNPJ_DV",
                "_c3": "MATRIZ_FILIAL",
                "_c4": "NOME_FANTASIA",
                "_c5": "SITUACAO_CADASTRAL",
                "_c6": "DATA_SITUACAO_CADASTRAL",
                "_c7": "CODIGO_MOTIVO_SITUACAO_CADASTRAL",
                "_c8": "CIDADE_EXTERIOR",
                "_c9": "CODIGO_PAIS",
                "_c10": "DATA_INICIO_ATIVIDADE",
                "_c11": "CNAE_PRINCIPAL",
                "_c12": "CNAE_SECUNDARIO",
                "_c13": "TIPO_LOGRADOURO",
                "_c14": "LOGRADOURO",
                "_c15": "NUMERO",
                "_c16": "COMPLEMENTO",
                "_c17": "BAIRRO",
                "_c18": "CEP",
                "_c19": "UF",
                "_c20": "CODIGO_MUNICIPIO",
                "_c21": "DDD_CONTATO",
                "_c22": "TELEFONE_CONTATO",
                "_c23": "DDD_COMERCIAL",
                "_c24": "TELEFONE_COMERCIAL",
                "_c25": "DDD_FAX",
                "_c26": "FAX",
                "_c27": "EMAIL",
                "_c28": "SITUACAO_ESPECIAL",
                "_c29": "DATA_SITUACAO_ESPECIAL",
            },
            "SIMPLES": {
                "_c0": "CNPJ_BASE",
                "_c1": "OPCAO_SIMPLES",
                "_c2": "DATA_OPCAO_SIMPLES",
                "_c3": "DATA_EXCLUSAO_SIMPLES",
                "_c4": "OPCAO_MEI",
                "_c5": "DATA_OPCAO_MEI",
                "_c6": "DATA_EXCLUSAO_MEI",
            },
            "SOCIOS": {
                "_c0": "CNPJ_BASE",
                "_c1": "TIPO_SOCIO",
                "_c2": "NOME_SOCIO",
                "_c3": "CPF_CNPJ",
                "_c4": "CODIGO_QUALIFICACAO_SOCIO",
                "_c5": "DATA_ENTRADA_SOCIEDADE",
                "_c6": "PERCENTUAL_CAPITAL_SOCIAL",
                "_c7": "CPF_REPRESENTANTE",
                "_c8": "NOME_REPRESENTANTE",
                "_c9": "CODIGO_QUALIFICACAO_REPRESENTANTE",
                "_c10": "FAIXA_ETARIA",
            },
            "PAISES": {"_c0": "CODIGO_PAIS", "_c1": "NOME_PAIS"},
            "MUNICIPIOS": {"_c0": "CODIGO_MUNICIPIO", "_c1": "NOME_MUNICIPIO"},
            "QUALIFICACOES": {
                "_c0": "CODIGO_QUALIFICACAO",
                "_c1": "DESCRIÇAO_QUALIFICACAO",
            },
            "NATUREZAS": {
                "_c0": "CODIGO_NATUREZA_JURIDICA",
                "_c1": "DESCRICAO_NATUREZA_JURIDICA",
            },
            "MOTIVOS": {
                "_c0": "CODIGO_MOTIVO_SITUACAO_CADASTRAL",
                "_c1": "DESCRICAO_MOTIVO_SITUACAO_CADASTRAL",
            },
            "CNAES": {"_c0": "CODIGO_CNAE", "_c1": "DESCRICAO_CNAE"},
        }

    def request(self):
        import re
        import urllib.request

        abre_url = urllib.request.urlopen(self.url)
        le_url = abre_url.read()
        converte_leitura_url = le_url.decode("utf8")
        abre_url.close()
        urls = []
        urls = [
            f"{self.url}{i.upper()}.{self.extensao}"
            for i in re.findall(
                f'href="(.*).(?:{self.extensao})"', converte_leitura_url
            )
            if i.startswith(self.nome_arquivo)
        ]
        logging.info(f"Retorno da função Request: {urls}")
        return urls

    def download(self, url: str = None, destino: str = None):
        ### Com requests
        """
        import requests
        with requests.get(url, stream=True) as response:
            with open(os.path.join(salvar_onde, arquivo), 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)

        """
        ### Com urllib
        """
        import urllib.request
        arquivo = url.split('/')[-1]
        # faz o download do arquivo e salva em salvar_onde/arquivo
        urllib.request.urlretrieve(url, os.path.join(salvar_onde, arquivo))
        ### Com SmartDL
        """

        ### Com SmartDL
        from pySmartDL import SmartDL

        obj = SmartDL(url, destino, threads=4, progress_bar=False)
        obj.start()
        return destino

    def run(self):
        import re
        import zipfile
        from pyspark.sql import SparkSession
        from pyspark.sql.functions import input_file_name, lit

        urls = self.request()

        # Define ou busca uma sessão do Spark
        spark = (
            SparkSession.builder.master("local[2]")
            .appName("OnlineReader")
            .getOrCreate()
        )
        spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
        for url in urls:
            # numero_arquivo = urls.index(url)
            # Diretório de execução do script
            current_dir = os.path.dirname(os.path.abspath(__file__))

            # Pega o nome do arquivo pela url
            nome_arquivo_download = url.split("/")[-1]

            # Define o caminho absoluto do diretório.
            salvar_onde = f"{current_dir}/RAW/{''.join(filter(str.isalpha,nome_arquivo_download.split('.')[0]))}/"
            logging.info(f"Os arquivos serão salvos no diretório: {salvar_onde}")
            # cria a pasta para armazenar o arquivo, se ela não existir
            if not os.path.exists(salvar_onde):
                os.makedirs(salvar_onde)

            # download do arquivo zip
            path = self.download(url, os.path.join(salvar_onde, nome_arquivo_download))

            # descompactação do arquivo zip
            with zipfile.ZipFile(path, "r") as zip_ref:
                # obtem o nome do primeiro arquivo dentro do zip
                nome_original_arquivo_zip = zip_ref.namelist()[0]

                # define um novo nome para o arquivo
                novo_nome_arquivo = f"CNPJ_{nome_arquivo_download.split('.')[0]}.csv"
                # cria um dicionário com as informações de origem e destino
                arquivos_para_extrair = {nome_original_arquivo_zip: novo_nome_arquivo}

                # realiza a extração do arquivo zip
                zip_ref.extractall(path=f"{salvar_onde}", members=arquivos_para_extrair)

                # renomeia o arquivo extraído com o novo nome
                os.replace(
                    os.path.join(salvar_onde, nome_original_arquivo_zip),
                    os.path.join(salvar_onde, novo_nome_arquivo),
                )
            logging.info(f"Deletando o arquivo baixado {nome_arquivo_download}")
            os.remove(path)
        # leitura do arquivo CSV em um dataframe Spark
        # Obtém a lista de arquivos no diretório
        lista_arquivos_no_diretorio = [
            os.path.join(salvar_onde, nome)
            for nome in os.listdir(salvar_onde)
            if nome.endswith(".csv")
        ]
        # Lê o arquivo CSV em um dataframe Spark e adiciona uma coluna com o nome do arquivo
        if len(lista_arquivos_no_diretorio) == 1:
            dados = (
                spark.read.format("csv")
                .option("header", "false")
                .option("delimiter", ";")
                .option("inferSchema", "true")
                .load(lista_arquivos_no_diretorio[0])
            )
            dados.withColumn("NOME_ARQUIVO", lit(lista_arquivos_no_diretorio[0]))
        # Lê cada arquivo CSV em um dataframe Spark e adiciona uma coluna com o nome do arquivo
        else:
            dados = None
            for arquivo_no_diretorio in lista_arquivos_no_diretorio:
                if dados is None:
                    dados = (
                        spark.read.format("csv")
                        .option("header", "false")
                        .option("delimiter", ";")
                        .option("inferSchema", "true")
                        .load(arquivo_no_diretorio)
                    )
                    dados = dados.withColumn(
                        "NOME_ARQUIVO", lit(arquivo_no_diretorio.split("\\")[-1])
                    )
                else:
                    # print(arquivo_no_diretorio)
                    dados_incrementados = (
                        spark.read.format("csv")
                        .option("header", "false")
                        .option("delimiter", ";")
                        .option("inferSchema", "true")
                        .load(arquivo_no_diretorio)
                    )
                    dados_incrementados = dados_incrementados.withColumn(
                        "NOME_ARQUIVO", lit(arquivo_no_diretorio.split("\\")[-1])
                    )
                    dados = dados.union(dados_incrementados)
        # USA O MÉTODO WITHCOLUMNRENAMED() PARA RENOMEAR AS COLUNAS
        for NOME_ANTIGO, NOVO_NOME in self.CNPJ_ESQUEMA[
            "".join(filter(str.isalpha, nome_arquivo_download.split(".")[0]))
        ].items():
            dados = dados.withColumnRenamed(NOME_ANTIGO, NOVO_NOME)
        logging.info(f"Estrutura do DataFrame: {dados.show(1, vertical=True)}")
        return dados
