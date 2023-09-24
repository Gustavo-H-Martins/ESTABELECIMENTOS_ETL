import os
import logging
import pandas as pd
import numpy as np
import pyspark
from pyspark.sql.functions import concat_ws, lpad, coalesce, when, lit, col, date_format, split, explode
from functools import reduce
from pyspark.sql import DataFrame
from typing import List
from time import localtime, strftime
import zipfile
from pyspark.sql import SparkSession

# Define ou busca uma sessão do Spark
spark = (
    SparkSession.builder.master("local[2]")
    .appName("OnlineReader")
    .config("spark.driver.memory", "6g")
    .config("spark.driver.maxResultSize", "6g")
    .config("spark.sql.debug.maxToStringFields", "50")  # Aumenta o número de campos exibidos
    #.config("spark.executor.extraJavaOptions", "-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps")
    #.config("spark.driver.extraJavaOptions", "-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps")
    .getOrCreate()
)

current_dir = os.path.dirname(os.path.abspath(__file__))
dir_log = current_dir + r"\logs"
file_log = current_dir + r"\logs\extract_transform_cnpj.log"
dir_dados = current_dir + r"\dados"

if not os.path.exists(dir_log):
    os.makedirs(dir_log)
# gerando log
logging.basicConfig(
    level=logging.DEBUG,
    filename=file_log,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

# Warnings: Possui uma série de funções e comandos para tratamento de mensagens de avisos e alertas do Python
import warnings

warnings.filterwarnings("ignore")

def filtra_cnpj_cnae_principal():
    dados = spark.sql(
        """SELECT
            regexp_replace(CONCAT(LPAD(ESTABELECIMENTOS.CNPJ_BASE, 8, '0'), LPAD(ESTABELECIMENTOS.CNPJ_ORDEM, 4, '0'),LPAD(ESTABELECIMENTOS.CNPJ_ORDEM, 2, '0')) ,'(\\d{2})(\\d{3})(\\d{3})(\\d{4})(\\d{2})','$1.$2.$3/$4-$5') AS CNPJ,
            EMPRESAS.RAZAO_SOCIAL,
            ESTABELECIMENTOS.NOME_FANTASIA,
            ESTABELECIMENTOS.MATRIZ_FILIAL,
            ESTABELECIMENTOS.SITUACAO_CADASTRAL,
                        date(
                substr(
                ESTABELECIMENTOS.DATA_SITUACAO_CADASTRAL, 1, 4
                ) || '-' || 
                substr(
                ESTABELECIMENTOS.DATA_SITUACAO_CADASTRAL, 5, 2
                ) || '-' || 
                substr(
                ESTABELECIMENTOS.DATA_SITUACAO_CADASTRAL, 7, 2
                )
            ) AS DATA_SITUACAO_CADASTRAL,
            date(
                substr(
                    ESTABELECIMENTOS.DATA_INICIO_ATIVIDADE, 1, 4
                ) || '-' || 
                substr(
                    ESTABELECIMENTOS.DATA_INICIO_ATIVIDADE, 5, 2
                ) || '-' || 
                substr(
                    ESTABELECIMENTOS.DATA_INICIO_ATIVIDADE, 7, 2
                )
            ) AS DATA_INICIO_ATIVIDADE,
            ESTABELECIMENTOS.CNAE_PRINCIPAL,
            CNAES.CNAE_DESCRICAO,
            ESTABELECIMENTOS.CNAE_SECUNDARIO,
            CONCAT(
                COALESCE(ESTABELECIMENTOS.TIPO_LOGRADOURO, ''),
                ' ',
                COALESCE(ESTABELECIMENTOS.LOGRADOURO, ''),
                ' ',
                COALESCE(ESTABELECIMENTOS.NUMERO, ''),
                ' ',
                COALESCE(ESTABELECIMENTOS.COMPLEMENTO, '')
            ) AS ENDERECO,
            ESTABELECIMENTOS.BAIRRO,
            MUNICIPIOS.NOME_MUNICIPIO AS CIDADE,
            ESTABELECIMENTOS.UF,
            ESTABELECIMENTOS.CEP,
            CONCAT(
                COALESCE(ESTABELECIMENTOS.DDD_CONTATO, ''),
                ' ',
                COALESCE(ESTABELECIMENTOS.TELEFONE_CONTATO, '')
            ) AS TELEFONE,
            ESTABELECIMENTOS.EMAIL
        FROM ESTABELECIMENTOS
        LEFT JOIN EMPRESAS ON  EMPRESAS.CNPJ_BASE = ESTABELECIMENTOS.CNPJ_BASE
        LEFT JOIN MUNICIPIOS ON ESTABELECIMENTOS.CODIGO_MUNICIPIO = MUNICIPIOS.CODIGO_MUNICIPIO
        LEFT JOIN CNAES ON ESTABELECIMENTOS.CNAE_PRINCIPAL = CNAES.CODIGO_CNAE
        WHERE ESTABELECIMENTOS.CNAE_PRINCIPAL IN ('4721102', '5611201', '5611203', '5611204', '5611205',  '5612100', '5620104') AND ESTABELECIMENTOS.SITUACAO_CADASTRAL IN (2, 3, 4)
        ORDER BY ESTABELECIMENTOS.CNPJ_BASE
    """)
    return dados

def filtra_cnpj_cnae_segundario():
    dados = spark.sql(
        """SELECT
          regexp_replace(CONCAT(LPAD(ESTABELECIMENTOS.CNPJ_BASE, 8, '0'), LPAD(ESTABELECIMENTOS.CNPJ_ORDEM, 4, '0'),LPAD(ESTABELECIMENTOS.CNPJ_ORDEM, 2, '0')) ,'(\\d{2})(\\d{3})(\\d{3})(\\d{4})(\\d{2})','$1.$2.$3/$4-$5') AS CNPJ,
          EMPRESAS.RAZAO_SOCIAL,
          ESTABELECIMENTOS.NOME_FANTASIA,
          ESTABELECIMENTOS.MATRIZ_FILIAL,
          ESTABELECIMENTOS.SITUACAO_CADASTRAL,
          date(
            substr(
              ESTABELECIMENTOS.DATA_SITUACAO_CADASTRAL, 1, 4
            ) || '-' || 
            substr(
              ESTABELECIMENTOS.DATA_SITUACAO_CADASTRAL, 5, 2
            ) || '-' || 
            substr(
              ESTABELECIMENTOS.DATA_SITUACAO_CADASTRAL, 7, 2
            )
          ) AS DATA_SITUACAO_CADASTRAL,
          date(
            substr(
              ESTABELECIMENTOS.DATA_INICIO_ATIVIDADE, 1, 4
              ) || '-' || 
            substr(
              ESTABELECIMENTOS.DATA_INICIO_ATIVIDADE, 5, 2
              ) || '-' || 
            substr(
              ESTABELECIMENTOS.DATA_INICIO_ATIVIDADE, 7, 2
              )
          ) AS DATA_INICIO_ATIVIDADE,
          ESTABELECIMENTOS.CNAE_PRINCIPAL,
          CNAES.CNAE_DESCRICAO,
          ESTABELECIMENTOS.CNAE_SECUNDARIO,
          CONCAT(
            COALESCE(ESTABELECIMENTOS.TIPO_LOGRADOURO, ''),
            ' ',
            COALESCE(ESTABELECIMENTOS.LOGRADOURO, ''),
            ' ',
            COALESCE(ESTABELECIMENTOS.NUMERO, ''),
            ' ',
            COALESCE(ESTABELECIMENTOS.COMPLEMENTO, '')
          ) AS ENDERECO,
          ESTABELECIMENTOS.BAIRRO,
          MUNICIPIOS.NOME_MUNICIPIO AS CIDADE,
          ESTABELECIMENTOS.UF,
          ESTABELECIMENTOS.CEP,
          CONCAT(
            COALESCE(ESTABELECIMENTOS.DDD_CONTATO, ''),
            ' ',
            COALESCE(ESTABELECIMENTOS.TELEFONE_CONTATO, '')
          ) AS TELEFONE,
          ESTABELECIMENTOS.EMAIL
        FROM ESTABELECIMENTOS
        LEFT JOIN EMPRESAS ON  EMPRESAS.CNPJ_BASE = ESTABELECIMENTOS.CNPJ_BASE
        LEFT JOIN MUNICIPIOS ON ESTABELECIMENTOS.CODIGO_MUNICIPIO = MUNICIPIOS.CODIGO_MUNICIPIO
        LEFT JOIN CNAES ON ESTABELECIMENTOS.CNAE_PRINCIPAL = CNAES.CODIGO_CNAE
        WHERE ESTABELECIMENTOS.CNAE_SECUNDARIO_VALUE IN ('4721102', '5611201', '5611203', '5611204', '5611205',  '5612100', '5620104')  AND ESTABELECIMENTOS.SITUACAO_CADASTRAL IN (2, 3, 4)
        ORDER BY ESTABELECIMENTOS.CNPJ_BASE
    """)
    return dados

# Função de unir vários dataframes
def unir_dataframes(lista_df: List) -> DataFrame:
    # Cria uma união por nome das colunas
    unir_dataframes_por_colunas_diferentes = lambda dfa, dfb: dfa.unionByName(dfb, allowMissingColumns=True)
    
    # use reduce to combine all the dataframes
    dataframe_final = reduce(unir_dataframes_por_colunas_diferentes, lista_df)
    
    return dataframe_final

class EXTRATOR_CNPJ:
    f"""
        argumentos:
            baixar_e_extrair (bool): define o que a quais ações vamos realizar, em relação a extrair e ler os dados, ou somente ler os dados já existentes,
            se os dados não existirem e colocar para ler ele vai baixar tudo antes de ler :
            {os.linesep}[True] para extrair e ler 
            {os.linesep}[False] para somente ler.
            url (str, opcional): "https://dadosabertos.rfb.gov.br/CNPJ/".
            nome_arquivo (str, opcional): informe o nome do arquivo na página da RFB que você quer buscar, padrão vazio.
            extensao (str, opcional): informe a extensão do arquivo que quer buscar, tem um pdf na fonte, o padrão é "zip".
        """
    def __init__(
        self,
        baixar_e_extrair:bool = True,
        url: str = "https://dadosabertos.rfb.gov.br/CNPJ/",
        nome_arquivo: str = None,
        extensao: str = "zip",
    ):
        
        self.baixar_e_extrair = baixar_e_extrair
        self.url = url
        self.nome_arquivo = nome_arquivo.capitalize()
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
            "CNAES": {"_c0": "CODIGO_CNAE", "_c1": "CNAE_DESCRICAO"},
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
            with open(destino, 'wb') as f:
                for chunk in response.iter_content(chunk_size=65576):
                    f.write(chunk)
        """
        
        """
        ### Com urllib

        import urllib.request
        arquivo = url.split('/')[-1]
        # faz o download do arquivo e salva em salvar_onde/arquivo
        urllib.request.urlretrieve(url, destino)
        ### Com SmartDL
        
        """

        
        ### Com SmartDL
        from pySmartDL import SmartDL

        obj = SmartDL(url, destino, threads=6, progress_bar=False)
        obj.start()
        
        return destino

    def run(self):
        if self.baixar_e_extrair != False:
            # ativa o método request
            urls = self.request()
            for url in urls:
                # numero_arquivo = urls.index(url)

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
        else:
            if self.nome_arquivo is None:
                self.nome_arquivo = str(input(f"""Se você não quer baixar os arquivos, preciso que me informe o nome para os arquivos que vou ler né amigão!
                {os.linesep} [SIMPLES]
                {os.linesep} [ESTABELECIMENTOS]
                {os.linesep} [SOCIOS]
                {os.linesep} [CNAES]
                {os.linesep} [MUNICIPIOS]
                {os.linesep} [EMPRESAS]
                {os.linesep} [MOTIVOS]
                {os.linesep} [NATUREZAS]
                {os.linesep} [PAISES]
                {os.linesep} [QUALIFICACOES]
                {os.linesep} informe o nome com todas as letras.""")).upper()
                # Define o caminho absoluto do diretório.
            salvar_onde = f"{current_dir}/RAW/{self.nome_arquivo}/"
        
        # leitura do arquivo CSV em um dataframe Spark
        # Obtém a lista de arquivos no diretório
        lista_arquivos_no_diretorio = [
            os.path.join(salvar_onde, nome)
            for nome in os.listdir(salvar_onde)
            if nome.endswith(".csv")
        ]
        # Lê o arquivo CSV em um dataframe Spark e adiciona uma coluna com o nome do arquivo
        if len(lista_arquivos_no_diretorio) == 1:
            arquivo_no_diretorio = lista_arquivos_no_diretorio[0]
            dados = (
                spark.read.format("csv")
                .option("header", "false")
                .option("delimiter", ";")
                .option("inferSchema", "true")
                .load(arquivo_no_diretorio)
            )
            nome_arquivo = arquivo_no_diretorio.split("\\")[-1]
            dados.withColumn("NOME_ARQUIVO", lit(nome_arquivo))

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
                    nome_arquivo = arquivo_no_diretorio.split("\\")[-1]
                    dados = dados.withColumn(
                        "NOME_ARQUIVO", lit(nome_arquivo)
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
                    nome_arquivo = arquivo_no_diretorio.split("\\")[-1]
                    dados_incrementados = dados_incrementados.withColumn(
                        "NOME_ARQUIVO", lit(nome_arquivo)
                    )
                    dados = dados.union(dados_incrementados)
        # USA O MÉTODO WITHCOLUMNRENAMED() PARA RENOMEAR AS COLUNAS
        V_ESQUEMA = arquivo_no_diretorio.replace(f"{salvar_onde}CNPJ_", "")
        # MODIFICA O ESQUEMA
        for NOME_ANTIGO, NOVO_NOME in self.CNPJ_ESQUEMA[
            "".join(filter(str.isalpha, V_ESQUEMA.split(".")[0]))
        ].items():
            dados = dados.withColumnRenamed(NOME_ANTIGO, NOVO_NOME)
        #logging.info(f"TOTAL DADOS RETORNADOS: {dados.count()}")
        return dados

if __name__ == '__main__':
    import time

    # Marca o tempo inicial
    start_time = time.time()
    print(f'Iniciado processo : {strftime("%d/%m/%Y %H:%M:%S", localtime())}')

    # Se passar baixar_e_extrair como false, precisa do nome do arquivo.
    print(f'Começando a buscar os dados: {strftime("%d/%m/%Y %H:%M:%S", localtime())}')
    ESTABELECIMENTOS = EXTRATOR_CNPJ(baixar_e_extrair=True, nome_arquivo="Estabelecimentos").run()

    print(f'Termino da coleta dos ESTABELECIMENTOS: {strftime("%d/%m/%Y %H:%M:%S", localtime())}')
    EMPRESAS = EXTRATOR_CNPJ(baixar_e_extrair=True, nome_arquivo="Empresas").run()

    print(f'Termino da coleta dos EMPRESAS: {strftime("%d/%m/%Y %H:%M:%S", localtime())}')
    MUNICIPIOS = EXTRATOR_CNPJ(baixar_e_extrair=True, nome_arquivo="Municipios").run()

    print(f'Final da coleta dos MUNICIPIOS: {strftime("%d/%m/%Y %H:%M:%S", localtime())}')
    CNAES = EXTRATOR_CNPJ(baixar_e_extrair=True, nome_arquivo="Cnaes").run()

    print(f'Final da coleta dos dados: {strftime("%d/%m/%Y %H:%M:%S", localtime())}')

    # Cria uma nova coluna com os valores da coluna CNAE_SECUNDARIO divididos em uma lista
    ESTABELECIMENTOS = ESTABELECIMENTOS.withColumn("CNAE_SECUNDARIO_LIST", split(ESTABELECIMENTOS["CNAE_SECUNDARIO"], ","))
    # Explode a coluna CNAE_SECUNDARIO_LIST para criar uma linha para cada valor
    ESTABELECIMENTOS = ESTABELECIMENTOS.select("*", explode("CNAE_SECUNDARIO_LIST").alias("CNAE_SECUNDARIO_VALUE"))

    # Cria uma view com o mesmo nome do DataFrame
    ESTABELECIMENTOS.createOrReplaceTempView("ESTABELECIMENTOS")
    EMPRESAS.createOrReplaceTempView("EMPRESAS")
    MUNICIPIOS.createOrReplaceTempView("MUNICIPIOS")
    CNAES.createOrReplaceTempView("CNAES")

    CNAES_FILTROS = {
        5620104:'Fornecimento de alimentos preparados preponderantemente para consumo domiciliar',
        5611201:'Restaurantes e similares',
        5611203:'Lanchonetes casas de chá de sucos e similares',
        5611204:'Bares e outros estabelecimentos especializados em servir bebidas sem entretenimento',
        5611205:'Bares e outros estabelecimentos especializados em servir bebidas com entretenimento',
        4721102:'Padaria e confeitaria com predominância de revenda'
        }
    
    # Importa a função de backup
    from backup_limpeza import backup_limpeza_simples, limpeza_sem_backup
    
    # Caminho para a subpasta "csv"
    subpasta_csv = os.path.join(dir_dados, "csv")  
    
    # Caminho para a subpasta "parquet"
    subpasta_parquet = os.path.join(dir_dados, "parquet")

    # Definindo um nome para o conjunto de dados
    nome_particao = f'BASE_RFB_{strftime("%d-%m-%Y_%H-%M-%S", localtime())}.csv'

    # Mapeia o caminho para salvar o arquivo, verifica se já existe dados lá e faz o backup
    arquivo_csv = os.path.join(subpasta_csv, nome_particao)
    arquivo_parquet = os.path.join(subpasta_parquet, nome_particao)

    if os.path.exists(subpasta_csv):
        dir_backup = os.path.join(dir_dados, "backup")
        if not os.path.exists(dir_backup):
            os.makedirs(dir_backup)

        # Remove os backups existêntes
        limpeza_sem_backup(dir_backup, ".zip")

        # Faz o backup dos dados atuais
        backup_limpeza_simples(pasta=subpasta_csv, nome_zipado=os.path.join(dir_backup, nome_particao.replace(".csv",".zip")))
        

    # Chama a 1ª função de filtrar por cnae
    dados_primario = filtra_cnpj_cnae_principal()

    # Chama a 2ª função de filtrar por cnae
    # dados_secundario = filtra_cnpj_cnae_segundario()
    
    # Habilitar as transferências de dados baseadas em Arro
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    # Cria um dataframe pandas
    # dados_pandas = dados_primario.toPandas()
    
    # Cria uma lista vazia para armazenar os chunks do dataframe pandas
    colunas_spark = dados_primario.columns
    # Obtem o número total de rows do dataframe spark
    total_rows = dados_primario.collect()

    dados_pandas = pd.DataFrame(data=total_rows, columns=colunas_spark)   
    # Salva o dataframe inicial
    dados_pandas.to_csv(arquivo_csv, sep=';', mode='a', index=False, encoding='utf-8')

    # Concatena os chunks em um único dataframe pandas
    #dados_pandas = pd.concat(chunks)

    # Encerra a SparkSession
    spark.stop()

    # Unifica os dois dataframes resultantes das funções anteriores
    # dados = unir_dataframes([dados_primario_limitados, dados_secundario_limitados])

    # Dropando valores duplicados em CNPJ, RAZAO_SOCIAL e NOME_FANTASIA
    # dados = dados.dropDuplicates(subset=["CNPJ", "RAZAO_SOCIAL","NOME_FANTASIA"])

    # Importa função de atualizar database
    from dados.utils.transform_rfb import atualiza_database

    # Atualiza o banco de dados
    atualiza_database()

    # Marca o tempo final
    end_time = time.time()

    # Calcula o tempo total de execução
    total_time = end_time - start_time
    # Printa as informações finais do processo
    print(f'Finalizado processo : {strftime("%d/%m/%Y %H:%M:%S", localtime())}')
    print(f"Tempo total de execução: {total_time:.2f} segundos")