# libs
import pandas as pd
import warnings
import os
from unidecode import unidecode 
import logging
warnings.filterwarnings('ignore')
pd.option_context(10,5)

current_dir = os.getcwd()
file_logs = current_dir.replace(r'alelo\dados',r'logs\alelo.log')
# configurando o registro de logs
logging.basicConfig(level=logging.DEBUG, filename=file_logs,encoding='utf-8', format="%(asctime)s - %(levelname)s - %(message)s")


# pega o arquivo gerado
base_alelo = r'./BASE_ALELO.csv'

# qual cabeçalho nós usamos mesmo??
cabecalho = ['ESTABELECIMENTOS',
 'ENDERECO',
 'BAIRRO',
 'MUNICIPIO',
 'UF',
 'CEP',
 'DDD',
 'TELEFONE',
 'LATITUDE',
 'LONGITUDE']

# carregada os dados no dataframe pandas aqui, simples né?
dados  = pd.read_csv(base_alelo, 
                        index_col=False, 
                        encoding='utf-8',
                        on_bad_lines='warn',
                        sep=';',
                        names=cabecalho)

# a parte de transform de fato está toda aqui, bem simples:
# com quaanto de dadps começou?
logging.info(f'Tinham: {dados.shape[0]} dados')

# Remove os dados duplicados, estranho que sempre aparecem
dados.drop_duplicates(inplace=True, ignore_index=True)

# coloca tudo em uppercase
dados['ENDERECO'] = dados['ENDERECO'].str.upper()
dados['MUNICIPIO'] = dados['MUNICIPIO'].str.upper()
dados['BAIRRO'] = dados['BAIRRO'].str.upper()

# criando a coluna "BANDEIRA", é bom pra evitar problema né fiotin?
dados['BANDEIRA'] = 'ALELO'
dados['EMAIL'] = ''

# concatena o ddd + telefon
dados['TELEFONE'] = dados['DDD'].map(str).replace('.0','')+ ' ' + dados['TELEFONE'].map(str).replace('.0', '')
"""
# tá ai uma coluna inútil, mas com muita utilidade
dados['Cidade_UF'] = dados['MUNICIPIO'].map(str) + ', ' + dados['UF'].map(str)
"""
# filtrando as colunas que vamos usar depois de toda a brincadeira
dados = dados[['ESTABELECIMENTOS',	'ENDERECO',	'BAIRRO',	'MUNICIPIO',	'UF',	'CEP',	'TELEFONE', 'EMAIL', 'LATITUDE',	'LONGITUDE', 'BANDEIRA']]

# conta quando de dados sobrou
logging.info(f'ficaram: {dados.shape[0]} dados')

# adicionando a coluna padrão ibge, é muito útil para colocar em mapas e essas coisas legais de geoprocessamento
CIDADE_PADRAO_IBGE=[]
for municipio in dados['MUNICIPIO']:
    CIDADE_PADRAO_IBGE.append(unidecode(str(municipio)))
dados['CIDADE_PADRAO_IBGE'] = CIDADE_PADRAO_IBGE

# tirando os telefones fakes ou sem valor interessante
telefone = []
for i in dados['TELEFONE']:
    if len(str(i)) < 9:
        telefone.append('Indisponível')
    else:
        telefone.append(str(i))
dados['TELEFONE'] = telefone

# conta quantos de dados tinham antes de tirar os telefones nulos
logging.info(f'tinham: {dados.shape[0]} dados antes de comparar telefones nulos')
dados.drop(dados[dados['TELEFONE'] == 'Indisponível'].index, inplace=True)
# contando quantos ficaram depois de tirar os nulos
logging.info(f'ficaram: {dados.shape[0]} dados após a operação de dropagem')

# Salva tudo novamente desta vez com um csv e outro excel, a galera gosta de "variedades"
dados.to_csv(base_alelo,sep=';', index=False, encoding='utf-8')
dados.to_excel('./BASE_ALELO.xlsx',sheet_name='BASE ALELO', index=False)