# libs
import pandas as pd
import warnings
import os
from unidecode import unidecode 
import logging
warnings.filterwarnings('ignore')
pd.option_context(10,5)

# define o caminho do diretório atual
current_dir = os.path.dirname(os.path.abspath(__file__))
file_logs = current_dir.replace(r'sodexo\dados',r'logs\sodexo.log')
# configurando o registro de logs
logging.basicConfig(level=logging.DEBUG, filename=file_logs,encoding='utf-8', format="%(asctime)s - %(levelname)s - %(message)s")

# pega o arquivo gerado
base_sodexo = current_dir + r'\BASE_SODEXO.csv'

# qual cabeçalho nós usamos mesmo?
cabecalho = ['ESTABELECIMENTOS' , 'RAZAO_SOCIAL',  'TELEFONE',
        'EMAIL' , 'SITE'  ,'TIPO_LOGRADOURO',  'LOGRADOURO',
        'NUMERO' , 'COMPLEMENTO' , 'BAIRRO',  'MUNICIPIO',  'UF',
        'CEP' , 'LATITUDE' , 'LONGITUDE' , 'BANDEIRA' , 'LISTA_NEGRA']

# carregada os dados no dataframe pandas aqui, simples né?
dados  = pd.read_csv(base_sodexo, 
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
dados['LISTA_NEGRA'] = dados['LISTA_NEGRA'].astype('boolean')
# coloca tudo em uppercase
dados['ESTABELECIMENTOS'] = dados['ESTABELECIMENTOS'].str.upper()
dados['LOGRADOURO'] = dados['LOGRADOURO'].str.upper()
dados['BAIRRO'] = dados['BAIRRO'].str.upper()
dados['MUNICIPIO'] = dados['MUNICIPIO'].str.upper()



# concatena 
dados['ENDERECO'] = dados['TIPO_LOGRADOURO'].map(str) + ' ' + dados['LOGRADOURO'].map(str) + ', ' + dados['NUMERO'].map(str) + ', ' + dados['COMPLEMENTO'].map(str) + ', ' + dados['BAIRRO'].map(str)  + ', ' + dados['MUNICIPIO'].map(str) + '-' + dados['UF'].map(str)

"""
# tá ai uma coluna inútil, mas com muita utilidade
dados['Cidade_UF'] = dados['MUNICIPIO'].map(str) + ', ' + dados['UF'].map(str)
"""
# Limpando a lista negra
logging.info(f'neste momento estamos limpando os da lista_negra')
lista_negra = dados[dados['LISTA_NEGRA'] == True]
dados.drop(dados[dados['LISTA_NEGRA'] == True ].index, inplace=True)
# conta quando de dados sobrou
logging.info(f'temos: {dados.shape[0]} estabelecimentos fora da lista negra')
logging.info(f'e também temos: {lista_negra.shape[0]} estabelecimentos na lista negra')

# filtrando as colunas que vamos usar depois de toda a brincadeira
dados = dados[['ESTABELECIMENTOS',	'ENDERECO',	'BAIRRO',	'MUNICIPIO',	'UF',	'CEP',	'TELEFONE', 'EMAIL', 'LATITUDE',	'LONGITUDE', 'BANDEIRA', 'SITE']]


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
logging.info(f'ficaram: {dados.shape[0]} dados')
dados.drop(dados[dados['TELEFONE'] == 'Indisponível'].index, inplace=True)
# contando quantos ficaram depois de tirar os nulos
logging.info(f'ficaram: {dados.shape[0]} dados')

# Salva tudo novamente desta vez com um csv e outro excel, a galera gosta de "variedades"
dados.to_csv(base_sodexo,sep=';', index=False, encoding='utf-8')
base_sodexo_xlsx = base_sodexo.replace('.csv', '.xlsx')
dados.to_excel(base_sodexo_xlsx ,sheet_name='BASE SODEXO', index=False)

