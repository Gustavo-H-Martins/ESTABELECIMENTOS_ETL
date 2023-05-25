# libs
import pandas as pd
import warnings
import os
import sqlite3
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
cabecalho = ['CNPJ', 'RAZAO_SOCIAL', 'ESTABELECIMENTOS',
            'ENDERECO', 'BAIRRO', 'CIDADE',
            'UF', 'CEP', 'TELEFONE',
            'EMAIL', 'LATITUDE', 'LONGITUDE', 'BANDEIRA']

# carregada os dados no dataframe pandas aqui, simples né?

dados  = pd.read_csv(base_sodexo, sep=';',usecols=cabecalho, dtype='string')

# a parte de transform de fato está toda aqui, bem simples:
# com quaanto de dadps começou?
logging.info(f'Tinham: {dados.shape[0]} dados')
# Remove os dados duplicados, estranho que sempre aparecem
dados.drop_duplicates(inplace=True, ignore_index=True)
dados.drop_duplicates(subset=["RAZAO_SOCIAL","ENDERECO", "BAIRRO", "CIDADE", "UF"])
# coloca tudo em uppercase
dados['ESTABELECIMENTOS'] = dados['ESTABELECIMENTOS'].str.upper()
dados['ENDERECO'] = dados['ENDERECO'].str.upper()
dados['BAIRRO'] = dados['BAIRRO'].str.upper()
dados['CIDADE'] = dados['CIDADE'].str.upper()

# filtrando as colunas que vamos usar depois de toda a brincadeira
dados = dados[['RAZAO_SOCIAL', 'ESTABELECIMENTOS', 'ENDERECO', 'BAIRRO', 'CIDADE', 'UF', 'CEP', 'TELEFONE', 'EMAIL', 'LATITUDE','LONGITUDE', 'BANDEIRA']]


# conta quando de dados sobrou
logging.info(f'ficaram: {dados.shape[0]} dados')

# tirando os telefones fakes ou sem valor interessante
telefone = []
for i in dados['TELEFONE']:
    if len(str(i)) < 7:
        telefone.append('Indisponível')
    else:
        telefone.append(str(i))
dados['TELEFONE'] = telefone

# conta quantos de dados tinham antes de tirar os telefones nulos
logging.info(f'ficaram: {dados.shape[0]} dados')
dados.drop(dados[dados['TELEFONE'] == 'Indisponível'].index, inplace=True)
# contando quantos ficaram depois de tirar os nulos
logging.info(f'ficaram: {dados.shape[0]} dados')

# Salva tudo novamente desta vez com um csv e no banco de dados, a galera gosta de "variedades"
dados.to_csv(base_sodexo,sep=';', index=False, encoding='utf-8')
#Criar uma conexão com o banco de dados sqlite
db_file = current_dir.replace('sodexo\dados', r'app\files\database.db')
if os.path.exists(db_file):
    import sys
    sys.path.append(current_dir.replace(r"dados", r"api"))
    from backup_limpeza import backup_sem_limpeza
    from time import localtime, strftime
    nome_backup = db_file.replace(r"database.db", "") + r"ZIP/"
    if not os.path.exists(nome_backup):
        os.makedirs(nome_backup)
    backup_sem_limpeza(pasta=db_file.replace(r"database.db", ""), nome_zipado=nome_backup + f"database_{strftime('%d-%m-%Y %H_%M_%S', localtime())}.zip", extensao='db')
conn = sqlite3.connect(database=db_file)

#Converter o dataframe em uma tabela no banco de dados
"""
O parâmetro if_exists=`append` verifica se a tabela já existe e incrementa os dados
O parâmetro index=False evita que o índice do dataframe seja inserido na tabela
O parâmetro dtype define o tipo de cada coluna na tabela
"""
dados.to_sql('tb_sodexo', conn, 
             if_exists='replace', index=False, 
             dtype={'RAZAO_SOCIAL': 'TEXT', #PRIMARY KEY', 
                    'ESTABELECIMENTOS': 'TEXT', 'ENDERECO': 'TEXT', 
                    'BAIRRO': 'TEXT', 'CIDADE': 'TEXT', 'UF': 'TEXT', 
                    'CEP': 'TEXT', 'TELEFONE': 'TEXT', 
                    'EMAIL': 'TEXT', 'LATITUDE': 'TEXT', 
                    'LONGITUDE': 'TEXT', 'BANDEIRA': 'TEXT'})
# Finaliza a transação
conn.commit()
# Executa o comando VACUUM para compactar o banco de dados
conn.execute('VACUUM')

# Fechar a conexão com o banco de dados
conn.close()
