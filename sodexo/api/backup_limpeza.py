import os
import zipfile
from datetime import datetime
def backup_limpeza_simples(pasta:str = os.listdir('.')[0], 
    nome_zipado:str = (f'backup_{str(datetime.now()).replace(":","-")}.zip'),
    extensao:str = '.csv'):
    """
    # Define o diretório de trabalho
    diretorio = pasta
    # Define o nome do arquivo zip a ser criado
    nome_zip = nome_zipado
    # Cria o arquivo zip
    with zipfile.ZipFile(nome_zip, mode='w',compression=zipfile.ZIP_DEFLATED) as zipf:
        # Percorre o diretório em busca de arquivos csv que atendem aos critérios de data de modificação
        for arquivo in os.listdir(diretorio):
            if arquivo.endswith(extensao):
                caminho_arquivo = os.path.join(diretorio, arquivo)
                # Adiciona o arquivo ao arquivo zip
                zipf.write(caminho_arquivo, arquivo)
                # Exclui o arquivo do diretório
                os.remove(caminho_arquivo)
    """
    # Define o diretório de trabalho
    diretorio = pasta

    # Define o nome do arquivo zip a ser criado
    nome_zip = nome_zipado

    # Cria o arquivo zip
    with zipfile.ZipFile(nome_zip, mode='w',compression=zipfile.ZIP_DEFLATED) as zipf:
        # Percorre o diretório em busca de arquivos csv que atendem aos critérios de data de modificação
        for arquivo in os.listdir(diretorio):
            if arquivo.endswith(extensao):
                caminho_arquivo = os.path.join(diretorio, arquivo)
                # Adiciona o arquivo ao arquivo zip
                zipf.write(caminho_arquivo, arquivo)
                # Exclui o arquivo do diretório
                os.remove(caminho_arquivo)