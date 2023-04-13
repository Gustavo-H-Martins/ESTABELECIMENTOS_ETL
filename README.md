### OBTENDO CAMINHOS ABSOLUTOS E RELATIVOS EM PYTHON
``` PYTHON
    import os
    # Obter o caminho absoluto do arquivo em execução
    file_path = os.path.abspath(__file__)

    print(file_path)

    # Obter o diretório do arquivo em execução
    dir_path = os.path.dirname(file_path)

    print(dir_path)

    # Obter o diretório do job
    current_job = os.getcwd()

    print(current_job)
```