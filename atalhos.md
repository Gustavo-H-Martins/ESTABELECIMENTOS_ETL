# Atalhos para este projeto em específico
## Acessar servidor `amazonaws`
* Já estando tudo configurado claro
```powershell
    # ssh -i "chave.pem" usuário@caminho-do-servidor-web
    ssh -i "apiLeads.pem" ubuntu@ec2-52-67-164-51.sa-east-1.compute.amazonaws.com
```

## Enviar arquivo para servidor `amazonaws`
```powershell
    # scp -i "chave.pem" "caminho-do-arquivo-local" usuário@caminho-do-servidor-web:caminho-da-pasta-que-receberá-o-arquivo-no-servidor
    scp -i "apiLeads.pem" "C:\Users\ABRASEL NACIONAL\Documents\GitHub\ESTABELECIMENTOS_ETL\app\files\database.db" ubuntu@ec2-52-67-164-51.sa-east-1.compute.amazonaws.com:/home/ubuntu/app/files
```

## Buscar arquivo no servidor `amazonaws` para o diretório local
```powershell
    # scp -i "chave.pem" usuário@caminho-do-servidor-web:caminho-do-arquivo-remoto "caminho-da-pasta-que-receberá-o-arquivo-no-ambiente-local"
    scp -i "apiLeads.pem" ubuntu@ec2-52-67-164-51.sa-east-1.compute.amazonaws.com:/home/ubuntu/app/files/database.db "C:\Users\ABRASEL NACIONAL\Documents\GitHub\ESTABELECIMENTOS_ETL\app\files"
```

## Atalhos `pm2` no servidor 
```bash
    # Lista aplicações
    pm2 -l

    # Estarta aplicações
    pm2 start -n "nome-aplicação" --watch

    # Restarta aplicações
    pm2 restart api_leads --update-env --watch

    # Lista logs da aplicação
    pm2 log
```

## Atalhos gerar `requirements.txt` `python`
```bash
    # Primeiro precisa instalar o pipreqs 
    python.exe -m pip install --upgrade pipreqs
    pipreqs --force --no-follow-links --debug --mode gt ./
```

## Salva Histórico do CMD em um arquivo `histórico.txt`
```powershell
    # O comando `Get-PSReadlineOption` é usado para obter as opções do módulo `PSReadLine`. A propriedade `HistorySavePath` retorna o caminho do arquivo de histórico do `PSReadLine`. O comando `Get-Content` é usado para ler o conteúdo desse arquivo de histórico
    Get-Content (Get-PSReadlineOption).HistorySavePath > histórico.txt
```

## Histórico recente no CMD 
```bash
    # O comando `doskey` é usado para exibir e editar comandos do prompt de comando. A opção `/history` é usada para exibir o histórico de comandos.
    doskey /history > history.txt
```

## Commandos Docker
```bash
    # O comando `docker build` é usado para construir uma imagem a partir de um `Dockerfile`. A opção `-t` é usada para especificar o nome e a tag da imagem.O . (ponto) no final do comando especifica o contexto de construção, que neste caso é o diretório atual.
    sudo docker build -t cadastur-image .

    # O comando `docker run` é usado para criar e executar um contêiner. A opção `-d` é usada para executar o contêiner em segundo plano (modo desanexado). As opções `-i` e `-t` são usadas para manter o STDIN aberto e alocar um pseudo-TTY, respectivamente pode ser usado `-dit`. Isso permite que você interaja com o contêiner usando o terminal. A opção `--name` é usada para especificar o nome do contêiner, que neste caso é `cadastur-container.` O último argumento, `cadastur-image`, especifica o nome da imagem a partir da qual o contêiner será criado.
    sudo docker run -d -i -t --name cadastur-container cadastur-image

    # O comando `docker exec` é usado para executar um comando em um contêiner em execução. As opções `-i` e `-t` são usadas para manter o STDIN aberto e alocar um pseudo-TTY, respectivamente. Isso permite que você interaja com o contêiner usando o terminal. O argumento `sh` especifica o comando a ser executado, que neste caso é um shell interativo.
    sudo docker exec -it cadastur-container sh

    # O comando `docker logs` é usado para recuperar os logs de um contêiner. A opção `-f` é usada para seguir a saída do log em tempo real. Isso significa que o comando continuará exibindo novas entradas de log à medida que elas forem geradas pelo contêiner. Você pode interromper o comando pressionando `Ctrl + C`.
    sudo docker logs -f cadastur-container

    # A opção `-v` é usada para montar um volume no contêiner. Neste caso, o comando monta o diretório atual `($(pwd))` no contêiner no caminho `/usr/src/cadastur`. O último argumento, `cadastur-image`, especifica o nome da imagem a partir da qual o contêiner será criado. 
    sudo docker run -dit --name cadastur-container  -v "$(pwd)":/usr/src/cadastur cadastur-image

    # O comando `docker compose -f docker-compose.yml down` é usado para parar e remover contêineres, redes, volumes e imagens criadas pelo comando `docker compose up` usando um arquivo Compose específico chamado `docker-compose.yml`. A opção `-f` é usada para especificar o caminho do arquivo Compose. Por padrão, apenas os contêineres para serviços definidos no arquivo Compose e as redes definidas na seção de redes do arquivo Compose são removidos. As redes e volumes definidos como externos nunca são removidos. Os volumes anônimos não são removidos por padrão, mas como eles não têm um nome estável, eles não serão montados automaticamente por um subsequente up. Para dados que precisam persistir entre atualizações, use caminhos explícitos como montagens de ligação ou volumes nomeados.
    sudo docker compose -f docker-compose.yml down

    # O comando `docker volume inspect` é usado para inspecionar um volume. O argumento `cadastur-volume` especifica o nome do volume a ser inspecionado. O comando retorna informações sobre o volume em formato JSON, incluindo o nome, o driver, a montagem e outras opções de configuração.
    sudo docker volume inspect cadastur-volume

    # Remove todas as imagens pendentes do docker
    sudo docker rmi $(docker images -f "dangling=true" -q)

    # Lista todos os contêineres.
    sudo docker ps -a

    # Lista todas as imagens disponíveis localmente.
    sudo docker images

    # Para o contêiner em execução chamado cadastur-container.
    sudo docker stop cadastur-container

    # Remove o contêiner chamado cadastur-container.
    sudo docker rm cadastur-container

    # Remove a imagem cadastur-image.
    sudo docker rmi cadastur-image

    # Baixa a imagem cadastur-image do registro remoto.
    docker pull cadastur-image

    # Envia a imagem cadastur-image para o registro remoto.
    docker push cadastur-image

    # Lista todas as redes disponíveis localmente.
    sudo docker network ls

    # Cria uma nova rede chamada cadastur-network
    sudo docker network create cadastur-network

    # Remove a rede chamada cadastur-networ
    sudo docker network rm cadastur-network
```

## Atualizar permissão de arquivo
``` bash
    # Dar permissão apenas para o proprietário usar
    chmod 600 ~/arquivo

    # Copia um arquivo de uma banda pra outras
    cp origem/file destino/file
```