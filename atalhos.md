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