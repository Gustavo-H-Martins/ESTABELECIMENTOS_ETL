#!/bin/bash

# Copiar arquivos do contêiner para o host
sudo docker cp siga-container:/usr/src/app/files/database.db /tmp/database.db

# Copiar arquivos do host para a instância EC2 remota
scp -i "apiLeads.pem" /tmp/database.db ubuntu@ec2-52-67-164-51.sa-east-1.compute.amazonaws.com:/home/ubuntu/app/files
