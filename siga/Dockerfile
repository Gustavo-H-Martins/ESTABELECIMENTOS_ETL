# Docker Build: sudo docker build -t siga-image .
# Docker Run: sudo docker run -dit --name siga-container siga-image
# Flags: -t telemetria TTY, -d detach, -i interative

FROM python:3.9-slim

WORKDIR /usr/src/siga

COPY requirements.txt ./
RUN python -m pip install --no-cache-dir -U pip
RUN pip install --no-cache-dir -r requirements.txt

# Instala o pacote openssh-client
RUN apt-get update && apt-get install -y openssh-client

# Adiciona a chave do host à lista de hosts conhecidos
RUN mkdir -p ~/.ssh && \
    ssh-keyscan ec2-52-67-164-51.sa-east-1.compute.amazonaws.com >> ~/.ssh/known_hosts

COPY . .
COPY apiLeads.pem ./
RUN chmod 600 apiLeads.pem
# Comando para manter o container ativo após finalizar o script : tail -f /dev/null
CMD ["sh", "-c", "python ./api/consumo_siga_api.py && scp -i ./apiLeads.pem /usr/src/app/files/database.db ubuntu@ec2-52-67-164-51.sa-east-1.compute.amazonaws.com:/home/ubuntu/app/files && echo CORREU TUDO BEM AQUI PAIZÃO!!!"] 