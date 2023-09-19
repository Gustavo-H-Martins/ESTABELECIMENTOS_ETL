# Docker Build : sudo docker build -t cadastur-image .
# Docker Run : sudo docker run -d -i -t --name cadastur-container cadastur-image
# Flags : -t telemetria TTY, -d detach,-i interative 

FROM database-image:latest

WORKDIR /usr/src/cadastur

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
RUN mkdir -p /usr/src/app/files/
# RUN scp -i ./apiLeads.pem ubuntu@ec2-52-67-164-51.sa-east-1.compute.amazonaws.com:/home/ubuntu/app/files/backup/database.db /usr/src/app/files/

CMD ["sh", "-c", "python ./api/consumo_cadastur_api.py && scp -i ./apiLeads.pem /usr/src/app/files/database.db ubuntu@ec2-52-67-164-51.sa-east-1.compute.amazonaws.com:/home/ubuntu/app/files"]

# Rodar esse docker run:
# sudo docker run -dit --name cadastur-container -v "$(pwd)":/usr/src/cadastur -v "$(pwd)"/../app/files:/usr/src/app/files cadastur-image