# Docker Build : sudo docker build -t database-image .
FROM python:3.9-slim

WORKDIR /usr/src/app/files/

# Essa imagem só copia o banco com os dados de informações e instancia o python  mesmo.
COPY ./database.db /usr/src/app/files/