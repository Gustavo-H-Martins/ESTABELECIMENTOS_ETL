# Docker Build: sudo docker build -t siga-image .
# Docker Run: sudo docker run -dit --name siga-container siga-image
# Flags: -t telemetria TTY, -d detach, -i interative

FROM python:3.9-slim

WORKDIR /usr/src/siga

COPY requirements.txt ./
RUN python -m pip install --no-cache-dir -U pip
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["sh", "-c", "python ./api/consumo_siga_api.py && tail -f /dev/null"]