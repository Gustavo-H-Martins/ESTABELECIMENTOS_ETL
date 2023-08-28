# Docker Build : sudo docker build -t cadastur-image .
# Docker Run : sudo docker run -d -i -t --name cadastur-container cadastur-image
# Flags : -t telemetria TTY, -d detach,-i interative 

FROM python:latest

WORKDIR /usr/src/cadastur

COPY requirements.txt ./
RUN python -m pip install --no-cache-dir -U pip
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

#CMD [ "python", "./api/consumo_cadastur_api.py" ]
CMD ["sh", "-c", "python ./api/consumo_cadastur_api.py && tail -f /dev/null"]
#CMD pip install -U pandas && python ./api/consumo_cadastur_api.py
