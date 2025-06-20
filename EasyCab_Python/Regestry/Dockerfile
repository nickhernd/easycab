FROM python:3.9-slim-buster

WORKDIR /app/Regestry

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY ./Regestry/cert.pem .
COPY ./Regestry/key.pem .
COPY ./Regestry .

CMD ["python3", "EC_Registry.py"]