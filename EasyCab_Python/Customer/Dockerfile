FROM python:3.9-slim-buster

WORKDIR /app/Customer

COPY ./Customer/requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY ./common /app/common

COPY ./Customer .

ENV PYTHONPATH=/app:$PYTHONPATH

CMD ["python3", "EC_Customer.py", "--kafka_broker", "localhost:9094", "--client_id", "a"]
