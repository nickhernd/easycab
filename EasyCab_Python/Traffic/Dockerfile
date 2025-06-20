FROM python:3.9-slim-buster

WORKDIR /app/Traffic

COPY ./Traffic/requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY ./common /app/common

COPY ./Traffic .

ENV PYTHONPATH=/app:$PYTHONPATH

CMD ["python3", "EC_CTC.py", "--kafka_broker", "localhost:9094"]
