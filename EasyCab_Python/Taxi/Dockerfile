FROM python:3.9-slim-buster

WORKDIR /app

COPY ./common /app/common
COPY ./Taxi /app/Taxi
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

ENV PYTHONPATH=/app:$PYTHONPATH

CMD ["python3", "/app/Taxi/EC_DE.py"]
