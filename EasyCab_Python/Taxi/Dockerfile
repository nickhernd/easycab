FROM python:3.9

WORKDIR /Taxi
COPY . /Taxi/

COPY requirements.txt /Taxi/
RUN pip install --no-cache-dir -r requirements.txt

CMD ["python3", "EC_DE.py", "--ip_port_ecc", "central:65432", "--kafka_broker", "kafka:29092", "--ip_port_ecs", "central:65432", "--taxi_id", "1"]