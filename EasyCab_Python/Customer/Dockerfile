FROM python:3.9

WORKDIR /Customer
COPY . /Customer/

COPY requirements.txt /Customer/
RUN pip install --no-cache-dir -r requirements.txt

CMD ["python3", "EC_Customer.py", "--kafka_broker", "kafka:29092", "--client_id", "a"]