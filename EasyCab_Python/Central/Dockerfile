FROM python:3.9

WORKDIR /Central
COPY . /Central/

COPY requirements.txt /Central/
RUN pip install --no-cache-dir -r requirements.txt

CMD ["python3", "EC_Central.py", "--listen_port", "65432", "--ip_port_broker", "kafka:29092"]