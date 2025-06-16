FROM alpine:latest

WORKDIR /Central
COPY . .

RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "central.py", "--listen_port", "65432", "--ip_port_broker", "localhost::9094"]

