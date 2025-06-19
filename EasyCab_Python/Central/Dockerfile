FROM python:3.9

WORKDIR /app

COPY ./common /app/common

COPY ./Central /app/Central

COPY ./Central/requirements.txt /app/Central/requirements.txt
RUN pip install --no-cache-dir -r /app/Central/requirements.txt

ENV PYTHONPATH=/app:$PYTHONPATH

CMD ["python3", "/app/Central/EC_Central.py", "--listen_port", "65432", "--ip_port_broker", "kafka:29092"]
