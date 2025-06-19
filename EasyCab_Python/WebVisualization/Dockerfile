FROM python:3.9

WORKDIR /app

COPY ./common /app/common

COPY ./WebVisualization /app/WebVisualization

COPY ./WebVisualization/requirements.txt /app/WebVisualization/requirements.txt
RUN pip install --no-cache-dir -r /app/WebVisualization/requirements.txt

ENV PYTHONPATH=/app:$PYTHONPATH

EXPOSE 8080
EXPOSE 8765
# CMD ["python3", "/app/WebVisualization/map_websocket_proxy.py", "--kafka_broker", "kafka:29092"]