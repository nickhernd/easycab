import asyncio
import websockets
import json
import argparse
from kafka import KafkaConsumer
import logging
import socket
import os

logging.basicConfig(level=logging.INFO, format='[%(name)s] %(message)s')
logger = logging.getLogger('WebVisualization')

connected_clients = set()

async def ws_handler(websocket, path):
    logger.info(f"Cliente WebSocket conectado desde {websocket.remote_address}")
    connected_clients.add(websocket)
    try:
        await websocket.wait_closed()
    finally:
        connected_clients.discard(websocket)
        logger.info(f"Cliente WebSocket desconectado desde {websocket.remote_address}")

async def send_map_update(message):
    if not connected_clients:
        return

    message_json = json.dumps(message)
    disconnected_clients = set()
    for client in connected_clients:
        try:
            await client.send(message_json)
        except websockets.exceptions.ConnectionClosed:
            logger.warning(f"Cliente {client.remote_address} se desconectó.")
            disconnected_clients.add(client)
        except Exception as e:
            logger.error(f"Error al enviar mensaje a cliente {client.remote_address}: {e}")
            disconnected_clients.add(client)
    for client in disconnected_clients:
        connected_clients.discard(client)

async def consume_kafka_messages(kafka_broker, kafka_topic):
    logger.info(f"Conectando a Kafka en {kafka_broker}, topic: {kafka_topic}")
    consumer = None
    while consumer is None:
        try:
            consumer = KafkaConsumer(
                kafka_topic,
                bootstrap_servers=kafka_broker,
                api_version=(0, 10, 1),
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                group_id='map_visualization_group',
                auto_offset_reset='latest'
            )
            logger.info("Conexión a Kafka establecida.")
        except Exception as e:
            logger.error(f"Error al conectar con Kafka: {e}. Reintentando en 5 segundos...")
            await asyncio.sleep(5)

    try:
        for message in consumer:
            await send_map_update(message.value)
    except Exception as e:
        logger.error(f"Error consumiendo de Kafka: {e}")
    finally:
        if consumer:
            consumer.close()
            logger.info("Consumidor de Kafka cerrado.")

async def main():
    parser = argparse.ArgumentParser(description="WebSocket Proxy for EasyCab Kafka Messages")
    parser.add_argument("--kafka_broker", default=os.environ.get("KAFKA_BROKER", "localhost:9092"), help="Kafka broker address (e.g., localhost:9092)")
    parser.add_argument("--kafka_topic", default=os.environ.get("KAFKA_TOPIC", "map_updates"), help="Kafka topic to consume from")
    parser.add_argument("--ws_host", default=os.environ.get("WS_HOST", "0.0.0.0"), help="WebSocket server host")
    parser.add_argument("--ws_port", type=int, default=int(os.environ.get("WS_PORT", 8765)), help="WebSocket server port")
    args = parser.parse_args()

    ws_server = await websockets.serve(ws_handler, args.ws_host, args.ws_port, family=socket.AF_INET)
    logger.info(f"WebSocket server iniciado en {args.ws_host}:{args.ws_port} (IPv4).")

    kafka_task = asyncio.create_task(consume_kafka_messages(args.kafka_broker, args.kafka_topic))
    await ws_server.wait_closed()
    await kafka_task

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Servidor detenido por el usuario.")
    except Exception as e:
        logger.error(f"Error inesperado en la aplicación principal: {e}")
