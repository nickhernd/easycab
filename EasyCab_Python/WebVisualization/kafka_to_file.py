from kafka import KafkaConsumer
import json
import time
import logging
import asyncio
import sys
import os

logging.basicConfig(level=logging.INFO, format='[%(name)s] %(message)s')
logger = logging.getLogger('KafkaToTCPSender')

KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:29092')
KAFKA_TOPIC = 'map_updates'

WS_SERVER_HOST = 'ws_server'
WS_SERVER_PORT = 8766

async def send_to_websocket_server(message):
    try:
        reader, writer = await asyncio.open_connection(WS_SERVER_HOST, WS_SERVER_PORT)
        message_str = json.dumps(message) + '\n'
        writer.write(message_str.encode('utf-8'))
        await writer.drain()
        writer.close()
        await writer.wait_closed()
        logger.info(f"Mensaje enviado a WS Server TCP: {message_str.strip()}")
    except ConnectionRefusedError:
        logger.warning(f"Conexión TCP rechazada a {WS_SERVER_HOST}:{WS_SERVER_PORT}. WS Server no está escuchando IPC.")
    except Exception as e:
        logger.error(f"Error al enviar mensaje TCP a WS Server: {e}")

async def consume_and_send():
    consumer = None
    while consumer is None:
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_BROKER,
                api_version=(0, 10, 1),
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                group_id='map_tcp_sender_group',
                auto_offset_reset='latest'
            )
            logger.info("Conexión a Kafka establecida para envío TCP.")
        except Exception as e:
            logger.error(f"Error al conectar con Kafka: {e}. Reintentando en 5 segundos...")
            await asyncio.sleep(5)

    try:
        for message in consumer:
            logger.info(f"Mensaje recibido de Kafka: {message.value}")
            await send_to_websocket_server(message.value)
            sys.stdout.flush()
    except Exception as e:
        logger.error(f"Error consumiendo de Kafka: {e}")
    finally:
        if consumer:
            consumer.close()
            logger.info("Consumidor de Kafka cerrado.")

if __name__ == "__main__":
    asyncio.run(consume_and_send())
