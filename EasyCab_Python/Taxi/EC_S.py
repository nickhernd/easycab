import time
import json
import sys
import os
import threading
from kafka import KafkaProducer
import argparse

# Argumentos de línea de comandos para configurar el sensor
argparse = argparse.ArgumentParser(description='EasyCab Sensor (EC_S) para simular y enviar datos de sensores de taxis.')
argparse.add_argument('--ip_port_ecde', type=str, default='localhost:9095', help='Dirección IP y puerto del ECDE (EasyCab Data Engine).')
argparse.add_argument('--id_taxi', type=int, required=True, help='ID del taxi al que está asociado este sensor.')
argparse.add_argument('--kafka_broker', type=str, default='localhost:9092', help='Dirección del broker de Kafka para enviar mensajes.')
args = argparse.parse_args()

# Añade el directorio raíz del proyecto a sys.path para importar módulos comunes
script_dir = os.path.dirname(__file__)
project_root = os.path.abspath(os.path.join(script_dir, '..'))
sys.path.insert(0, project_root)

from common.message_protocol import MessageProtocol

# --- Configuración del Sensor ---
TAXI_ID = args.id_taxi
KAFKA_BROKER = args.kafka_broker
SENSOR_UPDATE_INTERVAL = 1 # Intervalo de envío de datos del sensor en segundos
IP_PORT_ECDE = args.ip_port_ecde

# --- Estado del Sensor ---
current_sensor_status = MessageProtocol.STATUS_OK # Estado inicial del sensor
anomaly_details = "" # Detalles de la anomalía si el estado es KO

# --- Kafka Producer para el Sensor ---
sensor_producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def send_kafka_message(topic, message):
    """Envía un mensaje JSON a un tema de Kafka."""
    try:
        sensor_producer.send(topic, message)
        sensor_producer.flush()
    except Exception as e:
        print(f"Sensor (Taxi {TAXI_ID}): Error enviando mensaje a Kafka ({topic}): {e}")

def simulate_and_send_sensor_data():
    """Simula y envía periódicamente el estado del sensor."""
    global current_sensor_status, anomaly_details
    while True:
        sensor_msg = MessageProtocol.create_sensor_update(
            taxi_id=TAXI_ID,
            status=current_sensor_status,
            anomaly_details=anomaly_details
        )
        send_kafka_message('sensor_data', MessageProtocol.parse_message(sensor_msg))
        time.sleep(SENSOR_UPDATE_INTERVAL)

def listen_for_input():
    """Permite al usuario simular fallos o restauraciones del sensor mediante la entrada de teclado."""
    global current_sensor_status, anomaly_details
    print(f"Sensor (Taxi {TAXI_ID}): Pulsa 'k' para simular un fallo (KO), 'o' para restaurar (OK).")
    while True:
        user_input = input().strip().lower()
        if user_input == 'k':
            if current_sensor_status == MessageProtocol.STATUS_OK:
                current_sensor_status = MessageProtocol.STATUS_KO
                anomaly_details = "Fallo simulado por usuario."
                print(f"Sensor (Taxi {TAXI_ID}): ¡Simulando fallo (KO)! El taxi debería detenerse.")
            else:
                print(f"Sensor (Taxi {TAXI_ID}): El sensor ya está en estado KO.")
        elif user_input == 'o':
            if current_sensor_status == MessageProtocol.STATUS_KO:
                current_sensor_status = MessageProtocol.STATUS_OK
                anomaly_details = "Restaurado por usuario."
                print(f"Sensor (Taxi {TAXI_ID}): ¡Simulando restauración (OK)! El taxi debería reanudar.")
            else:
                print(f"Sensor (Taxi {TAXI_ID}): El sensor ya está en estado OK.")
        else:
            print(f"Entrada inválida. Pulsa 'k' para KO, 'o' para OK.")

def main():
    print(f"Iniciando EasyCab Sensor (EC_S) para el Taxi ID: {TAXI_ID}")

    # Inicia el hilo para simular y enviar datos del sensor
    sensor_thread = threading.Thread(target=simulate_and_send_sensor_data, daemon=True)
    sensor_thread.start()

    # Inicia el hilo para escuchar la entrada del usuario
    input_thread = threading.Thread(target=listen_for_input, daemon=True)
    input_thread.start()

    # Mantiene el programa principal en ejecución
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print(f"\nSensor (Taxi {TAXI_ID}): Cerrando...")
        sys.exit(0)

if __name__ == "__main__":
    main()
