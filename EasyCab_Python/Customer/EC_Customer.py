import time
import json
import sys
import os
import threading
from kafka import KafkaProducer, KafkaConsumer
import argparse

argparse = argparse.ArgumentParser(description='EasyCab Customer (EC_Customer) para gestionar solicitudes de servicio de taxis.')
argparse.add_argument('--kafka_broker', type=str, default='localhost:9094', help='Dirección del broker de Kafka para enviar mensajes.')
argparse.add_argument('--client_id', type=str, default='client_A', help='ID del cliente (opcional, por defecto "client_A").')
args = argparse.parse_args()


script_dir = os.path.dirname(__file__)
project_root = os.path.abspath(os.path.join(script_dir, '..'))
sys.path.insert(0, project_root)

from common.message_protocol import MessageProtocol

# --- Configuración del Cliente ---
CLIENT_ID = args.client_id
KAFKA_BROKER = args.kafka_broker
REQUESTS_FILE = 'customer_requests.txt'

# --- Estado del Cliente ---
current_request_index = 0
service_status = "idle" # idle, pending, accepted, denied, completed
current_assigned_taxi = None

# --- Datos del Mapa para Visualización ---
current_city_map = {}
current_taxi_fleet_state = {}
current_customer_requests_state = {}

# --- Kafka Producers ---
customer_producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# --- Kafka Consumers ---
service_notification_consumer = KafkaConsumer(
    'service_notifications',
    bootstrap_servers=[KAFKA_BROKER],
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    group_id=f'customer_notifications_group_{CLIENT_ID}' # Un group_id único para este cliente
)

# Kafka Consumer para actualizaciones del mapa
map_update_consumer = KafkaConsumer(
    'map_updates',
    bootstrap_servers=[KAFKA_BROKER],
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    group_id=f'map_viewer_group_customer_{CLIENT_ID}'
)

def send_kafka_message(topic, message):
    """Envía un mensaje JSON a un tema de Kafka."""
    try:
        customer_producer.send(topic, message)
        customer_producer.flush() 
        print(f"Cliente {CLIENT_ID} enviado a {topic}: {message}")
    except Exception as e:
        print(f"Cliente {CLIENT_ID}: Error enviando mensaje a Kafka ({topic}): {e}")

def load_customer_requests(file_path):
    """Carga las solicitudes del cliente desde un archivo."""
    requests = []
    try:
        with open(file_path, 'r') as f:
            for line in f:
                parts = line.strip().split()
                if len(parts) == 2 and parts[0] == CLIENT_ID:
                    destination_id = parts[1]
                    client_id = parts[0]
                    requests.append({"client_id": client_id, "destination_id": destination_id})
 
        print(f"Cliente {CLIENT_ID}: Solicitudes cargadas: {requests}")
    except FileNotFoundError:
        print(f"Error: Archivo de solicitudes '{file_path}' no encontrado.")
    except Exception as e:
        print(f"Error al cargar solicitudes: {e}")
    return requests

def process_service_notifications():
    """Procesa las notificaciones de servicio de la Central."""
    global service_status, current_assigned_taxi, current_request_index
    while True:
        try:
            for message in service_notification_consumer:
                print(f"[DEBUG] Cliente {CLIENT_ID}: Mensaje recibido en 'service_notifications': {message.value}")
                msg_value = message.value
                if msg_value.get("operation_code") == MessageProtocol.OP_SERVICE_NOTIFICATION:
                    data = msg_value["data"]
                    if data.get("client_id") == CLIENT_ID:
                        status = data.get("status")
                        msg = data.get("message")
                        taxi_id = data.get("taxi_id")
                        print(f"Cliente {CLIENT_ID}: Notificación de servicio - Status: {status}, Mensaje: {msg}, Taxi asignado: {taxi_id}")
                        if status == MessageProtocol.STATUS_OK:
                            print(f"[TRACE] Cliente {CLIENT_ID}: Servicio aceptado. Taxi asignado: {taxi_id}")
                            service_status = "accepted"
                            current_assigned_taxi = taxi_id
                        elif status == MessageProtocol.STATUS_KO:
                            print(f"[TRACE] Cliente {CLIENT_ID}: Servicio denegado por la central.")
                            service_status = "denied"
                            current_assigned_taxi = None
                elif msg_value.get("operation_code") == MessageProtocol.OP_SERVICE_COMPLETED:
                    data = msg_value["data"]
                    if data.get("client_id") == CLIENT_ID:
                        # Solo marcar como completado si el destino coincide con la solicitud actual
                        if 'destination_id' in data and 'requests' in globals() and current_request_index < len(requests):
                            expected_dest = requests[current_request_index]["destination_id"]
                            if data["destination_id"] == expected_dest:
                                print(f"Cliente {CLIENT_ID}: ¡Servicio completado por Taxi {data['taxi_id']} en {data['destination_id']}!")
                                print(f"[TRACE] Cliente {CLIENT_ID}: Estado cambiado a 'completed'.")
                                service_status = "completed"
                                current_assigned_taxi = None
                            else:
                                print(f"[WARN] Cliente {CLIENT_ID}: Servicio completado recibido pero destino no coincide. Esperado: {expected_dest}, Recibido: {data['destination_id']}")
                        else:
                            print(f"[WARN] Cliente {CLIENT_ID}: Servicio completado recibido pero no hay solicitudes activas o falta información.")
        except Exception as e:
            print(f"[ERROR] Cliente {CLIENT_ID}: Excepción en process_service_notifications: {e}")
            time.sleep(2)

def process_map_updates():
    """Procesa actualizaciones del mapa recibidas desde la Central (sin imprimir el mapa en consola)."""
    global current_city_map, current_taxi_fleet_state, current_customer_requests_state
    while True:
        try:
            for message in map_update_consumer:
                print(f"[DEBUG] Cliente {CLIENT_ID}: Mensaje recibido en 'map_updates': {message.value}")
                msg_value = message.value
                if msg_value.get("operation_code") == MessageProtocol.OP_MAP_UPDATE:
                    current_city_map = msg_value["data"]["city_map"]
                    current_taxi_fleet_state = msg_value["data"]["taxi_fleet"]
                    current_customer_requests_state = msg_value["data"]["customer_requests"]
        except Exception as e:
            print(f"[ERROR] Cliente {CLIENT_ID}: Excepción en process_map_updates: {e}")
            time.sleep(2)

def clear_console():
    if os.environ.get('TERM'):
        os.system('cls' if os.name == 'nt' else 'clear')

def draw_map():
    pass  # Eliminar impresión de mapa para depuración

def main():
    global current_request_index, service_status

    print(f"Iniciando EasyCab Customer (EC_Customer) para ID: {CLIENT_ID}")
    
    global requests
    requests = load_customer_requests(REQUESTS_FILE)
    if not requests:
        print(f"No hay solicitudes para el cliente {CLIENT_ID} en {REQUESTS_FILE}.")
        return

    # Iniciar hilos para procesar notificaciones y actualizaciones de mapa
    threading.Thread(target=process_service_notifications, daemon=True).start()
    threading.Thread(target=process_map_updates, daemon=True).start() # NUEVO: Hilo para procesar actualizaciones de mapa

    while current_request_index < len(requests):
        req = requests[current_request_index]
        destination_id = req["destination_id"]
        
        # Enviar solicitud solo si no hay un servicio en curso
        if service_status in ["idle", "denied", "completed"]:
            print(f"[TRACE] Cliente {CLIENT_ID}: Enviando solicitud de servicio a destino '{destination_id}'...")
            request_msg = MessageProtocol.create_customer_request(CLIENT_ID, destination_id)
            send_kafka_message('customer_requests', MessageProtocol.parse_message(request_msg))
            service_status = "pending" # Marcar como pendiente después de enviar
            print(f"[TRACE] Cliente {CLIENT_ID}: Estado cambiado a 'pending' tras enviar solicitud.")
        
        # Esperar hasta que el servicio sea procesado (aceptado/denegado/completado)
        while service_status == "pending" or service_status == "accepted":
            time.sleep(1) 
            print(f"[TRACE] Cliente {CLIENT_ID}: Esperando procesamiento de solicitud. Estado: {service_status}")

        if service_status == "completed":
            print(f"[TRACE] Cliente {CLIENT_ID}: Servicio '{current_request_index + 1}' completado. Pasando a la siguiente solicitud (si existe).")
            current_request_index += 1
            service_status = "idle" 
            time.sleep(4) 
        elif service_status == "denied":
            print(f"[TRACE] Cliente {CLIENT_ID}: Servicio '{current_request_index + 1}' denegado. Intentando la siguiente solicitud (si existe).")
            current_request_index += 1
            service_status = "idle"
            time.sleep(4) 

    print(f"Cliente {CLIENT_ID}: Todas las solicitudes procesadas. Saliendo.")

if __name__ == "__main__":
    main()