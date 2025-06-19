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
    global service_status, current_assigned_taxi

    for message in service_notification_consumer:
        msg_value = message.value
        if msg_value.get("operation_code") == MessageProtocol.OP_SERVICE_NOTIFICATION:
            data = msg_value["data"]
            if data.get("client_id") == CLIENT_ID:
                status = data.get("status")
                msg = data.get("message")
                taxi_id = data.get("taxi_id")
                
                print(f"Cliente {CLIENT_ID}: Notificación de servicio - Status: {status}, Mensaje: {msg}")

                if status == MessageProtocol.STATUS_OK:
                    service_status = "accepted"
                    current_assigned_taxi = taxi_id
                elif status == MessageProtocol.STATUS_KO:
                    service_status = "denied"
                    current_assigned_taxi = None
                
        elif msg_value.get("operation_code") == MessageProtocol.OP_SERVICE_COMPLETED:
            data = msg_value["data"]
            if data.get("client_id") == CLIENT_ID:
                print(f"Cliente {CLIENT_ID}: ¡Servicio completado por Taxi {data['taxi_id']} en {data['destination_id']}!")
                service_status = "completed"
                current_assigned_taxi = None 

def process_map_updates():
    """Escucha y procesa actualizaciones del mapa de la Central."""
    global current_city_map, current_taxi_fleet_state, current_customer_requests_state
    for message in map_update_consumer:
        msg_value = message.value
        if msg_value.get("operation_code") == MessageProtocol.OP_MAP_UPDATE:
            current_city_map = msg_value["data"]["city_map"]
            current_taxi_fleet_state = msg_value["data"]["taxi_fleet"]
            current_customer_requests_state = msg_value["data"]["customer_requests"]
            draw_map() 

def clear_console():
    if os.environ.get('TERM'):
        os.system('cls' if os.name == 'nt' else 'clear')

def draw_map():
    """Dibuja el mapa ASCII en la consola."""
    clear_console()
    print("-" * 30)
    print(f"Cliente {CLIENT_ID} | Estado de Servicio: {service_status}")
    if current_assigned_taxi:
        print(f"  Taxi Asignado: {current_assigned_taxi}")
    print("-" * 30)

    max_x = max([loc['x'] for loc in current_city_map.values()] + [taxi['x'] for taxi in current_taxi_fleet_state.values()] + [cust['origin_coords']['x'] for cust in current_customer_requests_state.values()])
    max_y = max([loc['y'] for loc in current_city_map.values()] + [taxi['y'] for taxi in current_taxi_fleet_state.values()] + [cust['origin_coords']['y'] for cust in current_customer_requests_state.values()])

    grid_width = max_x + 1
    grid_height = max_y + 1

    grid = [[' . ' for _ in range(grid_width)] for _ in range(grid_height)]

    # Colocar ubicaciones de la ciudad
    for loc_id, coords in current_city_map.items():
        if 0 <= coords['y'] < grid_height and 0 <= coords['x'] < grid_width:
            grid[coords['y']][coords['x']] = f" {loc_id} "

    # Colocar clientes (especialmente el cliente actual)
    for client_id, req_data in current_customer_requests_state.items():
        cx, cy = req_data['origin_coords']['x'], req_data['origin_coords']['y']
        if 0 <= cy < grid_height and 0 <= cx < grid_width:
            if client_id == CLIENT_ID:
                grid[cy][cx] = f"({CLIENT_ID[0].lower()})" # El cliente actual
            elif grid[cy][cx] == ' . ': # Otros clientes
                grid[cy][cx] = f" {client_id[0].lower()} "

    # Colocar taxis
    for taxi_id, taxi_data in current_taxi_fleet_state.items():
        tx, ty = taxi_data['x'], taxi_data['y']
        if 0 <= ty < grid_height and 0 <= tx < grid_width:
            grid[ty][tx] = f"[T{taxi_id}]"

    # Imprimir el grid (invertir Y para que (0,0) sea abajo izquierda)
    for row in reversed(grid):
        print("".join(row))
    print("-" * 30)


def main():
    global current_request_index, service_status

    print(f"Iniciando EasyCab Customer (EC_Customer) para ID: {CLIENT_ID}")
    
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
            print(f"Cliente {CLIENT_ID}: Enviando solicitud de servicio a destino '{destination_id}'...")
            request_msg = MessageProtocol.create_customer_request(CLIENT_ID, destination_id)
            send_kafka_message('customer_requests', MessageProtocol.parse_message(request_msg))
            service_status = "pending" # Marcar como pendiente después de enviar
        
        # Esperar hasta que el servicio sea procesado (aceptado/denegado/completado)
        while service_status == "pending" or service_status == "accepted":
            time.sleep(1) 
            print(f"Cliente {CLIENT_ID}: Esperando procesamiento de solicitud. Estado: {service_status}") # Depuración

        if service_status == "completed":
            print(f"Cliente {CLIENT_ID}: Servicio '{current_request_index + 1}' completado. Pasando a la siguiente solicitud (si existe).")
            current_request_index += 1
            service_status = "idle" 
            time.sleep(4) 
        elif service_status == "denied":
            print(f"Cliente {CLIENT_ID}: Servicio '{current_request_index + 1}' denegado. Intentando la siguiente solicitud (si existe).")
            current_request_index += 1
            service_status = "idle"
            time.sleep(4) 

    print(f"Cliente {CLIENT_ID}: Todas las solicitudes procesadas. Saliendo.")

if __name__ == "__main__":
    main()