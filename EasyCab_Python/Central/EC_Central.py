print("=== EC_Central.py iniciado ===")
import sys
print("Python version:", sys.version)

import socket
import threading
import json
import time
import sys
import os
import argparse
from kafka import KafkaProducer, KafkaConsumer
import copy
import requests
from datetime import datetime
import secrets

script_dir = os.path.dirname(__file__)
project_root = os.path.abspath(os.path.join(script_dir, '..'))
sys.path.insert(0, project_root)
sys.path.insert(0, '/common')

from common.message_protocol import MessageProtocol

# --- Configuración General ---
argparser = argparse.ArgumentParser()
argparser.add_argument('--listen_port', type=int, default=int(os.environ.get('CENTRAL_PORT_AUTH', 6000)), help='Puerto para autenticación de taxis (via sockets)')
argparser.add_argument('--ip_port_broker', type=str, default=os.environ.get('KAFKA_BROKER', 'localhost:9092'), help='Dirección del broker de Kafka')
argparser.add_argument('--central_host', type=str, default=os.environ.get('CENTRAL_HOST', '0.0.0.0'), help='Host/IP para el servidor de autenticación de taxis')
argparser.add_argument('--traffic_api', type=str, default=os.environ.get('TRAFFIC_API', 'http://traffic:5001/traffic_status'), help='URL del API REST de tráfico')
args = argparser.parse_args()

CENTRAL_PORT_AUTH = args.listen_port
KAFKA_BROKER = args.ip_port_broker
CENTRAL_HOST = args.central_host
TRAFFIC_API_URL = args.traffic_api
MAP_UPDATE_INTERVAL = 1
MAP_SIZE = 20

def wrap_position(x, y):
    """Devuelve las coordenadas ajustadas para la geometría esférica."""
    return x % MAP_SIZE, y % MAP_SIZE

# --- Estructuras de datos globales (simulando una base de datos) ---
# { "ID_LOCALIZACION": {"x": int, "y": int} }
CITY_MAP = {}
# { "ID_TAXI": {"x": int, "y": int, "status": str, "service_id": str, "current_destination_coords": (x, y)} }
# Status: "free", "occupied", "moving_to_customer", "moving_to_destination", "disabled"
TAXI_FLEET = {}
# { "CLIENT_ID": {"destination_id": str, "assigned_taxi_id": int or None, "status": str, "origin_coords": (x,y)} }
# Status: "pending", "assigned", "picked_up", "completed", "cancelled"
CUSTOMER_REQUESTS = {}

# --- Variables globales de Kafka ---
central_producer = None
taxi_position_consumer = None
sensor_data_consumer = None
customer_requests_consumer = None
central_service_notification_consumer = None
taxi_pickup_consumer = None
taxi_dropoff_consumer = None
taxis_available_consumer = None


# --- Auditoría ---
AUDIT_LOG = [] # Almacena los eventos de auditoría
TRAFFIC_STATUS = "UNKNOWN" # OK, KO, UNKNOWN

# Escribir en taxi_avilable.txt cada vez que se llama la funcion taxi_id x y estatus enviado por EC_DE


def add_audit_event(event):
    # En una aplicación real, esto se guardaría en una base de datos o sistema de logs.
    pass

def log_audit_event(who, ip, action, description):
    """Registra un evento de auditoría estructurada."""
    now = datetime.now().isoformat()
    event = {
        "timestamp": now,
        "who": who,
        "ip": ip,
        "action": action,
        "description": description
    }
    print(f"[AUDIT] {now} | {who} | {ip} | {action} | {description}")
    AUDIT_LOG.append(event)
    add_audit_event(event)

def check_traffic_status_periodically():
    """Consulta el API REST de EC_CTC cada 10 segundos para conocer el estado del tráfico."""
    global TRAFFIC_STATUS
    while True:
        try:
            response = requests.get(TRAFFIC_API_URL)
            if response.status_code == 200:
                data = response.json()
                new_status = data.get("status", "UNKNOWN")
                if new_status != TRAFFIC_STATUS:
                    log_audit_event(
                        who="EC_Central",
                        ip=CENTRAL_HOST,
                        action="TrafficStatusChange",
                        description=f"Estado del tráfico cambiado a {new_status}"
                    )
                TRAFFIC_STATUS = new_status
            else:
                log_audit_event(
                    who="EC_Central",
                    ip=CENTRAL_HOST,
                    action="TrafficStatusError",
                    description=f"Respuesta inesperada del API CTC: {response.status_code}"
                )
        except Exception as e:
            log_audit_event(
                who="EC_Central",
                ip=CENTRAL_HOST,
                action="TrafficStatusError",
                description=f"Error al consultar el API CTC: {e}"
            )
        time.sleep(10)

# Dummy function for run_audit_api to allow the code to run without an actual API implementation
def run_audit_api():
    print("Audit API server placeholder started.")
    # In a real scenario, this would start a Flask/Django server for the audit API
    pass

# --- Funciones de Carga de Datos ---
def load_city_map():
    """Carga el mapa de la ciudad desde config_map.txt."""
    try:
        with open('Central/config_map.txt', 'r') as f:
            for line in f:
                parts = line.strip().split()
                if len(parts) == 3:
                    loc_id = parts[0]
                    x, y = int(parts[1]), int(parts[2])
                    CITY_MAP[loc_id] = {"x": x, "y": y}
        print(f"Mapa de la ciudad cargado: {CITY_MAP}")
    except FileNotFoundError:
        print("Error: config_map.txt no encontrado.")
    except Exception as e:
        print(f"Error al cargar el mapa: {e}")

def load_taxi_fleet():
    """Carga la flota de taxis inicial desde taxis_available.txt."""
    try:
        with open('Central/taxis_available.txt', 'r') as f:
            for line in f:
                parts = line.strip().split()
                if len(parts) == 4:
                    taxi_id = int(parts[0])
                    x, y = int(parts[1]), int(parts[2])
                    status = parts[3]
                    TAXI_FLEET[taxi_id] = {
                        "x": x, "y": y, "status": status, "service_id": None,
                        "current_destination_coords": None,
                        "initial_x": x, "initial_y": y   # Guarda la posición inicial
                    }
        print(f"Flota de taxis cargada: {TAXI_FLEET}")
    except FileNotFoundError:
        print("Error: taxis_available.txt no encontrado.")
    except Exception as e:
        print(f"Error al cargar la flota de taxis: {e}")

# --- Funciones de Kafka ---
def init_kafka_clients():
    """Inicializa los productores y consumidores de Kafka."""
    global taxis_available_consumer, central_producer, taxi_position_consumer, sensor_data_consumer, customer_requests_consumer, central_service_notification_consumer, taxi_pickup_consumer, taxi_dropoff_consumer
    try:
        taxis_aavilable_consumer = KafkaConsumer(
            'taxis_available',
            bootstrap_servers=[KAFKA_BROKER],
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        )

        central_taxi_id_producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

        central_producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        taxi_position_consumer = KafkaConsumer(
            'taxi_movements',
            bootstrap_servers=[KAFKA_BROKER],
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            group_id='central_group',
            auto_offset_reset='latest' # 'earliest' para empezar desde el principio, 'latest' para nuevos mensajes
        )
        sensor_data_consumer = KafkaConsumer(
            'sensor_data',
            bootstrap_servers=[KAFKA_BROKER],
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            group_id='central_group',
            auto_offset_reset='latest'
        )
        customer_requests_consumer = KafkaConsumer(
            'customer_requests',
            bootstrap_servers=[KAFKA_BROKER],
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            group_id='central_group',
            auto_offset_reset='latest'
        )
        central_service_notification_consumer = KafkaConsumer(
            'service_notifications',
            bootstrap_servers=[KAFKA_BROKER],
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            group_id='central_service_consumer_group',
            auto_offset_reset='latest'
        )
        taxi_pickup_consumer = KafkaConsumer(
            'taxi_pickups',
            bootstrap_servers=[KAFKA_BROKER],
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            group_id='central_group',
            auto_offset_reset='latest'
        )
        taxi_dropoff_consumer = KafkaConsumer(
            'taxi_dropoffs',
            bootstrap_servers=[KAFKA_BROKER],
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            group_id='central_group',
            auto_offset_reset='latest'
        )
        print("Clientes Kafka inicializados correctamente.")
    except Exception as e:
        print(f"Error al inicializar clientes Kafka: {e}")
        sys.exit(1) # Salir si Kafka no puede inicializarse


def send_central_update(topic, message):
    """Envía un mensaje JSON a un tema de Kafka usando el producer central."""
    try:
        central_producer.send(topic, message)
        print(f"[Central] Enviado a {topic}: {message}")
        central_producer.flush()
    except Exception as e:
        print(f"Error al enviar mensaje a Kafka ({topic}): {e}")

def process_taxi_movement_messages():
    """Procesa los mensajes del tema 'taxi_movements'."""
    global TAXI_FLEET, CUSTOMER_REQUESTS
    for message in taxi_position_consumer:
        msg_value = message.value
        if msg_value.get("operation_code") == MessageProtocol.OP_TAXI_POSITION:
            taxi_id = msg_value["data"]["taxi_id"]
            x = msg_value["data"]["x"]
            y = msg_value["data"]["y"]
            status = msg_value["data"]["status"]

            # Handle potential ID change (e.g., '1' to '1a')
            original_taxi_id = taxi_id
            if isinstance(taxi_id, str) and taxi_id.endswith('a'):
                base_taxi_id = taxi_id[:-1]
            else:
                base_taxi_id = str(taxi_id)

            if taxi_id in TAXI_FLEET:
                taxi = TAXI_FLEET[taxi_id]
            elif base_taxi_id in TAXI_FLEET and str(taxi_id) != base_taxi_id: # Only re-map if the ID actually changed
                taxi = TAXI_FLEET[base_taxi_id]
                TAXI_FLEET[taxi_id] = taxi
                del TAXI_FLEET[base_taxi_id]
                print(f"Central: Taxi ID changed from {base_taxi_id} to {taxi_id}")
            elif base_taxi_id in TAXI_FLEET and str(taxi_id) == base_taxi_id: # If it's the base ID, just use it
                taxi = TAXI_FLEET[base_taxi_id]
                taxi_id = base_taxi_id # Ensure we use the canonical ID for internal dictionary
            else:
                print(f"Advertencia: Movimiento de taxi no registrado o ID {taxi_id} no gestionado. Saltando actualización.")
                continue

            # Update the taxi's position and status
            taxi["x"] = x
            taxi["y"] = y
            taxi["status"] = status

            if status == "free" and taxi.get("status_before_free") == "returning_to_base":
                print(f"Taxi {taxi_id} ha llegado a su base y está libre.")
                taxi["service_id"] = None
                taxi["current_destination_coords"] = None
                taxi["status_before_free"] = None # Reset flag
            elif status == "free":
                taxi["service_id"] = None
                taxi["current_destination_coords"] = None


            # Emit map update JSON in every movement
            map_state_message = MessageProtocol.create_map_update(
                city_map=copy.deepcopy(CITY_MAP),
                taxi_fleet=copy.deepcopy(TAXI_FLEET),
                customer_requests=copy.deepcopy(CUSTOMER_REQUESTS)
            )
            send_central_update('map_updates', MessageProtocol.parse_message(map_state_message))

def process_sensor_data_messages():
    """Procesa los mensajes del tema 'sensor_data'."""
    for message in sensor_data_consumer:
        msg_value = message.value
        if msg_value.get("operation_code") == MessageProtocol.OP_SENSOR_UPDATE:
            taxi_id = msg_value["data"]["taxi_id"]
            status = msg_value["data"]["status"] # "OK" o "KO"
            anomaly_details = msg_value["data"].get("anomaly_details", "")
            if taxi_id in TAXI_FLEET:
                if status == MessageProtocol.STATUS_KO:
                    print(f"¡ALERTA! Sensor de Taxi {taxi_id} reporta KO. Detalles: {anomaly_details}. Deshabilitando taxi.")
                    TAXI_FLEET[taxi_id]["status"] = "disabled"
                elif status == MessageProtocol.STATUS_OK:
                    if TAXI_FLEET[taxi_id]["status"] == "disabled":
                        print(f"Taxi {taxi_id} se ha recuperado (sensores OK). Marcando como libre.")
                        TAXI_FLEET[taxi_id]["status"] = "free"
            else:
                print(f"Advertencia: Actualización de sensor de taxi no registrado: {taxi_id}")

def draw_map_console(city_map, taxi_fleet, customer_requests):
    """Dibuja el mapa 20x20 en consola con colores y wrap-around."""
    grid = [["   " for _ in range(MAP_SIZE)] for _ in range(MAP_SIZE)]

    # Localizaciones (mayúsculas, fondo azul)
    for loc_id, coords in city_map.items():
        x, y = wrap_position(coords['x'], coords['y'])
        grid[y][x] = f"\033[44m {loc_id} \033[0m"   # Fondo azul

    # Clientes (minúsculas, fondo amarillo)
    for client_id, req_data in customer_requests.items():
        if "origin_coords" in req_data:
            x, y = wrap_position(req_data["origin_coords"]["x"], req_data["origin_coords"]["y"])
            grid[y][x] = f"\033[43m {client_id[0].lower()} \033[0m"   # Fondo amarillo

    # Taxis (número, verde si moviéndose, rojo si parado)
    for taxi_id, taxi_data in taxi_fleet.items():
        x, y = wrap_position(taxi_data["x"], taxi_data["y"])
        if taxi_data["status"] in ["moving_to_customer", "moving_to_destination"]:
            color = "\033[42m"   # Fondo verde
        else:
            color = "\033[41m"   # Fondo rojo
        grid[y][x] = f"{color}{str(taxi_id).rjust(3)}\033[0m"

    # Imprimir el grid (Y invertido para que 0,0 sea abajo izquierda)
    for row in reversed(grid):
        print("".join(row))
    print("-" * 60)

def process_customer_request_messages():
    """Procesa los mensajes del tema 'customer_requests'."""
    for message in customer_requests_consumer:
        msg_value = message.value
        if msg_value.get("operation_code") == MessageProtocol.OP_CUSTOMER_REQUEST:
            client_id = msg_value["data"]["client_id"]
            destination_id = msg_value["data"]["destination_id"]

            # --- MODIFICACIÓN AQUÍ: Usa el client_id como el origin_id ---
            # Esto asume que el ID del cliente (ej. 'a', 'b') coincide con un ID de ubicación en CITY_MAP
            client_origin_id = client_id

            if client_origin_id not in CITY_MAP:
                print(f"Error: Origen del cliente '{client_origin_id}' (basado en client_id) no encontrado en el mapa.")
                notification_msg = MessageProtocol.create_service_notification(
                    client_id=client_id, status=MessageProtocol.STATUS_KO, message=f"Origen '{client_origin_id}' no válido. Servicio denegado."
                )
                send_central_update('service_notifications', MessageProtocol.parse_message(notification_msg))
                return

            if destination_id not in CITY_MAP:
                print(f"Error: Destino '{destination_id}' no encontrado en el mapa.")
                notification_msg = MessageProtocol.create_service_notification(
                    client_id=client_id, status=MessageProtocol.STATUS_KO, message=f"Destino '{destination_id}' no válido. Servicio denegado."
                )
                send_central_update('service_notifications', MessageProtocol.parse_message(notification_msg))
                return

            print(f"Nueva solicitud de cliente '{client_id}' con destino '{destination_id}' desde origen '{client_origin_id}'")

            if client_id not in CUSTOMER_REQUESTS or CUSTOMER_REQUESTS[client_id]["status"] == "completed":
                CUSTOMER_REQUESTS[client_id] = {
                    "destination_id": destination_id,
                    "assigned_taxi_id": None,
                    "status": "pending",
                    "origin_coords": CITY_MAP[client_origin_id] # Usamos el client_origin_id ahora correcto
                }

                assign_taxi_to_request(client_id, destination_id)

            else:
                print(f"Cliente {client_id} ya tiene una solicitud {CUSTOMER_REQUESTS[client_id]['status']}.")
                notification_msg = MessageProtocol.create_service_notification(
                    client_id=client_id, status=MessageProtocol.STATUS_KO, message=f"Su solicitud ya está en curso (Estado: {CUSTOMER_REQUESTS[client_id]['status']})."
                )
                send_central_update('service_notifications', MessageProtocol.parse_message(notification_msg))



def assign_taxi_to_request(client_id, destination_id):
    """Intenta asignar un taxi libre a una solicitud de cliente."""
    assigned = False
    client_origin_coords = CUSTOMER_REQUESTS[client_id]["origin_coords"]
    final_destination_coords = CITY_MAP[destination_id]

    for taxi_id, taxi_data in TAXI_FLEET.items():
        if taxi_data["status"] == "free":
            TAXI_FLEET[taxi_id]["status"] = "moving_to_customer"
            TAXI_FLEET[taxi_id]["service_id"] = client_id
            TAXI_FLEET[taxi_id]["current_destination_coords"] = client_origin_coords
            CUSTOMER_REQUESTS[client_id]["assigned_taxi_id"] = taxi_id
            CUSTOMER_REQUESTS[client_id]["status"] = "assigned"

            print(f"Asignado Taxi {taxi_id} a cliente {client_id}. Taxi va a recoger en ({client_origin_coords['x']},{client_origin_coords['y']}). Destino final: {destination_id} ({final_destination_coords['x']},{final_destination_coords['y']})")

            notification_msg = MessageProtocol.create_service_notification(
                client_id=client_id, status=MessageProtocol.STATUS_OK, taxi_id=taxi_id, message=f"Servicio aceptado. Taxi {taxi_id} en camino a recogerle."
            )
            send_central_update('service_notifications', MessageProtocol.parse_message(notification_msg))

            taxi_command_msg = MessageProtocol.create_taxi_command(
                taxi_id=taxi_id, command="PICKUP", new_destination_coords=client_origin_coords, client_id=client_id, final_destination_id=destination_id
            )
            send_central_update('taxi_commands', MessageProtocol.parse_message(taxi_command_msg))

            assigned = True
            break

    if not assigned:
        print(f"No hay taxis libres para la solicitud del cliente {client_id}. Enviando notificación KO.")
        notification_msg = MessageProtocol.create_service_notification(
            client_id=client_id, status=MessageProtocol.STATUS_KO, message="Lo sentimos, no hay taxis disponibles en este momento."
        )
        send_central_update('service_notifications', MessageProtocol.parse_message(notification_msg))

def process_service_completed_messages():
    """Procesa los mensajes del tema 'service_notifications' cuando el taxi finaliza un servicio."""
    for message in central_service_notification_consumer:
        msg_value = message.value
        if msg_value.get("operation_code") == MessageProtocol.OP_SERVICE_COMPLETED:
            client_id = msg_value["data"]["client_id"]
            taxi_id = msg_value["data"]["taxi_id"]
            destination_id = msg_value["data"]["destination_id"]
            print(f"Central (Service Notifications): Confirmed service completed for client {client_id} by Taxi {taxi_id} at destination {destination_id}")
            
def process_taxi_pickup_messages():
    """Procesa los mensajes del topic 'taxi_pickups' para detectar recogidas y enviar el comando de ir al destino."""
    global TAXI_FLEET, CUSTOMER_REQUESTS
    for message in taxi_pickup_consumer:
        print(f"[Central DEBUG] Recibido mensaje en taxi_pickups: {message.value}") # DEBUG: Confirm message receipt
        msg_value = message.value
        if msg_value.get("operation_code") == MessageProtocol.OP_TAXI_PICKUP:
            taxi_id_from_msg = msg_value["data"]["taxi_id"]
            client_id = msg_value["data"]["client_id"]
            pickup_coords = msg_value["data"]["pickup_coords"]

            # Handle potential ID change (e.g., '1' to '1a')
            current_taxi_id = taxi_id_from_msg
            if taxi_id_from_msg not in TAXI_FLEET:
                # If the taxi ID changed (e.g., from '1' to '1a'), try the base ID
                base_taxi_id = taxi_id_from_msg[:-1]
                if base_taxi_id in TAXI_FLEET:
                    TAXI_FLEET[taxi_id_from_msg] = TAXI_FLEET.pop(base_taxi_id)
                    current_taxi_id = taxi_id_from_msg
                    print(f"Central: Taxi ID updated from {base_taxi_id} to {current_taxi_id} upon pickup confirmation.")
                else:
                    print(f"Central: Advertencia: Taxi ID {taxi_id_from_msg} (o su base) no encontrado en TAXI_FLEET.")
                    continue # Skip if taxi not found
            
            # Ensure the taxi is actually in our fleet after ID handling
            if current_taxi_id not in TAXI_FLEET:
                print(f"Central: Error crítico: Taxi {current_taxi_id} no está en TAXI_FLEET después de manejo de ID. Saltando.")
                continue

            if client_id in CUSTOMER_REQUESTS: # Check if request still exists
                taxi = TAXI_FLEET[current_taxi_id]
                customer_request = CUSTOMER_REQUESTS[client_id]

                print(f"Central: Taxi {current_taxi_id} ha recogido al cliente {client_id} en ({pickup_coords['x']},{pickup_coords['y']}). Preparando comando para ir al destino...")

                # Retrieve the final destination coordinates from CITY_MAP
                final_destination_coords = CITY_MAP.get(customer_request["destination_id"])
                if not final_destination_coords:
                    print(f"Central: Error: Destino final '{customer_request['destination_id']}' para cliente {client_id} no encontrado en CITY_MAP.")
                    # Potentially send a KO notification to the client here
                    continue # Cannot send GOTO_DEST if destination is unknown

                # Update Central's internal state for the taxi
                taxi["status"] = "moving_to_destination"
                taxi["service_id"] = client_id
                taxi["current_destination_coords"] = final_destination_coords # <--- This is key

                # Send GOTO_DEST command to the taxi
                print(f"[Central DEBUG] Enviando GOTO_DEST a taxi {current_taxi_id} con destino {final_destination_coords}") # DEBUG: Confirm command sending attempt
                taxi_command_msg = MessageProtocol.create_taxi_command(
                    taxi_id=current_taxi_id, # Use the potentially updated ID for the command
                    command="GOTO_DEST",
                    new_destination_coords=final_destination_coords, # <--- The actual coordinates
                    client_id=client_id,
                    final_destination_id=customer_request["destination_id"]
                )
                send_central_update('taxi_commands', MessageProtocol.parse_message(taxi_command_msg))

                # Remove the client request from CUSTOMER_REQUESTS after the pickup is confirmed and command sent
                del CUSTOMER_REQUESTS[client_id]
                print(f"Central: Cliente {client_id} eliminado de CUSTOMER_REQUESTS tras pickup y comando de destino.")


                # Emit updated map state to reflect taxi's new status and removed client request
                map_state_message = MessageProtocol.create_map_update(
                    city_map=copy.deepcopy(CITY_MAP),
                    taxi_fleet=copy.deepcopy(TAXI_FLEET),
                    customer_requests=copy.deepcopy(CUSTOMER_REQUESTS)
                )
                send_central_update('map_updates', MessageProtocol.parse_message(map_state_message))
            else:
                print(f"Central: Solicitud de cliente {client_id} ya no existe o no es válida al recibir pickup para taxi {taxi_id_from_msg}.")

def process_taxi_dropoff_messages():
    """Procesa los mensajes del topic 'taxi_dropoffs' para detectar llegada al destino y enviar el comando de volver a base."""
    global TAXI_FLEET, CUSTOMER_REQUESTS # CUSTOMER_REQUESTS should be empty for this client by now
    for message in taxi_dropoff_consumer:
        msg_value = message.value
        if msg_value.get("operation_code") == MessageProtocol.OP_TAXI_DROPOFF:
            taxi_id = msg_value["data"]["taxi_id"]
            client_id = msg_value["data"]["client_id"]
            destination_id = msg_value["data"]["destination_id"]
            dropoff_coords = msg_value["data"]["dropoff_coords"]

            # Ensure we have the correct taxi ID (potentially '1a')
            if taxi_id not in TAXI_FLEET and taxi_id[:-1] in TAXI_FLEET:
                 base_taxi_id = taxi_id[:-1]
                 TAXI_FLEET[taxi_id] = TAXI_FLEET.pop(base_taxi_id)
                 current_taxi_id = taxi_id
                 print(f"Central: Taxi ID updated from {base_taxi_id} to {current_taxi_id} upon dropoff confirmation.")
            else:
                 current_taxi_id = taxi_id

            if current_taxi_id in TAXI_FLEET:
                taxi = TAXI_FLEET[current_taxi_id]
                print(f"Central: Taxi {current_taxi_id} ha dejado al cliente {client_id} en destino {destination_id} en {dropoff_coords}. Enviando notificación de servicio completado y comando para volver a base...")

                # Send service completed notification (to the client/EC_CTS)
                service_completed_msg = MessageProtocol.create_service_completed(
                    client_id=client_id, taxi_id=current_taxi_id, destination_id=destination_id
                )
                send_central_update('service_notifications', MessageProtocol.parse_message(service_completed_msg))

                # Update Central's internal state for the taxi
                taxi["status"] = "returning_to_base"
                taxi["service_id"] = None # No longer assigned to a service
                # Get the initial coordinates for the taxi
                initial_coords = {"x": taxi["initial_x"], "y": taxi["initial_y"]}
                taxi["current_destination_coords"] = initial_coords
                taxi["status_before_free"] = "returning_to_base" # Flag for when it returns to base

                # Send RETURN_TO_BASE command to the taxi
                taxi_command_msg = MessageProtocol.create_taxi_command(
                    taxi_id=current_taxi_id,
                    command="RETURN_TO_BASE",
                    new_destination_coords=initial_coords,
                    client_id=None,
                    final_destination_id=None
                )
                send_central_update('taxi_commands', MessageProtocol.parse_message(taxi_command_msg))

                # Emit updated map state
                map_state_message = MessageProtocol.create_map_update(
                    city_map=copy.deepcopy(CITY_MAP),
                    taxi_fleet=copy.deepcopy(TAXI_FLEET),
                    customer_requests=copy.deepcopy(CUSTOMER_REQUESTS)
                )
                send_central_update('map_updates', MessageProtocol.parse_message(map_state_message))

                # ---
                # NOTE: The customer is already removed from CUSTOMER_REQUESTS after pickup.
                # If you want to show the customer at the destination, add them to a new structure here.
                # By default, the customer disappears from the map after dropoff (standard behavior).
                # ---
            else:
                print(f"Central: Ignorando dropoff. Taxi {current_taxi_id} no encontrado.")

# --- Función para enviar el estado completo del mapa ---
def send_map_updates_periodically():
    """Envía el estado completo del mapa a Kafka periódicamente."""
    while True:
        map_state_message = MessageProtocol.create_map_update(
            city_map=copy.deepcopy(CITY_MAP),
            taxi_fleet=copy.deepcopy(TAXI_FLEET),
            customer_requests=copy.deepcopy(CUSTOMER_REQUESTS)
        )
        send_central_update('map_updates', MessageProtocol.parse_message(map_state_message))
        # draw_map_console(CITY_MAP, TAXI_FLEET, CUSTOMER_REQUESTS)
        time.sleep(MAP_UPDATE_INTERVAL)


# --- Funciones de Sockets (Autenticación de Taxis) ---
ACTIVE_TOKENS = {}   # {taxi_id: token}

def generate_token():
    return secrets.token_hex(16)

def handle_taxi_auth_client(conn, addr):
    """Maneja una conexión de socket entrante para autenticación de taxi."""
    print(f"Conexión de autenticación de taxi desde {addr}")
    try:
        data = conn.recv(1024).decode('utf-8')
        if data:
            message = MessageProtocol.parse_message(data)
            if message.get("operation_code") == MessageProtocol.OP_AUTH_REQUEST:
                taxi_id = message["data"].get("taxi_id")
                if taxi_id: # Check if taxi_id is present
                    if taxi_id not in TAXI_FLEET:
                        # --- START OF MODIFICATION ---
                        # Registrar taxi nuevo en la central si no existe
                        TAXI_FLEET[taxi_id] = {
                            "x": 0,
                            "y": 0,
                            "status": "free",
                            "service_id": None,
                            "current_destination_coords": None,
                            "initial_x": 0,
                            "initial_y": 0
                        }
                        print(f"Central: Taxi {taxi_id} registrado dinámicamente en TAXI_FLEET.")
                        # --- END OF MODIFICATION ---

                    # Now that we know the taxi_id is in TAXI_FLEET (either pre-existing or just added)
                    # Generar token y guardarlo temporalmente
                    token = generate_token()
                    ACTIVE_TOKENS[taxi_id] = token
                    response_msg = MessageProtocol.create_auth_response(
                        taxi_id, MessageProtocol.STATUS_OK, token
                    )
                    log_audit_event("Taxi", addr[0], "AuthSuccess", f"Taxi {taxi_id} autenticado. Token: {token}")
                    if TAXI_FLEET[taxi_id]["status"] != "disabled":
                        TAXI_FLEET[taxi_id]["status"] = "free"
                    print(f"Taxi {taxi_id} autenticado y listo.")
                else: # This handles cases where taxi_id is None or empty
                    response_msg = MessageProtocol.create_auth_response(
                        None, MessageProtocol.STATUS_KO, "ID de taxi no válido."
                    )
                    log_audit_event("Taxi", addr[0], "AuthFail", "Intento de autenticación fallido: ID de taxi no proporcionado")
                    print(f"Fallo de autenticación: ID de taxi no válido.")
                conn.sendall(response_msg.encode('utf-8'))
            else:
                print(f"Mensaje de autenticación inesperado: {message}")
                conn.sendall(MessageProtocol.create_auth_response(None, MessageProtocol.STATUS_KO, "Mensaje inválido").encode('utf-8'))
    except Exception as e:
        print(f"Error al manejar la autenticación del taxi: {e}")
        log_audit_event("Taxi", addr[0], "AuthError", str(e))
    finally:
        conn.close()
        print(f"Conexión de autenticación de {addr} cerrada.")


def start_auth_server():
    """Inicia el servidor de sockets para la autenticación de taxis."""
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1) # Reutilizar la dirección
    server_socket.bind((CENTRAL_HOST, CENTRAL_PORT_AUTH))
    server_socket.listen()
    print(f"Servidor de autenticación de taxis escuchando en {CENTRAL_HOST}:{CENTRAL_PORT_AUTH}")

    while True:
        try:
            conn, addr = server_socket.accept()
            thread = threading.Thread(target=handle_taxi_auth_client, args=(conn, addr))
            thread.start()
        except KeyboardInterrupt:
            print("\nCerrando servidor de autenticación...")
            break
        except Exception as e:
            print(f"Error en el servidor de autenticación: {e}")
    server_socket.close()

# --- Espera datos de EC_DE send_new_taxi_data_to_central y actulizar taxis_available.txt espera de 5 segundos para recibir los datos ---
def write_taxi_available_file():
    """Escribe el estado actual de los taxis disponibles en taxis_available.txt cada 5 segundos."""
    global TAXI_FLEET
    while True:
        try:
            print("Estado actual de los taxis disponibles:")
            for taxi_id, taxi_info in TAXI_FLEET.items():
                print(f"Taxi {taxi_id}: ({taxi_info['x']}, {taxi_info['y']}) - {taxi_info['status']}")
            with open('Central/taxis_available.txt', 'w') as f:
                for taxi_id, taxi_info in TAXI_FLEET.items():
                    f.write(f"{taxi_id} {taxi_info['x']} {taxi_info['y']} {taxi_info['status']}\n")
            print("Archivo taxis_available.txt actualizado.")
            time.sleep(5)
        except Exception as e:
            print(f"Error al escribir taxis_available.txt: {e}")
            time.sleep(5)



# --- Función Principal ---
def main():
    print("Iniciando EasyCab Central...")
    load_city_map()
    # Hilo para escribir taxis_available.txt usando el estado global TAXI_FLEET
    threading.Thread(target=write_taxi_available_file, daemon=True).start()
    load_taxi_fleet()
    init_kafka_clients()   # Inicializa los clientes Kafka después de cargar datos

    # Iniciar hilos para los consumidores de Kafka
    threading.Thread(target=process_taxi_movement_messages, daemon=True).start()
    threading.Thread(target=process_sensor_data_messages, daemon=True).start()
    threading.Thread(target=process_customer_request_messages, daemon=True).start()
    threading.Thread(target=process_service_completed_messages, daemon=True).start()
    threading.Thread(target=process_taxi_pickup_messages, daemon=True).start()
    threading.Thread(target=process_taxi_dropoff_messages, daemon=True).start()
    # Hilo para enviar actualizaciones del mapa
    threading.Thread(target=send_map_updates_periodically, daemon=True).start()
    # Hilo para consultar el estado del tráfico
    threading.Thread(target=check_traffic_status_periodically, daemon=True).start()
    # Iniciar el API REST de auditoría en un hilo aparte
    threading.Thread(target=run_audit_api, daemon=True).start()
    start_auth_server()


if __name__ == "__main__":
    main()