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
import sqlite3
import random

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

# Ruta al archivo de la base de datos SQLite
DATABASE_FILE = os.path.join(script_dir, 'easycab.db') # El archivo .db estará en la carpeta Central

def wrap_position(x, y):
    """Devuelve las coordenadas ajustadas para la geometría esférica."""
    return x % MAP_SIZE, y % MAP_SIZE

# --- Estructuras de datos globales (ahora se sincronizarán con la base de datos) ---
# { "ID_LOCALIZACION": {"x": int, "y": int} }
CITY_MAP = {}
# { "ID_TAXI": {"x": int, "y": int, "status": str, "service_id": str, "current_destination_coords": (x, y), "initial_x": int, "initial_y": int} }
# Status: "free", "occupied", "moving_to_customer", "moving_to_destination", "disabled", "returning_to_base"
TAXI_FLEET = {}
# { "CLIENT_ID": {"destination_id": str, "assigned_taxi_id": str or None, "status": str, "origin_coords": {"x": int, "y": int}} }
# Status: "idle", "pending", "assigned", "picked_up", "completed", "denied", "cancelled"
CUSTOMER_REQUESTS = {}

# --- Variables globales de Kafka ---
central_producer = None
taxi_position_consumer = None
sensor_data_consumer = None
customer_requests_consumer = None
central_service_notification_consumer = None
taxi_pickup_consumer = None
taxi_dropoff_consumer = None
# taxis_available_consumer = None # Este consumidor no es necesario si el registro se gestiona vía API y DB.


# --- Auditoría ---
TRAFFIC_STATUS = "UNKNOWN" # OK, KO, UNKNOWN

def get_db_connection():
    """Establece una conexión a la base de datos SQLite."""
    conn = sqlite3.connect(DATABASE_FILE)
    conn.row_factory = sqlite3.Row # Permite acceder a las columnas por nombre
    return conn

def log_audit_event(who, ip, action, description):
    """Registra un evento de auditoría estructurada en la base de datos."""
    now = datetime.now().isoformat()
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO audit_log (timestamp, who, ip, action, description) VALUES (?, ?, ?, ?, ?)",
            (now, who, ip, action, description)
        )
        conn.commit()
        conn.close()
        print(f"[AUDIT] {now} | {who} | {ip} | {action} | {description}")
    except sqlite3.Error as e:
        print(f"Error al escribir en la base de datos de auditoría: {e}")

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
                    description=f"Respuesta inesperada del API CTC: {response.status_code} - {response.text}"
                )
        except requests.exceptions.ConnectionError:
            log_audit_event(
                who="EC_Central",
                ip=CENTRAL_HOST,
                action="TrafficStatusError",
                description="Error de conexión al API CTC. Asegúrate de que EC_CTC esté corriendo."
            )
            print("Error de conexión al API CTC. Reintentando...")
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
    print("Audit API server placeholder started. (The actual audit_api.py handles the Flask server.)")
    pass # Este EC_Central solo escribe en la DB, el audit_api.py es el que expone la API Flask

# --- Funciones de Carga de Datos ---
def load_city_map():
    """Carga el mapa de la ciudad desde config_map.txt."""
    try:
        with open(os.path.join(script_dir, 'config_map.txt'), 'r') as f: # Ruta ajustada
            for line in f:
                parts = line.strip().split()
                if len(parts) == 3:
                    loc_id = parts[0]
                    x, y = int(parts[1]), int(parts[2])
                    CITY_MAP[loc_id] = {"x": x, "y": y}
        print(f"Mapa de la ciudad cargado: {CITY_MAP}")
    except FileNotFoundError:
        print("Error: config_map.txt no encontrado. Asegúrate de que esté en la carpeta Central.")
    except Exception as e:
        print(f"Error al cargar el mapa: {e}")

def load_taxi_fleet_from_db():
    """Carga la flota de taxis desde la base de datos."""
    global TAXI_FLEET
    TAXI_FLEET = {} # Limpiar la flota existente en memoria
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT taxi_id, x, y, status, service_id, current_destination_x, current_destination_y, initial_x, initial_y FROM taxis")
        taxis = cursor.fetchall()
        for taxi_row in taxis:
            taxi_id = taxi_row["taxi_id"]
            TAXI_FLEET[taxi_id] = {
                "x": taxi_row["x"],
                "y": taxi_row["y"],
                "status": taxi_row["status"],
                "service_id": taxi_row["service_id"],
                "current_destination_coords": {"x": taxi_row["current_destination_x"], "y": taxi_row["current_destination_y"]} if taxi_row["current_destination_x"] is not None else None,
                "initial_x": taxi_row["initial_x"],
                "initial_y": taxi_row["initial_y"]
            }
        conn.close()
        print(f"Flota de taxis cargada desde DB: {TAXI_FLEET}")
    except sqlite3.Error as e:
        print(f"Error al cargar la flota de taxis desde DB: {e}. Asegúrate de ejecutar database_setup.py.")
        sys.exit(1) # Salir si no se pueden cargar los taxis

def update_taxi_in_db(taxi_id, x, y, status, service_id=None, current_destination_coords=None, initial_x=None, initial_y=None):
    """Actualiza o inserta un taxi en la base de datos."""
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            """INSERT OR REPLACE INTO taxis (taxi_id, x, y, status, service_id, current_destination_x, current_destination_y, initial_x, initial_y)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (taxi_id, x, y, status, service_id,
             current_destination_coords["x"] if current_destination_coords else None,
             current_destination_coords["y"] if current_destination_coords else None,
             initial_x, initial_y)
        )
        conn.commit()
    except sqlite3.Error as e:
        print(f"Error al actualizar/insertar taxi {taxi_id} en DB: {e}")
    finally:
        conn.close()

def delete_taxi_from_db(taxi_id):
    """Elimina un taxi de la base de datos."""
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("DELETE FROM taxis WHERE taxi_id = ?", (taxi_id,))
        conn.commit()
        print(f"Taxi {taxi_id} eliminado de la base de datos.")
    except sqlite3.Error as e:
        print(f"Error al eliminar taxi {taxi_id} de DB: {e}")
    finally:
        conn.close()

def load_customer_requests_from_db():
    """Carga las solicitudes de clientes desde la base de datos."""
    global CUSTOMER_REQUESTS
    CUSTOMER_REQUESTS = {} # Limpiar las solicitudes existentes en memoria
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT client_id, destination_id, assigned_taxi_id, status, origin_x, origin_y FROM customers")
        customers = cursor.fetchall()
        for customer_row in customers:
            client_id = customer_row["client_id"]
            CUSTOMER_REQUESTS[client_id] = {
                "destination_id": customer_row["destination_id"],
                "assigned_taxi_id": customer_row["assigned_taxi_id"],
                "status": customer_row["status"],
                "origin_coords": {"x": customer_row["origin_x"], "y": customer_row["origin_y"]}
            }
        conn.close()
        print(f"Solicitudes de clientes cargadas desde DB: {CUSTOMER_REQUESTS}")
    except sqlite3.Error as e:
        print(f"Error al cargar las solicitudes de clientes desde DB: {e}. Asegúrate de ejecutar database_setup.py.")
        sys.exit(1)

def update_customer_in_db(client_id, destination_id, assigned_taxi_id, status, origin_x, origin_y):
    """Actualiza o inserta un cliente en la base de datos."""
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            """INSERT OR REPLACE INTO customers (client_id, destination_id, assigned_taxi_id, status, origin_x, origin_y)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (client_id, destination_id, assigned_taxi_id, status, origin_x, origin_y)
        )
        conn.commit()
    except sqlite3.Error as e:
        print(f"Error al actualizar/insertar cliente {client_id} en DB: {e}")
    finally:
        conn.close()

def delete_customer_from_db(client_id):
    """Elimina un cliente de la base de datos."""
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("DELETE FROM customers WHERE client_id = ?", (client_id,))
        conn.commit()
        print(f"Cliente {client_id} eliminado de la base de datos.")
    except sqlite3.Error as e:
        print(f"Error al eliminar cliente {client_id} de DB: {e}")
    finally:
        conn.close()

# --- Funciones de Kafka ---
def init_kafka_clients():
    """Inicializa los productores y consumidores de Kafka."""
    global central_producer, taxi_position_consumer, sensor_data_consumer, customer_requests_consumer, central_service_notification_consumer, taxi_pickup_consumer, taxi_dropoff_consumer
    try:
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
        log_audit_event("EC_Central", CENTRAL_HOST, "KafkaInitError", f"Fallo al inicializar clientes Kafka: {e}")
        sys.exit(1) # Salir si Kafka no puede inicializarse


def send_central_update(topic, message):
    """Envía un mensaje JSON a un tema de Kafka usando el producer central."""
    try:
        central_producer.send(topic, message)
        print(f"[Central] Enviado a {topic}: {message}")
        central_producer.flush()
    except Exception as e:
        print(f"Error al enviar mensaje a Kafka ({topic}): {e}")
        log_audit_event("EC_Central", CENTRAL_HOST, "KafkaSendError", f"Error al enviar a {topic}: {e}")

def process_taxi_movement_messages():
    """Procesa los mensajes del tema 'taxi_movements'."""
    global TAXI_FLEET, CUSTOMER_REQUESTS
    for message in taxi_position_consumer:
        msg_value = message.value
        if msg_value.get("operation_code") == MessageProtocol.OP_TAXI_POSITION:
            taxi_id = str(msg_value["data"]["taxi_id"]) # Asegurar que sea string para las claves del diccionario y DB
            x = msg_value["data"]["x"]
            y = msg_value["data"]["y"]
            status = msg_value["data"]["status"]

            # Actualizar en memoria y en la base de datos
            if taxi_id in TAXI_FLEET:
                TAXI_FLEET[taxi_id]["x"] = x
                TAXI_FLEET[taxi_id]["y"] = y
                TAXI_FLEET[taxi_id]["status"] = status
                # Asegurar que los datos de service_id y destino se mantengan o se actualicen
                update_taxi_in_db(
                    taxi_id, x, y, status,
                    TAXI_FLEET[taxi_id].get("service_id"),
                    TAXI_FLEET[taxi_id].get("current_destination_coords"),
                    TAXI_FLEET[taxi_id]["initial_x"],
                    TAXI_FLEET[taxi_id]["initial_y"]
                )
            else:
                print(f"Advertencia: Movimiento de taxi {taxi_id} no gestionado o no registrado. Se registrará automáticamente.")
                # Si el taxi no está en memoria, asumimos que es nuevo y lo añadimos a la DB y memoria
                # Esto es para manejar taxis que se registran directamente con el auth server sin pasar por el 'taxis_available'
                initial_x, initial_y = 1, 1 # Asumimos posición inicial por defecto si no está en DB
                TAXI_FLEET[taxi_id] = {
                    "x": x, "y": y, "status": status, "service_id": None,
                    "current_destination_coords": None,
                    "initial_x": initial_x, "initial_y": initial_y # Se asume una inicial si no se conoce
                }
                update_taxi_in_db(taxi_id, x, y, status, None, None, initial_x, initial_y)


            if status == "free" and TAXI_FLEET[taxi_id].get("status_before_free") == "returning_to_base":
                print(f"Taxi {taxi_id} ha llegado a su base y está libre.")
                TAXI_FLEET[taxi_id]["service_id"] = None
                TAXI_FLEET[taxi_id]["current_destination_coords"] = None
                TAXI_FLEET[taxi_id]["status_before_free"] = None # Reset flag
                update_taxi_in_db(
                    taxi_id,
                    TAXI_FLEET[taxi_id]["x"],
                    TAXI_FLEET[taxi_id]["y"],
                    TAXI_FLEET[taxi_id]["status"],
                    TAXI_FLEET[taxi_id]["service_id"],
                    TAXI_FLEET[taxi_id]["current_destination_coords"],
                    TAXI_FLEET[taxi_id]["initial_x"],
                    TAXI_FLEET[taxi_id]["initial_y"]
                )
            elif status == "free":
                TAXI_FLEET[taxi_id]["service_id"] = None
                TAXI_FLEET[taxi_id]["current_destination_coords"] = None
                update_taxi_in_db(
                    taxi_id,
                    TAXI_FLEET[taxi_id]["x"],
                    TAXI_FLEET[taxi_id]["y"],
                    TAXI_FLEET[taxi_id]["status"],
                    TAXI_FLEET[taxi_id]["service_id"],
                    TAXI_FLEET[taxi_id]["current_destination_coords"],
                    TAXI_FLEET[taxi_id]["initial_x"],
                    TAXI_FLEET[taxi_id]["initial_y"]
                )

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
            taxi_id = str(msg_value["data"]["taxi_id"])
            status = msg_value["data"]["status"] # "OK" o "KO"
            anomaly_details = msg_value["data"].get("anomaly_details", "")
            if taxi_id in TAXI_FLEET:
                if status == MessageProtocol.STATUS_KO:
                    print(f"¡ALERTA! Sensor de Taxi {taxi_id} reporta KO. Detalles: {anomaly_details}. Deshabilitando taxi.")
                    TAXI_FLEET[taxi_id]["status"] = "disabled"
                    log_audit_event("EC_Sensor", taxi_id, "SensorAnomaly", f"Taxi {taxi_id} reporta KO: {anomaly_details}. Deshabilitado.")
                elif status == MessageProtocol.STATUS_OK:
                    if TAXI_FLEET[taxi_id]["status"] == "disabled":
                        print(f"Taxi {taxi_id} se ha recuperado (sensores OK). Marcando como libre.")
                        TAXI_FLEET[taxi_id]["status"] = "free"
                        log_audit_event("EC_Sensor", taxi_id, "SensorRecovery", f"Taxi {taxi_id} recuperado. Habilitado.")
                # Actualizar el estado en la base de datos
                update_taxi_in_db(
                    taxi_id, TAXI_FLEET[taxi_id]["x"], TAXI_FLEET[taxi_id]["y"], TAXI_FLEET[taxi_id]["status"],
                    TAXI_FLEET[taxi_id].get("service_id"), TAXI_FLEET[taxi_id].get("current_destination_coords"),
                    TAXI_FLEET[taxi_id]["initial_x"], TAXI_FLEET[taxi_id]["initial_y"]
                )
            else:
                print(f"Advertencia: Actualización de sensor de taxi no registrado: {taxi_id}")
                log_audit_event("EC_Sensor", taxi_id, "SensorAnomalyWarning", f"Actualización de sensor de taxi no registrado: {taxi_id} - {anomaly_details}")


def process_customer_request_messages():
    """Procesa los mensajes del tema 'customer_requests'."""
    for message in customer_requests_consumer:
        msg_value = message.value
        if msg_value.get("operation_code") == MessageProtocol.OP_CUSTOMER_REQUEST:
            client_id = str(msg_value["data"]["client_id"])
            destination_id = msg_value["data"]["destination_id"]
            origin_x = msg_value["data"].get("origin_x") # Recibir origen del cliente
            origin_y = msg_value["data"].get("origin_y") # Recibir origen del cliente

            print(f"Nueva solicitud de cliente '{client_id}' con destino '{destination_id}' desde origen ({origin_x},{origin_y})")

            # Actualizar o insertar cliente en la base de datos
            update_customer_in_db(client_id, destination_id, None, "pending", origin_x, origin_y)
            CUSTOMER_REQUESTS[client_id] = { # También actualizar en memoria
                "destination_id": destination_id,
                "assigned_taxi_id": None,
                "status": "pending",
                "origin_coords": {"x": origin_x, "y": origin_y}
            }
            log_audit_event("EC_Customer", client_id, "CustomerRequest", f"Cliente {client_id} solicita servicio a {destination_id} desde ({origin_x},{origin_y}).")


            if TRAFFIC_STATUS == "KO":
                print(f"Tráfico en KO. Denegando solicitud de cliente {client_id}.")
                notification_msg = MessageProtocol.create_service_notification(
                    client_id=client_id, status=MessageProtocol.STATUS_KO, message="Servicio denegado. Tráfico no viable en la ciudad."
                )
                send_central_update('service_notifications', MessageProtocol.parse_message(notification_msg))
                update_customer_in_db(client_id, destination_id, None, "denied", origin_x, origin_y) # Actualizar estado en DB
                log_audit_event("EC_Central", CENTRAL_HOST, "ServiceDenied", f"Solicitud de cliente {client_id} denegada por tráfico KO.")
                continue


            if client_id not in CUSTOMER_REQUESTS or CUSTOMER_REQUESTS[client_id]["status"] in ["completed", "denied", "idle"]:
                # La solicitud ya se añadió arriba, ahora solo intentar asignar taxi
                assign_taxi_to_request(client_id, destination_id)

            else:
                print(f"Cliente {client_id} ya tiene una solicitud {CUSTOMER_REQUESTS[client_id]['status']}.")
                notification_msg = MessageProtocol.create_service_notification(
                    client_id=client_id, status=MessageProtocol.STATUS_KO, message=f"Su solicitud ya está en curso (Estado: {CUSTOMER_REQUESTS[client_id]['status']})."
                )
                send_central_update('service_notifications', MessageProtocol.parse_message(notification_msg))
                log_audit_event("EC_Central", CENTRAL_HOST, "DuplicateRequest", f"Solicitud duplicada de cliente {client_id}. Estado actual: {CUSTOMER_REQUESTS[client_id]['status']}.")


def assign_taxi_to_request(client_id, destination_id):
    """Intenta asignar un taxi libre a una solicitud de cliente."""
    assigned = False
    # Cargar la última información del cliente desde la DB para asegurar coherencia
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT origin_x, origin_y FROM customers WHERE client_id = ?", (client_id,))
    customer_row = cursor.fetchone()
    conn.close()

    if not customer_row:
        print(f"Error: Cliente {client_id} no encontrado en la base de datos al intentar asignar taxi.")
        log_audit_event("EC_Central", CENTRAL_HOST, "AssignTaxiError", f"Cliente {client_id} no encontrado en DB para asignación.")
        return

    client_origin_coords = {"x": customer_row["origin_x"], "y": customer_row["origin_y"]}
    final_destination_coords = CITY_MAP.get(destination_id)

    if not final_destination_coords:
        print(f"Error: Destino '{destination_id}' no encontrado en el mapa para el cliente {client_id}.")
        notification_msg = MessageProtocol.create_service_notification(
            client_id=client_id, status=MessageProtocol.STATUS_KO, message=f"Destino '{destination_id}' no válido. Servicio denegado."
        )
        send_central_update('service_notifications', MessageProtocol.parse_message(notification_msg))
        update_customer_in_db(client_id, destination_id, None, "denied", client_origin_coords['x'], client_origin_coords['y'])
        log_audit_event("EC_Central", CENTRAL_HOST, "ServiceDenied", f"Solicitud de cliente {client_id} denegada: Destino {destination_id} no encontrado.")
        return


    for taxi_id, taxi_data in TAXI_FLEET.items():
        if taxi_data["status"] == "free":
            TAXI_FLEET[taxi_id]["status"] = "moving_to_customer"
            TAXI_FLEET[taxi_id]["service_id"] = client_id
            TAXI_FLEET[taxi_id]["current_destination_coords"] = client_origin_coords
            
            CUSTOMER_REQUESTS[client_id]["assigned_taxi_id"] = taxi_id # Actualizar en memoria
            CUSTOMER_REQUESTS[client_id]["status"] = "assigned"       # Actualizar en memoria

            # Actualizar base de datos para taxi
            update_taxi_in_db(
                taxi_id, taxi_data["x"], taxi_data["y"], TAXI_FLEET[taxi_id]["status"],
                TAXI_FLEET[taxi_id]["service_id"], TAXI_FLEET[taxi_id]["current_destination_coords"],
                taxi_data["initial_x"], taxi_data["initial_y"]
            )
            # Actualizar base de datos para cliente
            update_customer_in_db(
                client_id, destination_id, taxi_id, CUSTOMER_REQUESTS[client_id]["status"],
                client_origin_coords['x'], client_origin_coords['y']
            )

            print(f"Asignado Taxi {taxi_id} a cliente {client_id}. Taxi va a recoger en ({client_origin_coords['x']},{client_origin_coords['y']}). Destino final: {destination_id} ({final_destination_coords['x']},{final_destination_coords['y']})")
            log_audit_event("EC_Central", CENTRAL_HOST, "ServiceAssigned", f"Taxi {taxi_id} asignado a cliente {client_id} para {destination_id}.")


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
        update_customer_in_db(client_id, destination_id, None, "denied", client_origin_coords['x'], client_origin_coords['y'])
        log_audit_event("EC_Central", CENTRAL_HOST, "ServiceDenied", f"Solicitud de cliente {client_id} denegada: No hay taxis libres.")

def process_service_completed_messages():
    """Procesa los mensajes del tema 'service_notifications' cuando el taxi finaliza un servicio."""
    for message in central_service_notification_consumer:
        msg_value = message.value
        if msg_value.get("operation_code") == MessageProtocol.OP_SERVICE_COMPLETED:
            client_id = str(msg_value["data"]["client_id"])
            taxi_id = str(msg_value["data"]["taxi_id"])
            destination_id = msg_value["data"]["destination_id"]
            print(f"Central (Service Notifications): Confirmed service completed for client {client_id} by Taxi {taxi_id} at destination {destination_id}")
            # No es necesario actualizar el CUSTOMER_REQUESTS aquí, ya que se eliminará tras el dropoff.
            # Solo logear el evento.
            log_audit_event("EC_Central", CENTRAL_HOST, "ServiceCompleted", f"Servicio completado para cliente {client_id} por Taxi {taxi_id} en {destination_id}.")
            # Se eliminará el cliente de la DB en process_taxi_dropoff_messages

def process_taxi_pickup_messages():
    """Procesa los mensajes del topic 'taxi_pickups' para detectar recogidas y enviar el comando de ir al destino."""
    global TAXI_FLEET, CUSTOMER_REQUESTS
    for message in taxi_pickup_consumer:
        print(f"[Central DEBUG] Recibido mensaje en taxi_pickups: {message.value}") # DEBUG: Confirm message receipt
        msg_value = message.value
        if msg_value.get("operation_code") == MessageProtocol.OP_TAXI_PICKUP:
            taxi_id_from_msg = str(msg_value["data"]["taxi_id"])
            client_id = str(msg_value["data"]["client_id"])
            pickup_coords = msg_value["data"]["pickup_coords"]

            # Intentar obtener el taxi de la flota
            taxi = TAXI_FLEET.get(taxi_id_from_msg)
            if not taxi:
                print(f"Central: Advertencia: Taxi ID {taxi_id_from_msg} no encontrado en TAXI_FLEET al procesar pickup.")
                log_audit_event("EC_Central", CENTRAL_HOST, "PickupError", f"Pickup de taxi no registrado {taxi_id_from_msg} para cliente {client_id}.")
                continue

            customer_request = CUSTOMER_REQUESTS.get(client_id)
            if not customer_request: # Check if request still exists
                print(f"Central: Solicitud de cliente {client_id} ya no existe o no es válida al recibir pickup para taxi {taxi_id_from_msg}.")
                log_audit_event("EC_Central", CENTRAL_HOST, "PickupError", f"Solicitud de cliente {client_id} no encontrada para pickup de taxi {taxi_id_from_msg}.")
                continue
            
            # Retrieve the final destination coordinates from CITY_MAP
            final_destination_coords = CITY_MAP.get(customer_request["destination_id"])
            if not final_destination_coords:
                print(f"Central: Error: Destino final '{customer_request['destination_id']}' para cliente {client_id} no encontrado en CITY_MAP.")
                log_audit_event("EC_Central", CENTRAL_HOST, "PickupError", f"Destino final {customer_request['destination_id']} no encontrado para cliente {client_id}.")
                # Potentially send a KO notification to the client here
                continue # Cannot send GOTO_DEST if destination is unknown

            print(f"Central: Taxi {taxi_id_from_msg} ha recogido al cliente {client_id} en ({pickup_coords['x']},{pickup_coords['y']}). Preparando comando para ir al destino...")
            log_audit_event("EC_Central", CENTRAL_HOST, "CustomerPickedUp", f"Taxi {taxi_id_from_msg} recogió cliente {client_id} en ({pickup_coords['x']},{pickup_coords['y']}).")

            # Update Central's internal state for the taxi
            taxi["status"] = "moving_to_destination"
            taxi["service_id"] = client_id
            taxi["current_destination_coords"] = final_destination_coords # <--- This is key
            
            # Update customer status in memory and DB
            customer_request["status"] = "picked_up"
            update_customer_in_db(
                client_id, customer_request["destination_id"], taxi_id_from_msg,
                customer_request["status"], customer_request["origin_coords"]["x"],
                customer_request["origin_coords"]["y"]
            )
            
            # Update taxi status in DB
            update_taxi_in_db(
                taxi_id_from_msg, taxi["x"], taxi["y"], taxi["status"],
                taxi["service_id"], taxi["current_destination_coords"],
                taxi["initial_x"], taxi["initial_y"]
            )


            # Send GOTO_DEST command to the taxi
            print(f"[Central DEBUG] Enviando GOTO_DEST a taxi {taxi_id_from_msg} con destino {final_destination_coords}") # DEBUG: Confirm command sending attempt
            taxi_command_msg = MessageProtocol.create_taxi_command(
                taxi_id=taxi_id_from_msg, # Use the potentially updated ID for the command
                command="GOTO_DEST",
                new_destination_coords=final_destination_coords, # <--- The actual coordinates
                client_id=client_id,
                final_destination_id=customer_request["destination_id"]
            )
            send_central_update('taxi_commands', MessageProtocol.parse_message(taxi_command_msg))

            # Emit updated map state to reflect taxi's new status and updated client request
            map_state_message = MessageProtocol.create_map_update(
                city_map=copy.deepcopy(CITY_MAP),
                taxi_fleet=copy.deepcopy(TAXI_FLEET),
                customer_requests=copy.deepcopy(CUSTOMER_REQUESTS)
            )
            send_central_update('map_updates', MessageProtocol.parse_message(map_state_message))
            

def process_taxi_dropoff_messages():
    """Procesa los mensajes del topic 'taxi_dropoffs' para detectar llegada al destino y enviar el comando de volver a base."""
    global TAXI_FLEET, CUSTOMER_REQUESTS
    for message in taxi_dropoff_consumer:
        msg_value = message.value
        if msg_value.get("operation_code") == MessageProtocol.OP_TAXI_DROPOFF:
            taxi_id = str(msg_value["data"]["taxi_id"])
            client_id = str(msg_value["data"]["client_id"])
            destination_id = msg_value["data"]["destination_id"]
            dropoff_coords = msg_value["data"]["dropoff_coords"]

            taxi = TAXI_FLEET.get(taxi_id)
            if not taxi:
                print(f"Central: Ignorando dropoff. Taxi {taxi_id} no encontrado.")
                log_audit_event("EC_Central", CENTRAL_HOST, "DropoffError", f"Dropoff de taxi no registrado: {taxi_id}.")
                continue

            print(f"Central: Taxi {taxi_id} ha dejado al cliente {client_id} en destino {destination_id} en {dropoff_coords}. Enviando notificación de servicio completado y comando para volver a base...")
            log_audit_event("EC_Central", CENTRAL_HOST, "CustomerDroppedOff", f"Taxi {taxi_id} dejó a cliente {client_id} en {destination_id}.")

            # Send service completed notification (to the client/EC_Customer)
            service_completed_msg = MessageProtocol.create_service_completed(
                client_id=client_id, taxi_id=taxi_id, destination_id=destination_id
            )
            send_central_update('service_notifications', MessageProtocol.parse_message(service_completed_msg))

            # Update Central's internal state for the taxi
            taxi["status"] = "returning_to_base"
            taxi["service_id"] = None # No longer assigned to a service
            # Get the initial coordinates for the taxi
            initial_coords = {"x": taxi["initial_x"], "y": taxi["initial_y"]}
            taxi["current_destination_coords"] = initial_coords
            taxi["status_before_free"] = "returning_to_base" # Flag for when it returns to base

            # Update taxi status in DB
            update_taxi_in_db(
                taxi_id, taxi["x"], taxi["y"], taxi["status"],
                taxi["service_id"], taxi["current_destination_coords"],
                taxi["initial_x"], taxi["initial_y"]
            )

            # Update customer status in DB and remove from in-memory CUSTOMER_REQUESTS
            # The client's request is now completed. We mark it as such in the DB
            # and remove it from the active CUSTOMER_REQUESTS in memory.
            # If a new request comes from this client, it will be added again.
            update_customer_in_db(client_id, destination_id, taxi_id, "completed", dropoff_coords['x'], dropoff_coords['y'])
            if client_id in CUSTOMER_REQUESTS:
                del CUSTOMER_REQUESTS[client_id]


            # Send RETURN_TO_BASE command to the taxi
            taxi_command_msg = MessageProtocol.create_taxi_command(
                taxi_id=taxi_id,
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
        time.sleep(MAP_UPDATE_INTERVAL)


# --- Funciones de Sockets (Autenticación de Taxis) ---
ACTIVE_TOKENS = {}   # {taxi_id: token}

def generate_token():
    return secrets.token_hex(16)

def handle_taxi_auth_client(conn, addr):
    """Maneja una conexión de socket entrante para autenticación de taxi."""
    print(f"Conexión de autenticación de taxi desde {addr}")
    client_ip = addr[0]
    try:
        data = conn.recv(1024).decode('utf-8')
        if data:
            message = MessageProtocol.parse_message(data)
            if message.get("operation_code") == MessageProtocol.OP_AUTH_REQUEST:
                taxi_id = str(message["data"].get("taxi_id"))
                if taxi_id:
                    # Comprobar si el taxi está registrado en la tabla 'taxis'
                    conn_db = get_db_connection()
                    cursor = conn_db.cursor()
                    cursor.execute("SELECT taxi_id, x, y, status FROM taxis WHERE taxi_id = ?", (taxi_id,))
                    taxi_data_db = cursor.fetchone()
                    conn_db.close()

                    if taxi_data_db:
                        # Taxi ya existe en la DB
                        print(f"Central: Taxi {taxi_id} encontrado en la DB. Autenticando...")
                        TAXI_FLEET[taxi_id] = { # Asegurar que esté en memoria
                            "x": taxi_data_db["x"],
                            "y": taxi_data_db["y"],
                            "status": taxi_data_db["status"],
                            "service_id": None, # Resetear estado de servicio al autenticarse
                            "current_destination_coords": None,
                            "initial_x": taxi_data_db["x"], # Mantener la posición actual como inicial
                            "initial_y": taxi_data_db["y"]
                        }
                        # Generar token y guardarlo temporalmente
                        token = generate_token()
                        ACTIVE_TOKENS[taxi_id] = token
                        response_msg = MessageProtocol.create_auth_response(
                            taxi_id, MessageProtocol.STATUS_OK, token
                        )
                        log_audit_event("Taxi", client_ip, "AuthSuccess", f"Taxi {taxi_id} autenticado. Token: {token}")
                        # El estado del taxi en la DB ya debería ser 'free' o similar al autenticarse
                        # No es necesario forzar a "free" aquí, a menos que se quiera resetear el estado
                        if TAXI_FLEET[taxi_id]["status"] == "disabled": # Si estaba deshabilitado, se pone en libre al reautenticarse
                            TAXI_FLEET[taxi_id]["status"] = "free"
                            update_taxi_in_db(taxi_id, TAXI_FLEET[taxi_id]["x"], TAXI_FLEET[taxi_id]["y"], "free", None, None, TAXI_FLEET[taxi_id]["initial_x"], TAXI_FLEET[taxi_id]["initial_y"])

                        print(f"Taxi {taxi_id} autenticado y listo.")
                    else:
                        # Taxi NO REGISTRADO en la base de datos (EC_Registry no lo ha dado de alta)
                        response_msg = MessageProtocol.create_auth_response(
                            taxi_id, MessageProtocol.STATUS_KO, "Taxi no registrado en el sistema. Regístrese primero."
                        )
                        log_audit_event("Taxi", client_ip, "AuthFail", f"Taxi {taxi_id} intento de autenticación fallido: No registrado.")
                        print(f"Fallo de autenticación para Taxi {taxi_id}: No registrado.")
                else: # This handles cases where taxi_id is None or empty
                    response_msg = MessageProtocol.create_auth_response(
                        None, MessageProtocol.STATUS_KO, "ID de taxi no válido."
                    )
                    log_audit_event("Taxi", client_ip, "AuthFail", "Intento de autenticación fallido: ID de taxi no proporcionado")
                    print(f"Fallo de autenticación: ID de taxi no válido.")
                conn.sendall(response_msg.encode('utf-8'))
            else:
                print(f"Mensaje de autenticación inesperado: {message}")
                log_audit_event("Taxi", client_ip, "AuthError", f"Mensaje inesperado durante autenticación: {message}")
                conn.sendall(MessageProtocol.create_auth_response(None, MessageProtocol.STATUS_KO, "Mensaje inválido").encode('utf-8'))
    except Exception as e:
        print(f"Error al manejar la autenticación del taxi: {e}")
        log_audit_event("Taxi", client_ip, "AuthError", str(e))
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
    log_audit_event("EC_Central", CENTRAL_HOST, "AuthServerStart", f"Servidor de autenticación iniciado en {CENTRAL_HOST}:{CENTRAL_PORT_AUTH}.")

    while True:
        try:
            conn, addr = server_socket.accept()
            thread = threading.Thread(target=handle_taxi_auth_client, args=(conn, addr))
            thread.start()
        except KeyboardInterrupt:
            print("\nCerrando servidor de autenticación...")
            log_audit_event("EC_Central", CENTRAL_HOST, "AuthServerShutdown", "Servidor de autenticación cerrado por KeyboardInterrupt.")
            break
        except Exception as e:
            print(f"Error en el servidor de autenticación: {e}")
            log_audit_event("EC_Central", CENTRAL_HOST, "AuthServerError", f"Error crítico en servidor de autenticación: {e}")
    server_socket.close()

# --- Este hilo ya no es necesario si la gestión de taxis se hace con DB y API Registry ---
# def write_taxi_available_file():
#     """Escribe el estado actual de los taxis disponibles en taxis_available.txt cada 5 segundos."""
#     global TAXI_FLEET
#     while True:
#         try:
#             # La lógica de escritura de taxis_available.txt se ha movido a la DB
#             # Este hilo podría eliminarse o modificarse si todavía hay necesidad de un archivo de texto.
#             # Por ahora, simplemente imprime el estado.
#             print("Estado actual de los taxis disponibles (desde memoria, sincronizado con DB):")
#             for taxi_id, taxi_info in TAXI_FLEET.items():
#                 print(f"Taxi {taxi_id}: ({taxi_info['x']}, {taxi_info['y']}) - {taxi_info['status']}")
#             time.sleep(5)
#         except Exception as e:
#             print(f"Error en write_taxi_available_file (DEBUG): {e}")
#             time.sleep(5)


# --- Función Principal ---
def main():
    print("Iniciando EasyCab Central...")
    # Asegurarse de que la base de datos esté lista
    # (Se espera que database_setup.py se ejecute previamente)
    
    load_city_map()
    load_taxi_fleet_from_db() # Cargar taxis desde la DB
    load_customer_requests_from_db() # Cargar clientes desde la DB
    
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
    # Hilo para el API REST de auditoría (solo placeholder aquí)
    threading.Thread(target=run_audit_api, daemon=True).start()
    
    # Iniciar el servidor de autenticación de sockets (bloqueante)
    start_auth_server()


if __name__ == "__main__":
    main()