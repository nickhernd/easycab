import socket
import threading
import json
import time
import sys
import os
import copy # Importar copy para la función de deepcopy
from kafka import KafkaProducer, KafkaConsumer

# Obtiene la ruta del directorio del script actual (Central/)
script_dir = os.path.dirname(__file__)
# Obtiene la ruta del directorio EasyCab_Python/
project_root = os.path.abspath(os.path.join(script_dir, '..'))
# Añade el directorio raíz del proyecto a sys.path
sys.path.insert(0, project_root)

from common.message_protocol import MessageProtocol 

# --- Configuración General ---
CENTRAL_HOST = '0.0.0.0' # Escucha en todas las interfaces disponibles
CENTRAL_PORT_AUTH = 65432 # Puerto para autenticación de taxis (via sockets)
KAFKA_BROKER = 'localhost:9094' # Dirección del broker de Kafka
MAP_UPDATE_INTERVAL = 2 # Segundos entre cada actualización del mapa

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

# --- Kafka Producers ---
central_producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# --- Kafka Consumers ---
taxi_position_consumer = KafkaConsumer(
    'taxi_movements',
    bootstrap_servers=[KAFKA_BROKER],
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    group_id='central_group'
)

sensor_data_consumer = KafkaConsumer(
    'sensor_data',
    bootstrap_servers=[KAFKA_BROKER],
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    group_id='central_group'
)

customer_requests_consumer = KafkaConsumer(
    'customer_requests',
    bootstrap_servers=[KAFKA_BROKER],
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    group_id='central_group'
)

central_service_notification_consumer = KafkaConsumer(
    'service_notifications',
    bootstrap_servers=[KAFKA_BROKER],
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    group_id='central_notification_processor_group'
)

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
                        "initial_x": x, "initial_y": y  # Guarda la posición inicial
                    }
        print(f"Flota de taxis cargada: {TAXI_FLEET}")
    except FileNotFoundError:
        print("Error: taxis_available.txt no encontrado.")
    except Exception as e:
        print(f"Error al cargar la flota de taxis: {e}")

# --- Funciones de Kafka ---
def send_central_update(topic, message):
    """Envía un mensaje JSON a un tema de Kafka usando el producer central."""
    try:
        central_producer.send(topic, message)
        # print(f"Enviado a {topic}: {message}") # Descomentar para depuración intensiva
        central_producer.flush()
    except Exception as e:
        print(f"Error al enviar mensaje a Kafka ({topic}): {e}")

def process_taxi_movement_messages():
    """Procesa los mensajes del tema 'taxi_movements'."""
    for message in taxi_position_consumer:
        msg_value = message.value
        if msg_value.get("operation_code") == MessageProtocol.OP_TAXI_POSITION:
            taxi_id = msg_value["data"]["taxi_id"]
            x = msg_value["data"]["x"]
            y = msg_value["data"]["y"]
            status = msg_value["data"]["status"]

            if taxi_id in TAXI_FLEET:
                taxi = TAXI_FLEET[taxi_id]
                taxi["x"] = x
                taxi["y"] = y
                taxi["status"] = status

                # Si está yendo a recoger al cliente y ha llegado
                if status == "moving_to_customer" and taxi["service_id"]:
                    client_id = taxi["service_id"]
                    client_coords = CUSTOMER_REQUESTS[client_id]["origin_coords"]
                    if (x, y) == (client_coords["x"], client_coords["y"]):
                        destination_id = CUSTOMER_REQUESTS[client_id]["destination_id"]
                        final_destination_coords = CITY_MAP[destination_id]
                        print(f"Taxi {taxi_id} ha recogido al cliente {client_id} en ({x},{y}). Enviando comando para ir al destino final {destination_id} ({final_destination_coords['x']},{final_destination_coords['y']})")
                        # Cambia el estado del taxi
                        taxi["status"] = "moving_to_destination"
                        taxi["current_destination_coords"] = final_destination_coords
                        # Cambia el estado del cliente (opcional, para trazabilidad)
                        CUSTOMER_REQUESTS[client_id]["status"] = "picked_up"
                        # Manda comando para ir al destino
                        taxi_command_msg = MessageProtocol.create_taxi_command(
                            taxi_id=taxi_id, command="GOTO_DEST", new_destination_coords=final_destination_coords, client_id=client_id, final_destination_id=destination_id
                        )
                        send_central_update('taxi_commands', MessageProtocol.parse_message(taxi_command_msg))

                # Si está yendo al destino y ha llegado
                elif status == "moving_to_destination" and taxi["current_destination_coords"] and \
                     (x, y) == (taxi["current_destination_coords"]["x"], taxi["current_destination_coords"]["y"]):
                    client_id = taxi["service_id"]
                    # Notifica servicio completado
                    service_completed_msg = MessageProtocol.create_service_completed(
                        client_id=client_id, taxi_id=taxi_id, destination_id=CUSTOMER_REQUESTS[client_id]["destination_id"]
                    )
                    send_central_update('service_notifications', MessageProtocol.parse_message(service_completed_msg))

                    # Cambia el estado del taxi y manda a base
                    taxi["status"] = "returning_to_base"
                    taxi["current_destination_coords"] = CITY_MAP["A"] # Asume que la base es "A"
                    taxi["service_id"] = None

                    taxi_command_msg = MessageProtocol.create_taxi_command(
                        taxi_id=taxi_id, command="RETURN_TO_BASE", new_destination_coords=CITY_MAP["A"]
                    )
                    send_central_update('taxi_commands', MessageProtocol.parse_message(taxi_command_msg))

                # Si está volviendo a base y ha llegado
                elif status == "returning_to_base" and taxi["current_destination_coords"] and \
                     (x, y) == (taxi["current_destination_coords"]["x"], taxi["current_destination_coords"]["y"]):
                    taxi["status"] = "free"
                    taxi["current_destination_coords"] = None
            else:
                print(f"Advertencia: Movimiento de taxi no registrado: {taxi_id}")

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
        grid[y][x] = f"\033[44m {loc_id} \033[0m"  # Fondo azul

    # Clientes (minúsculas, fondo amarillo)
    for client_id, req_data in customer_requests.items():
        if "origin_coords" in req_data:
            x, y = wrap_position(req_data["origin_coords"]["x"], req_data["origin_coords"]["y"])
            grid[y][x] = f"\033[43m {client_id[0].lower()} \033[0m"  # Fondo amarillo

    # Taxis (número, verde si moviéndose, rojo si parado)
    for taxi_id, taxi_data in taxi_fleet.items():
        x, y = wrap_position(taxi_data["x"], taxi_data["y"])
        if taxi_data["status"] in ["moving_to_customer", "moving_to_destination"]:
            color = "\033[42m"  # Fondo verde
        else:
            color = "\033[41m"  # Fondo rojo
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
            
            client_origin_id =  'a'  # Asignar un origen por defecto
            
            if client_origin_id not in CITY_MAP:
                print(f"Error: Origen del cliente '{client_origin_id}' no encontrado en el mapa.")
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
                    "origin_coords": CITY_MAP[client_origin_id]
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
            
            print(f"Servicio completado para cliente {client_id} por Taxi {taxi_id} en destino {destination_id}")
            
            # Elimina al cliente del estado
            if client_id in CUSTOMER_REQUESTS:
                del CUSTOMER_REQUESTS[client_id]
            # El taxi debe volver a su posición inicial
            # Guarda la posición inicial al cargar la flota
            if "initial_x" in TAXI_FLEET[taxi_id] and "initial_y" in TAXI_FLEET[taxi_id]:
                initial_coords = {"x": TAXI_FLEET[taxi_id]["initial_x"], "y": TAXI_FLEET[taxi_id]["initial_y"]}
            else:
                initial_coords = {"x": 0, "y": 0}
            TAXI_FLEET[taxi_id]["status"] = "returning_to_base"
            TAXI_FLEET[taxi_id]["current_destination_coords"] = initial_coords

            taxi_command_msg = MessageProtocol.create_taxi_command(
                taxi_id=taxi_id, command="RETURN_TO_BASE", new_destination_coords=initial_coords
            )
            send_central_update('taxi_commands', MessageProtocol.parse_message(taxi_command_msg))

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
        # draw_map_console(CITY_MAP, TAXI_FLEET, CUSTOMER_REQUESTS)  # <-- Elimina o comenta esta línea
        time.sleep(MAP_UPDATE_INTERVAL)

# --- Funciones de Sockets (Autenticación de Taxis) ---
def handle_taxi_auth_client(conn, addr):
    """Maneja una conexión de socket entrante para autenticación de taxi."""
    print(f"Conexión de autenticación de taxi desde {addr}")
    try:
        data = conn.recv(1024).decode('utf-8')
        if data:
            message = MessageProtocol.parse_message(data)
            if message.get("operation_code") == MessageProtocol.OP_AUTH_REQUEST:
                taxi_id = message["data"].get("taxi_id")
                if taxi_id and taxi_id in TAXI_FLEET:
                    response_msg = MessageProtocol.create_auth_response(
                        taxi_id, MessageProtocol.STATUS_OK, "Autenticado correctamente."
                    )
                    if TAXI_FLEET[taxi_id]["status"] != "disabled":
                        TAXI_FLEET[taxi_id]["status"] = "free" 
                    print(f"Taxi {taxi_id} autenticado y listo.")
                else:
                    response_msg = MessageProtocol.create_auth_response(
                        taxi_id, MessageProtocol.STATUS_KO, "ID de taxi no válido o no registrado."
                    )
                    print(f"Fallo de autenticación para ID {taxi_id}: No válido.")
                conn.sendall(response_msg.encode('utf-8'))
            else:
                print(f"Mensaje de autenticación inesperado: {message}")
                conn.sendall(MessageProtocol.create_auth_response(None, MessageProtocol.STATUS_KO, "Mensaje inválido").encode('utf-8'))
    except Exception as e:
        print(f"Error al manejar la autenticación del taxi: {e}")
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

# --- Función Principal ---
def main():
    print("Iniciando EasyCab Central...")
    load_city_map()
    load_taxi_fleet()

    # Iniciar hilos para los consumidores de Kafka
    threading.Thread(target=process_taxi_movement_messages, daemon=True).start()
    threading.Thread(target=process_sensor_data_messages, daemon=True).start()
    threading.Thread(target=process_customer_request_messages, daemon=True).start()
    threading.Thread(target=process_service_completed_messages, daemon=True).start()

    # NUEVO: Hilo para enviar actualizaciones del mapa
    threading.Thread(target=send_map_updates_periodically, daemon=True).start()

    start_auth_server()


if __name__ == "__main__":
    main()