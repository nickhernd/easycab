import socket
import time
import random
import json
import sys
import threading
import os
from kafka import KafkaProducer, KafkaConsumer
from collections import deque
import requests
import argparse

# Argumentos de línea de comandos para configuración
argparser = argparse.ArgumentParser(description='Taxi Digital Engine (EC_DE) para EasyCab.')
argparser.add_argument('--ip_port_ecc', type=str, default='localhost:9096')
argparser.add_argument('--kafka_broker', type=str, default='localhost:9094')
argparser.add_argument('--ip_port_ecs', type=str, default='localhost:9096')
argparser.add_argument('--taxi_id', type=int, default=1, help='ID del taxi (por defecto 1)')
args = argparser.parse_args()

# Añade el directorio raíz del proyecto a sys.path para importar módulos comunes
script_dir = os.path.dirname(__file__)
project_root = os.path.abspath(os.path.join(script_dir, '..'))
sys.path.insert(0, project_root)

from common.message_protocol import MessageProtocol

# --- Configuración del Taxi ---
TAXI_ID = args.taxi_id
CENTRAL_HOST = args.ip_port_ecc
CENTRAL_PORT_AUTH = 65432
KAFKA_BROKER = args.kafka_broker
IP_PORT_ECS = args.ip_port_ecs

# --- Estado del Taxi ---
current_x = 0
current_y = 0
status = "disconnected"
assigned_service_id = None
target_x = None
target_y = None
final_destination_id = None

# --- Estados para visualización del mapa ---
current_city_map = {}
current_taxi_fleet_state = {}
current_customer_requests_state = {}

# --- Kafka Producer y Consumers ---
taxi_producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

taxi_command_consumer = KafkaConsumer(
    'taxi_commands',
    bootstrap_servers=[KAFKA_BROKER],
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    group_id=f'taxi_command_group_{TAXI_ID}'
)

map_update_consumer = KafkaConsumer(
    'map_updates',
    bootstrap_servers=[KAFKA_BROKER],
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    group_id=f'map_viewer_group_taxi_{TAXI_ID}'
)

taxi_token = None

def send_kafka_message(topic, message):
    """Envía un mensaje JSON a un tema de Kafka."""
    try:
        taxi_producer.send(topic, message)
        taxi_producer.flush()
    except Exception as e:
        print(f"Taxi {TAXI_ID}: Error enviando mensaje a Kafka ({topic}): {e}")

def authenticate_with_central():
    """Autenticación con la Central, obtiene token y cambia estado a 'free' si es exitoso."""
    global status, taxi_token
    print(f"Taxi {TAXI_ID}: Intentando conectar con la Central para autenticación en {CENTRAL_HOST}:{CENTRAL_PORT_AUTH}...")
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((CENTRAL_HOST, CENTRAL_PORT_AUTH))
            auth_request_msg = MessageProtocol.create_auth_request(TAXI_ID)
            s.sendall(auth_request_msg.encode('utf-8'))
            response_data = s.recv(1024).decode('utf-8')
            response_msg = MessageProtocol.parse_message(response_data)

            if response_msg.get("operation_code") == MessageProtocol.OP_AUTH_RESPONSE:
                if response_msg["data"].get("status") == MessageProtocol.STATUS_OK:
                    taxi_token = response_msg["data"].get("message")
                    status = "free"
                    print(f"Taxi {TAXI_ID}: Autenticación exitosa. Token recibido: {taxi_token}")
                    send_current_position()
                    return True
                else:
                    print(f"Taxi {TAXI_ID}: Fallo en la autenticación: {response_msg['data'].get('message', 'Desconocido')}")
            else:
                print(f"Taxi {TAXI_ID}: Respuesta de autenticación inesperada: {response_msg}")
    except ConnectionRefusedError:
        print(f"Taxi {TAXI_ID}: Conexión rechazada. Asegúrate de que EC_Central esté ejecutándose y el puerto {CENTRAL_PORT_AUTH} esté abierto.")
    except Exception as e:
        print(f"Taxi {TAXI_ID}: Error durante la autenticación: {e}")
    status = "disconnected"
    return False

def send_current_position():
    """Envía la posición y estado actual del taxi a Kafka."""
    position_msg = MessageProtocol.create_taxi_position_update(TAXI_ID, current_x, current_y, status)
    msg_dict = MessageProtocol.parse_message(position_msg)
    msg_dict["token"] = taxi_token
    send_kafka_message('taxi_movements', json.dumps(msg_dict))

MAP_SIZE = 20

def wrap_position(x, y):
    """Asegura que la posición esté dentro de los límites del mapa (wrap-around)."""
    return x % MAP_SIZE, y % MAP_SIZE

def get_current_position():
    return current_x, current_y

def shortest_path_step(start_x, start_y, goal_x, goal_y):
    """Calcula el siguiente paso óptimo hacia el destino usando BFS y wrap-around."""
    visited = set()
    queue = deque()
    queue.append((start_x, start_y, []))
    while queue:
        x, y, path = queue.popleft()
        if (x, y) == (goal_x, goal_y):
            return path[0] if path else (x, y)
        for dx, dy in [(-1,0),(1,0),(0,-1),(0,1)]:
            nx, ny = wrap_position(x + dx, y + dy)
            if (nx, ny) not in visited:
                visited.add((nx, ny))
                queue.append((nx, ny, path + [(nx, ny)]))
    return (start_x, start_y)

def calculate_next_step(current_x, current_y, target_x, target_y):
    return shortest_path_step(current_x, current_y, target_x, target_y)

def simulate_movement():
    """Simula el movimiento del taxi según su estado y destino."""
    global current_x, current_y, status, target_x, target_y, assigned_service_id, final_destination_id

    if status in ["disconnected", "disabled", "stopped"]:
        return

    if target_x is not None and target_y is not None:
        if (current_x, current_y) == (target_x, target_y):
            if status == "moving_to_customer":
                print(f"Taxi {TAXI_ID}: Cliente {assigned_service_id} recogido en ({current_x},{current_y}). Avisando a la Central y esperando destino final...")
                send_current_position()
            elif status == "moving_to_destination":
                print(f"Taxi {TAXI_ID}: Cliente {assigned_service_id} dejado en destino final {final_destination_id} ({current_x},{current_y}). Avisando a la Central...")
                service_completed_msg = MessageProtocol.create_service_completed(
                    client_id=assigned_service_id, taxi_id=TAXI_ID, destination_id=final_destination_id
                )
                send_kafka_message('service_notifications', MessageProtocol.parse_message(service_completed_msg))
            elif status == "returning_to_base":
                print(f"Taxi {TAXI_ID}: He vuelto a la base ({current_x},{current_y}). Quedo libre.")
                status = "free"
                target_x = 0
                target_y = 0
                send_current_position()
            else:
                print(f"Taxi {TAXI_ID}: Estado inesperado al llegar a destino: {status}")
        else:
            new_x, new_y = calculate_next_step(current_x, current_y, target_x, target_y)
            if (new_x, new_y) != (current_x, current_y):
                current_x = new_x
                current_y = new_y
                send_current_position()
    elif status == "free":
        # Movimiento aleatorio cuando está libre
        if random.random() < 0.3:
            delta_x = random.choice([-1, 0, 1])
            delta_y = random.choice([-1, 0, 1])
            new_x, new_y = wrap_position(current_x + delta_x, current_y + delta_y)
            if (new_x, new_y) != (current_x, current_y):
                current_x = new_x
                current_y = new_y
                send_current_position()

def simulate_sensor_data():
    """Simula fallos y recuperación de sensores, notificando a la Central."""
    global status
    if random.random() < 0.005 and status not in ["disabled", "stopped"]:
        sensor_status = MessageProtocol.STATUS_KO
        anomaly_details = random.choice(["Freno anómalo", "Nivel de batería bajo", "Error de navegación"])
        print(f"Taxi {TAXI_ID}: ¡Problema de sensor reportado! {anomaly_details}")
        status = "disabled"
        send_current_position()
    elif status == "disabled" and random.random() < 0.2:
        sensor_status = MessageProtocol.STATUS_OK
        anomaly_details = "Sistema recuperado."
        print(f"Taxi {TAXI_ID}: Sensores recuperados. Reestableciendo...")
        status = "free"
        send_current_position()
    sensor_msg = MessageProtocol.create_sensor_update(TAXI_ID, sensor_status, anomaly_details)
    send_kafka_message('sensor_data', MessageProtocol.parse_message(sensor_msg))

def process_taxi_commands():
    """Procesa comandos recibidos desde la Central para este taxi."""
    global status, target_x, target_y, assigned_service_id, final_destination_id

    for message in taxi_command_consumer:
        msg_value = message.value
        if msg_value.get("operation_code") == MessageProtocol.OP_TAXI_COMMAND:
            command_data = msg_value["data"]
            if command_data.get("taxi_id") == TAXI_ID:
                command = command_data.get("command")
                new_destination_coords = command_data.get("new_destination_coords")
                client_id_for_service = command_data.get("client_id")
                final_dest_id_for_service = command_data.get("final_destination_id")

                print(f"Taxi {TAXI_ID}: Recibido comando '{command}'. Datos: {command_data}")

                if command == "PICKUP" and new_destination_coords:
                    target_x, target_y = new_destination_coords["x"], new_destination_coords["y"]
                    status = "moving_to_customer"
                    assigned_service_id = client_id_for_service
                    final_destination_id = final_dest_id_for_service
                    print(f"Taxi {TAXI_ID}: Dirigiéndose a recoger al cliente {assigned_service_id} en ({target_x},{target_y}).")
                    send_current_position()
                elif command == "GOTO_DEST" and new_destination_coords:
                    target_x, target_y = new_destination_coords["x"], new_destination_coords["y"]
                    status = "moving_to_destination"
                    assigned_service_id = client_id_for_service
                    final_destination_id = final_dest_id_for_service
                    print(f"Taxi {TAXI_ID}: Dirigiéndose al destino final {final_destination_id} en ({target_x},{target_y}).")
                    send_current_position()
                elif command == "STOP":
                    status = "stopped"
                    target_x = None
                    target_y = None
                    print(f"Taxi {TAXI_ID}: Detenido por comando central.")
                    send_current_position()
                elif command == "RESUME":
                    if status == "stopped":
                        status = "free"
                        print(f"Taxi {TAXI_ID}: Reanudando operaciones.")
                        send_current_position()
                elif command == "RETURN_TO_BASE" and new_destination_coords:
                    target_x, target_y = new_destination_coords["x"], new_destination_coords["y"]
                    status = "returning_to_base"
                    assigned_service_id = None
                    final_destination_id = None
                    print(f"Taxi {TAXI_ID}: Regresando a base en ({target_x},{target_y}).")
                    send_current_position()

def process_map_updates():
    """Procesa actualizaciones del mapa recibidas desde la Central y dibuja el mapa."""
    global current_city_map, current_taxi_fleet_state, current_customer_requests_state
    for message in map_update_consumer:
        msg_value = message.value
        if msg_value.get("operation_code") == MessageProtocol.OP_MAP_UPDATE:
            current_city_map = msg_value["data"]["city_map"]
            current_taxi_fleet_state = msg_value["data"]["taxi_fleet"]
            current_customer_requests_state = msg_value["data"]["customer_requests"]
            draw_map()

def clear_console():
    os.system('cls' if os.name == 'nt' else 'clear')

def draw_map():
    """Dibuja el mapa 20x20 con taxis, clientes y localizaciones."""
    clear_console()
    print("-" * 40)
    print(f"Taxi {TAXI_ID} | Estado: {status} | Pos: ({current_x},{current_y})")
    print("-" * 40)

    grid = [["   " for _ in range(MAP_SIZE)] for _ in range(MAP_SIZE)]

    # Localizaciones (mayúsculas, fondo azul)
    for loc_id, coords in current_city_map.items():
        x, y = wrap_position(coords['x'], coords['y'])
        grid[y-1][x-1] = f"\033[44m {loc_id} \033[0m"

    # Clientes (minúsculas, fondo amarillo)
    for client_id, req_data in current_customer_requests_state.items():
        cx, cy = wrap_position(req_data['origin_coords']['x'], req_data['origin_coords']['y'])
        grid[cy-1][cx-1] = f"\033[43m {client_id[0].lower()} \033[0m"

    # Taxis (número, verde si moviéndose, rojo si parado)
    for taxi_id, taxi_data in current_taxi_fleet_state.items():
        tx, ty = wrap_position(taxi_data['x'], taxi_data['y'])
        if taxi_data["status"] in ["moving_to_customer", "moving_to_destination", "returning_to_base"]:
            color = "\033[42m"
        else:
            color = "\033[41m"
        grid[ty-1][tx-1] = f"{color}{str(taxi_id).rjust(3)}\033[0m"

    for row in reversed(grid):
        print("".join(row))
    print("-" * 40)

def register_taxi_in_registry():
    """Registra el taxi en el Registry externo."""
    url = "https://localhost:5002/register"
    try:
        response = requests.post(url, json={"taxi_id": TAXI_ID}, verify=False)
        if response.status_code == 200 and response.json().get("status") == "OK":
            print(f"Taxi {TAXI_ID}: Registrado correctamente en Registry.")
            return True
        else:
            print(f"Taxi {TAXI_ID}: Error al registrar en Registry: {response.text}")
            return False
    except Exception as e:
        print(f"Taxi {TAXI_ID}: Error al conectar con Registry: {e}")
        return False

def main():
    """Punto de entrada principal: inicializa el taxi, registra, autentica y lanza los hilos de operación."""
    global current_x, current_y, status

    print(f"Iniciando Taxi Digital Engine (EC_DE) con ID: {TAXI_ID}")

    # Cargar la posición inicial del taxi desde archivo
    try:
        with open('Central/taxis_available.txt', 'r') as f:
            for line in f:
                parts = line.strip().split()
                if len(parts) == 4:
                    taxi_id_file = int(parts[0])
                    if taxi_id_file == TAXI_ID:
                        current_x = int(parts[1])
                        current_y = int(parts[2])
                        status = "disconnected"
                        print(f"Taxi {TAXI_ID}: Posición inicial cargada ({current_x},{current_y}).")
                        break
            else:
                print(f"Advertencia: Taxi {TAXI_ID} no encontrado en taxis_available.txt. Usando 0,0 como posición inicial.")
                current_x = 0
                current_y = 0
    except FileNotFoundError:
        print("Error: taxis_available.txt no encontrado. Usando 0,0 como posición inicial.")
        current_x = 0
        current_y = 0

    if not register_taxi_in_registry():
        print(f"Taxi {TAXI_ID}: No se pudo registrar en Registry. Saliendo...")
        sys.exit(1)

    if not authenticate_with_central():
        print(f"Taxi {TAXI_ID}: Fallo en la autenticación. Saliendo...")
        sys.exit(1)

    # Lanzar hilos para comandos y actualizaciones de mapa
    threading.Thread(target=process_taxi_commands, daemon=True).start()
    threading.Thread(target=process_map_updates, daemon=True).start()

    while True:
        simulate_movement()
        #simulate_sensor_data()
        time.sleep(1)

if __name__ == "__main__":
    main()
