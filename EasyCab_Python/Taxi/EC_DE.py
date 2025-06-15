import socket
import time
import random
import json
import sys
import threading
import os # Importar os para limpiar la consola
from kafka import KafkaProducer, KafkaConsumer
from collections import deque
import requests

# Obtiene la ruta del directorio del script actual (Central/)
script_dir = os.path.dirname(__file__)
# Obtiene la ruta del directorio EasyCab_Python/
project_root = os.path.abspath(os.path.join(script_dir, '..'))
# Añade el directorio raíz del proyecto a sys.path
sys.path.insert(0, project_root)

from common.message_protocol import MessageProtocol

# --- Configuración del Taxi ---
TAXI_ID = int(sys.argv[1]) if len(sys.argv) > 1 else 1 # ID del taxi, pasado como argumento
CENTRAL_HOST = 'localhost' # La IP o hostname de la máquina donde corre EC_Central
CENTRAL_PORT_AUTH = 65432 # Puerto para autenticación de la Central
KAFKA_BROKER = 'localhost:9094' # Dirección del broker de Kafka
IP_PORT_ECS = 'localhost:9096'


# --- Estado del Taxi ---
current_x = 0
current_y = 0
status = "disconnected" # disconnected, authenticating, free, moving_to_customer, moving_to_destination, disabled, stopped
assigned_service_id = None # ID del cliente para el servicio actual
target_x = None
target_y = None
final_destination_id = None # ID de la localización final del cliente

# --- Datos del Mapa para Visualización ---
# Los mantendremos actualizados con los mensajes de la Central
current_city_map = {}
current_taxi_fleet_state = {}
current_customer_requests_state = {}

# --- Kafka Producer para el Taxi ---
taxi_producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# --- Kafka Consumer para Comandos del Taxi ---
taxi_command_consumer = KafkaConsumer(
    'taxi_commands',
    bootstrap_servers=[KAFKA_BROKER],
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    group_id=f'taxi_command_group_{TAXI_ID}'
)

# NUEVO: Kafka Consumer para actualizaciones del mapa
map_update_consumer = KafkaConsumer(
    'map_updates',
    bootstrap_servers=[KAFKA_BROKER],
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    group_id=f'map_viewer_group_taxi_{TAXI_ID}' # Cada taxi necesita su propio group_id para recibir todos los mensajes
)

taxi_token = None

def send_kafka_message(topic, message):
    """Envía un mensaje JSON a un tema de Kafka."""
    try:
        taxi_producer.send(topic, message)
        taxi_producer.flush() 
        # print(f"Taxi {TAXI_ID} enviado a {topic}: {message}") # Descomentar para depuración
    except Exception as e:
        print(f"Taxi {TAXI_ID}: Error enviando mensaje a Kafka ({topic}): {e}")

def authenticate_with_central():
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
                    taxi_token = response_msg["data"].get("message")  # El token se envía en el campo message
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
    """Envía la posición actual del taxi a Kafka, incluyendo el token."""
    position_msg = MessageProtocol.create_taxi_position_update(TAXI_ID, current_x, current_y, status)
    # Añade el token al mensaje
    msg_dict = MessageProtocol.parse_message(position_msg)
    msg_dict["token"] = taxi_token
    send_kafka_message('taxi_movements', json.dumps(msg_dict))

MAP_SIZE = 20

def wrap_position(x, y):
    return x % MAP_SIZE, y % MAP_SIZE

def get_current_position():
    """Devuelve la posición actual del taxi."""
    return current_x, current_y

def shortest_path_step(start_x, start_y, goal_x, goal_y):
    """Devuelve el siguiente paso hacia el objetivo usando BFS y wrap-around."""
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
    return (start_x, start_y)  # Si no hay camino, quedarse

def calculate_next_step(current_x, current_y, target_x, target_y):
    """Calcula el siguiente paso óptimo hacia el destino."""
    return shortest_path_step(current_x, current_y, target_x, target_y)

def simulate_movement():
    global current_x, current_y, status, target_x, target_y, assigned_service_id, final_destination_id

    if status in ["disconnected", "disabled", "stopped"]:
        return 

    if target_x is not None and target_y is not None:
        if (current_x, current_y) == (target_x, target_y):
            if status == "moving_to_customer":
                print(f"Taxi {TAXI_ID}: Cliente {assigned_service_id} recogido en ({current_x},{current_y}). Avisando a la Central y esperando destino final...")
                send_current_position()  # Solo reporta, espera comando GOTO_DEST de la Central
            elif status == "moving_to_destination":
                print(f"Taxi {TAXI_ID}: Cliente {assigned_service_id} dejado en destino final {final_destination_id} ({current_x},{current_y}). Avisando a la Central...")
                service_completed_msg = MessageProtocol.create_service_completed(
                    client_id=assigned_service_id, taxi_id=TAXI_ID, destination_id=final_destination_id
                )
                send_kafka_message('service_notifications', MessageProtocol.parse_message(service_completed_msg))
                # Espera comando RETURN_TO_BASE de la Central
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
        if random.random() < 0.3: 
            delta_x = random.choice([-1, 0, 1])
            delta_y = random.choice([-1, 0, 1])
            new_x, new_y = wrap_position(current_x + delta_x, current_y + delta_y)
            if (new_x, new_y) != (current_x, current_y):
                current_x = new_x
                current_y = new_y
                send_current_position()

def simulate_sensor_data():
    """Simula el envío periódico de datos de sensores."""
    global status
    if random.random() < 0.005 and status not in ["disabled", "stopped"]: # Reducir la probabilidad para no ser tan molesto
        sensor_status = MessageProtocol.STATUS_KO
        anomaly_details = random.choice(["Freno anómalo", "Nivel de batería bajo", "Error de navegación"])
        print(f"Taxi {TAXI_ID}: ¡Problema de sensor reportado! {anomaly_details}")
        status = "disabled"
        send_current_position() # Actualizar el estado en la Central
    elif status == "disabled" and random.random() < 0.2: 
        sensor_status = MessageProtocol.STATUS_OK
        anomaly_details = "Sistema recuperado."
        print(f"Taxi {TAXI_ID}: Sensores recuperados. Reestableciendo...")
        status = "free" # Podría volver a su estado anterior o free
        send_current_position() # Actualizar el estado en la Central
    
    sensor_msg = MessageProtocol.create_sensor_update(TAXI_ID, sensor_status, anomaly_details)
    send_kafka_message('sensor_data', MessageProtocol.parse_message(sensor_msg))


def process_taxi_commands():
    """Escucha y procesa comandos de la Central para este taxi."""
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
                    send_current_position() # Notificar a la Central el cambio de estado
                elif command == "GOTO_DEST" and new_destination_coords:
                    target_x, target_y = new_destination_coords["x"], new_destination_coords["y"]
                    status = "moving_to_destination"
                    assigned_service_id = client_id_for_service # Mantener el service_id
                    final_destination_id = final_dest_id_for_service # Mantener el final_destination_id
                    print(f"Taxi {TAXI_ID}: Dirigiéndose al destino final {final_destination_id} en ({target_x},{target_y}).")
                    send_current_position() # Notificar a la Central el cambio de estado
                elif command == "STOP":
                    status = "stopped"
                    target_x = None
                    target_y = None
                    print(f"Taxi {TAXI_ID}: Detenido por comando central.")
                    send_current_position() # Notificar a la Central el cambio de estado
                elif command == "RESUME":
                    if status == "stopped":
                        status = "free" # o reanudar al estado anterior si tenía un servicio
                        print(f"Taxi {TAXI_ID}: Reanudando operaciones.")
                        send_current_position() # Notificar a la Central el cambio de estado
                elif command == "RETURN_TO_BASE" and new_destination_coords:
                    target_x, target_y = new_destination_coords["x"], new_destination_coords["y"]
                    status = "returning_to_base"
                    assigned_service_id = None
                    final_destination_id = None
                    print(f"Taxi {TAXI_ID}: Regresando a base en ({target_x},{target_y}).")
                    send_current_position()
def process_map_updates():
    """Escucha y procesa actualizaciones del mapa de la Central."""
    global current_city_map, current_taxi_fleet_state, current_customer_requests_state
    for message in map_update_consumer:
        msg_value = message.value
        if msg_value.get("operation_code") == MessageProtocol.OP_MAP_UPDATE:
            current_city_map = msg_value["data"]["city_map"]
            current_taxi_fleet_state = msg_value["data"]["taxi_fleet"]
            current_customer_requests_state = msg_value["data"]["customer_requests"]
            draw_map() # Dibujar el mapa cada vez que llega una actualización

def clear_console():
    """Limpia la consola."""
    os.system('cls' if os.name == 'nt' else 'clear')

def draw_map():
    """Dibuja el mapa 20x20 con colores y wrap-around."""
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
            color = "\033[42m"  # Verde
        else:
            color = "\033[41m"  # Rojo
        grid[ty-1][tx-1] = f"{color}{str(taxi_id).rjust(3)}\033[0m"

    # Imprimir el grid (Y invertido para que 1,1 sea abajo izquierda)
    for row in reversed(grid):
        print("".join(row))
    print("-" * 40)

def register_taxi_in_registry():
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
    global current_x, current_y, status

    print(f"Iniciando Taxi Digital Engine (EC_DE) con ID: {TAXI_ID}")

    # Cargar la posición inicial del taxi
    try:
        with open('Central/taxis_available.txt', 'r') as f:
            for line in f:
                parts = line.strip().split()
                if len(parts) == 4:
                    taxi_id_file = int(parts[0])
                    if taxi_id_file == TAXI_ID:
                        current_x = int(parts[1])  # NO sumes 1
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

    # Iniciar hilos para procesar comandos y actualizaciones de mapa
    threading.Thread(target=process_taxi_commands, daemon=True).start()
    threading.Thread(target=process_map_updates, daemon=True).start() # NUEVO: Hilo para procesar actualizaciones de mapa

    while True:
        simulate_movement()
        #simulate_sensor_data()
        time.sleep(1) # Espera 1 segundo para el siguiente paso de simulación

if __name__ == "__main__":
    main()