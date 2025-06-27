import socket
import time
import random
import json
import sys
import threading
import os
from kafka import KafkaProducer, KafkaConsumer
from collections import deque
import argparse
import http.client
import ssl

# Argumentos de línea de comandos para configuración
argparser = argparse.ArgumentParser(description='Taxi Digital Engine (EC_DE) para EasyCab.')
argparser.add_argument('--ip_ecc_host', type=str, default='localhost', help='Hostname o IP para la autenticación de EC_Central.')
argparser.add_argument('--ip_ecc_port', type=int, default=65432, help='Puerto para la autenticación de EC_Central.')
argparser.add_argument('--kafka_broker', type=str, default='localhost:9094', help='Dirección del broker de Kafka.')
argparser.add_argument('--ip_port_ecs', type=str, default='localhost:5002', help='Dirección del Registry (host:puerto).')
args = argparser.parse_args()

# --- Modificación para obtener un ID de Taxi dinámico ---
# Obtiene el nombre de host del contenedor (ej. 'pc3-taxi-1', 'pc3-taxi-2')
container_hostname = os.getenv('HOSTNAME', 'unknown_host')
print(f"DEBUG_DE (Pre-Derivation): Hostname recibido: '{container_hostname}'") # Debug print

try:
    # Intenta extraer un número del nombre de host.
    # Esto asume una convención de nombres consistente de Docker Compose (e.g., <project>-<service>-<number>).
    parts = container_hostname.split('-')
    print(f"DEBUG_DE (Derivation): Partes del hostname: {parts}") # Debug print

    numerical_id = 1 # Valor por defecto si no se encuentra un número
    for part in reversed(parts): # Busca desde el final para el número de instancia
        if part.isdigit():
            numerical_id = int(part)
            break
    
    global TAXI_ID # Se asegura de que estamos modificando la variable global
    TAXI_ID = numerical_id
    print(f"DEBUG_DE (Derivation): ID numérico derivado: {numerical_id}, TAXI_ID asignado: {TAXI_ID}.") # Debug print
    print(f"Taxi {TAXI_ID}: ID derivado del hostname '{container_hostname}'.")
except Exception as e:
    global TAXI_ID # Se asegura de que estamos modificando la variable global en caso de error
    print(f"DEBUG_DE (Derivation Error): Excepción durante la derivación del ID: {e}") # Debug print
    print(f"Taxi {TAXI_ID}: No se pudo derivar el ID del taxi del hostname '{container_hostname}', usando 1 por defecto. Error: {e}")
    TAXI_ID = 1 # Fallback si la derivación falla

# Añade el directorio raíz del proyecto a sys.path para importar módulos comunes
script_dir = os.path.dirname(__file__)
project_root = os.path.abspath(os.path.join(script_dir, '..'))
sys.path.insert(0, project_root)

from common.message_protocol import MessageProtocol

# --- Configuración del Taxi ---
# TAXI_ID ya se ha establecido dinámicamente arriba
CENTRAL_HOST = args.ip_ecc_host
CENTRAL_PORT_AUTH = args.ip_ecc_port
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
_just_returned_to_base = False


# --- Estados para visualización del mapa (no se usan para dibujar en este script) ---
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
    group_id=f'taxi_command_group_{TAXI_ID}' # Grupo de consumidor único para cada taxi
)

map_update_consumer = KafkaConsumer(
    'map_updates',
    bootstrap_servers=[KAFKA_BROKER],
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    group_id=f'map_viewer_group_taxi_{TAXI_ID}' # Grupo de consumidor único para cada taxi
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
    msg_dict["token"] = taxi_token # Añade el token para la Central
    send_kafka_message('taxi_movements', msg_dict)

MAP_SIZE = 20 # Tamaño del mapa para el wrap-around (se asume que la central usa el mismo tamaño)

def wrap_position(x, y):
    """Asegura que la posición esté dentro de los límites del mapa (wrap-around)."""
    return x % MAP_SIZE, y % MAP_SIZE

def get_current_position():
    """Retorna la posición actual del taxi."""
    return current_x, current_y

def shortest_path_step(start_x, start_y, goal_x, goal_y):
    """Calcula el siguiente paso óptimo hacia el destino usando BFS y wrap-around."""
    visited = set()
    queue = deque()
    queue.append((start_x, start_y, [])) # (x, y, path_to_here)

    while queue:
        x, y, path = queue.popleft()

        if (x, y) == (goal_x, goal_y):
            # Si ya estamos en el objetivo o si es el primer paso del camino más corto
            return path[0] if path else (x, y)

        # Direcciones: arriba, abajo, izquierda, derecha
        for dx, dy in [(-1, 0), (1, 0), (0, -1), (0, 1)]:
            nx, ny = wrap_position(x + dx, y + dy) # Aplica wrap-around

            if (nx, ny) not in visited:
                visited.add((nx, ny))
                queue.append((nx, ny, path + [(nx, ny)]))
    return (start_x, start_y) # Si no se encuentra un camino (no debería pasar con wrap-around)

def calculate_next_step(current_x, current_y, target_x, target_y):
    """Determina el siguiente paso hacia un objetivo."""
    return shortest_path_step(current_x, current_y, target_x, target_y)

def simulate_movement():
    """Simula el movimiento del taxi según su estado y destino, mostrando logs de depuración."""
    global current_x, current_y, status, target_x, target_y, assigned_service_id, final_destination_id, _just_returned_to_base

    # Log siempre el estado actual del taxi para depuración
    print(f"[TAXI LOG] ID: {TAXI_ID} | Estado: {status} | Posición actual: ({current_x},{current_y}) | Destino: ({target_x},{target_y}) | Cliente asignado: {assigned_service_id} | Destino final: {final_destination_id}")

    # FIX: Si el taxi está en waiting_goto_dest pero ya tiene destino, cambiar a moving_to_destination
    if status == "waiting_goto_dest" and target_x is not None and target_y is not None:
        print(f"[TAXI FIX] Taxi {TAXI_ID}: Cambiando de waiting_goto_dest a moving_to_destination para ir a ({target_x},{target_y})")
        status = "moving_to_destination"

    # FIX: Si el taxi está en waiting_return_to_base pero ya tiene destino (base), cambiar a returning_to_base
    if status == "waiting_return_to_base" and target_x is not None and target_y is not None:
        print(f"[TAXI FIX] Taxi {TAXI_ID}: Cambiando de waiting_return_to_base a returning_to_base para ir a base en ({target_x},{target_y})")
        status = "returning_to_base"

    # Si el taxi está esperando un comando no manejado o en estado inmovilizado, no se mueve
    if status in ["disconnected", "disabled", "stopped", "waiting_goto_dest", "waiting_return_to_base"]:
        return

    # Si hay un destino definido, intentar moverse
    if target_x is not None and target_y is not None:
        if (current_x, current_y) == (target_x, target_y):
            # Ha llegado al destino
            if status == "moving_to_customer":
                print(f"Taxi {TAXI_ID}: Cliente {assigned_service_id} recogido en ({current_x},{current_y}). Avisando a la Central y esperando siguiente comando...")
                pickup_msg = MessageProtocol.create_taxi_pickup(
                    taxi_id=TAXI_ID, client_id=assigned_service_id, pickup_coords={"x": current_x, "y": current_y}
                )
                send_kafka_message('taxi_pickups', MessageProtocol.parse_message(pickup_msg))
                send_current_position()
                status = "waiting_goto_dest" # Espera el comando GOTO_DEST para llevar al cliente
                _just_returned_to_base = False
            elif status == "moving_to_destination":
                print(f"Taxi {TAXI_ID}: Cliente {assigned_service_id} dejado en destino final {final_destination_id} ({current_x},{current_y}). Avisando a la Central y esperando siguiente comando...")
                dropoff_msg = MessageProtocol.create_taxi_dropoff(
                    taxi_id=TAXI_ID, client_id=assigned_service_id, destination_id=final_destination_id, dropoff_coords={"x": current_x, "y": current_y}
                )
                send_kafka_message('taxi_dropoffs', MessageProtocol.parse_message(dropoff_msg))
                send_current_position()
                status = "waiting_return_to_base" # Espera el comando RETURN_TO_BASE
                _just_returned_to_base = False
            elif status == "returning_to_base":
                print(f"Taxi {TAXI_ID}: He vuelto a la base ({current_x},{current_y}). Quedo libre.")
                status = "free"
                # Opcional: Podrías querer resetear la posición a una "base" predefinida para cada taxi aquí
                # Si las bases están en 'Central/taxis_available.txt' y quieres que el taxi vaya allí:
                try:
                    # En un entorno real, las posiciones de base se configurarían por servicio o BBDD, no un archivo local.
                    # Aquí asumo que este archivo se usaría para definir la base inicial de cada taxi.
                    with open('Central/taxis_available.txt', 'r') as f:
                        for line in f:
                            parts = line.strip().split()
                            if len(parts) >= 3 and int(parts[0]) == TAXI_ID:
                                initial_x = int(parts[1])
                                initial_y = int(parts[2])
                                current_x = initial_x
                                current_y = initial_y
                                print(f"Taxi {TAXI_ID}: Posición reseteada a base ({current_x},{current_y}) tras finalizar servicio.")
                                break
                except FileNotFoundError:
                    print(f"Taxi {TAXI_ID}: Archivo 'Central/taxis_available.txt' no encontrado, no se pudo resetear la posición de base.")
                except Exception as e:
                    print(f"Taxi {TAXI_ID}: Error al recuperar posición inicial de base desde archivo: {e}")

                target_x = None
                target_y = None
                assigned_service_id = None
                final_destination_id = None
                send_current_position()
                _just_returned_to_base = True # Bandera para indicar que acaba de volver a base
            else:
                print(f"Taxi {TAXI_ID}: Estado inesperado al llegar a destino: {status}")
        else:
            # Todavía no ha llegado al destino, calcular y mover
            print(f"[TAXI LOG] Taxi {TAXI_ID}: Avanzando de ({current_x},{current_y}) hacia ({target_x},{target_y})")
            new_x, new_y = calculate_next_step(current_x, current_y, target_x, target_y)
            if (new_x, new_y) != (current_x, current_y): # Solo actualiza si hay movimiento
                current_x = new_x
                current_y = new_y
                send_current_position()

def simulate_sensor_data():
    """Simula fallos y recuperación de sensores, notificando a la Central."""
    global status
    # Probabilidad de fallo del sensor
    if random.random() < 0.005 and status not in ["disabled", "stopped"]:
        sensor_status = MessageProtocol.STATUS_KO
        anomaly_details = random.choice(["Freno anómalo", "Nivel de batería bajo", "Error de navegación"])
        print(f"Taxi {TAXI_ID}: ¡Problema de sensor reportado! {anomaly_details}")
        status = "disabled" # Cambia el estado a deshabilitado
        send_current_position()
    # Probabilidad de recuperación del sensor si está deshabilitado
    elif status == "disabled" and random.random() < 0.2:
        sensor_status = MessageProtocol.STATUS_OK
        anomaly_details = "Sistema recuperado."
        print(f"Taxi {TAXI_ID}: Sensores recuperados. Reestableciendo...")
        status = "free" # Vuelve al estado libre
        send_current_position()
    else:
        sensor_status = MessageProtocol.STATUS_OK # Si no hay problema, el estado es OK
        anomaly_details = "Normal" # Sin anomalías
    sensor_msg = MessageProtocol.create_sensor_update(TAXI_ID, sensor_status, anomaly_details)
    send_kafka_message('sensor_data', MessageProtocol.parse_message(sensor_msg))

def process_taxi_commands():
    """Procesa comandos recibidos desde la Central para este taxi."""
    global status, target_x, target_y, assigned_service_id, final_destination_id, _just_returned_to_base

    # Escucha continuamente los mensajes en el tema 'taxi_commands'
    for message in taxi_command_consumer:
        msg_value = message.value # El mensaje ya está deserializado por el consumer
        if msg_value.get("operation_code") == MessageProtocol.OP_TAXI_COMMAND:
            command_data = msg_value["data"]
            # Permite que el comando sea para el ID numérico o sus variantes de string
            # Esto es útil si la Central genera IDs como '1', '1a', '01', etc.
            taxi_id_msg = str(command_data.get("taxi_id"))
            taxi_id_variants = [str(TAXI_ID), str(TAXI_ID) + "a", str(TAXI_ID).zfill(2), str(TAXI_ID).zfill(2) + "a"]

            if taxi_id_msg in taxi_id_variants:
                command = command_data.get("command")
                new_destination_coords = command_data.get("new_destination_coords")
                client_id_for_service = command_data.get("client_id")
                final_dest_id_for_service = command_data.get("final_destination_id")

                print(f"[TAXI DEBUG] Taxi {TAXI_ID}: Recibido comando '{command}'. Datos: {command_data}")

                if command == "GOTO_DEST":
                    print(f"[TAXI DEBUG] Taxi {TAXI_ID}: new_destination_coords recibido en GOTO_DEST: {new_destination_coords}")
                    if new_destination_coords and "x" in new_destination_coords and "y" in new_destination_coords:
                        target_x, target_y = new_destination_coords["x"], new_destination_coords["y"]
                        status = "moving_to_destination"
                        assigned_service_id = client_id_for_service
                        final_destination_id = final_dest_id_for_service
                        print(f"[TAXI DEBUG] Taxi {TAXI_ID}: Dirigiéndose al destino final {final_destination_id} en ({target_x},{target_y}).")
                        send_current_position()
                        _just_returned_to_base = False
                    else:
                        print(f"[TAXI ERROR] Taxi {TAXI_ID}: GOTO_DEST comando recibido sin coordenadas válidas: {new_destination_coords}")

                elif command == "PICKUP":
                    if new_destination_coords and "x" in new_destination_coords and "y" in new_destination_coords:
                        target_x, target_y = new_destination_coords["x"], new_destination_coords["y"]
                        status = "moving_to_customer"
                        assigned_service_id = client_id_for_service
                        final_destination_id = final_dest_id_for_service # Podría ser None en este punto si solo es recogida
                        print(f"[TAXI DEBUG] Taxi {TAXI_ID}: Dirigiéndose a recoger al cliente {assigned_service_id} en ({target_x},{target_y}).")
                        send_current_position()
                    else:
                        print(f"[TAXI ERROR] Taxi {TAXI_ID}: PICKUP comando recibido sin coordenadas válidas: {new_destination_coords}")

                elif command == "STOP":
                    status = "stopped"
                    target_x = None # Limpia el destino actual
                    target_y = None
                    print(f"[TAXI DEBUG] Taxi {TAXI_ID}: Detenido por comando central.")
                    send_current_position()

                elif command == "RESUME":
                    if status == "stopped": # Solo reanuda si estaba detenido
                        status = "free" # Vuelve a estar libre
                        print(f"[TAXI DEBUG] Taxi {TAXI_ID}: Reanudando operaciones.")
                        send_current_position()
                    else:
                        print(f"[TAXI DEBUG] Taxi {TAXI_ID}: Comando RESUME ignorado, el taxi no estaba detenido.")

                elif command == "RETURN_TO_BASE":
                    if new_destination_coords and "x" in new_destination_coords and "y" in new_destination_coords:
                        target_x, target_y = new_destination_coords["x"], new_destination_coords["y"]
                        status = "returning_to_base"
                        assigned_service_id = None # Ya no hay cliente asignado
                        final_destination_id = None # Ya no hay destino final de cliente
                        print(f"[TAXI DEBUG] Taxi {TAXI_ID}: Regresando a base en ({target_x},{target_y}).")
                        send_current_position()
                        _just_returned_to_base = False
                    else:
                        print(f"[TAXI ERROR] Taxi {TAXI_ID}: RETURN_TO_BASE comando recibido sin coordenadas válidas: {new_destination_coords}")
            # else:
            #     print(f"[TAXI DEBUG] Taxi {TAXI_ID}: Comando recibido para otro taxi: {taxi_id_msg}")

def process_map_updates():
    """Procesa actualizaciones del mapa recibidas desde la Central (sin imprimir el mapa en consola)."""
    global current_city_map, current_taxi_fleet_state, current_customer_requests_state
    for message in map_update_consumer:
        msg_value = message.value
        if msg_value.get("operation_code") == MessageProtocol.OP_MAP_UPDATE:
            current_city_map = msg_value["data"]["city_map"]
            current_taxi_fleet_state = msg_value["data"]["taxi_fleet"]
            current_customer_requests_state = msg_value["data"]["customer_requests"]
    # No se llama a draw_map ni se imprime nada para evitar ruido en la consola de cada taxi

def register_taxi_in_registry():
    """Registra el taxi en el Registry externo usando http.client."""
    global TAXI_ID, IP_PORT_ECS

    # Parsea el host y el puerto de IP_PORT_ECS (ej. 'registry:5002')
    registry_host = IP_PORT_ECS.split(':')[0]
    registry_port = int(IP_PORT_ECS.split(':')[-1])

    url_path = "/register"
    headers = {'Content-Type': 'application/json'}
    payload = json.dumps({"taxi_id": TAXI_ID}) # Envía el ID único del taxi

    print(f"Taxi {TAXI_ID}: Intentando registrar en Registry en https://{registry_host}:{registry_port}{url_path}...")

    try:
        # Usa HTTPSConnection para HTTPS.
        # context=ssl._create_unverified_context() es para ignorar la verificación SSL,
        # útil en entornos de desarrollo con certificados auto-firmados o sin certs válidos.
        conn = http.client.HTTPSConnection(registry_host, registry_port, context=ssl._create_unverified_context())
        conn.request("POST", url_path, body=payload, headers=headers)
        response = conn.getresponse()
        response_data = response.read().decode('utf-8')
        conn.close()

        if response.status == 200:
            response_json = json.loads(response_data)
            if response_json.get("status") == "OK":
                print(f"Taxi {TAXI_ID}: Registrado correctamente en Registry.")
                return True
            else:
                print(f"Taxi {TAXI_ID}: Error al registrar en Registry: {response_data}")
                return False
        else:
            print(f"Taxi {TAXI_ID}: Error HTTP {response.status} al registrar en Registry: {response_data}")
            return False
    except Exception as e:
        print(f"Taxi {TAXI_ID}: Error al conectar con Registry: {e}")
        return False

def send_new_taxi_data_to_central():
    """Envía los datos del taxi disponible a la Central a través de Kafka."""
    global current_x, current_y, status, TAXI_ID
    # Se incluye el ID único del taxi y su estado inicial
    taxi_fleet = {"TAXI_ID": TAXI_ID, "x": current_x, "y": current_y, "status": status}
    send_kafka_message('taxis_available', taxi_fleet)
    print(f"Taxi {TAXI_ID}: Datos enviados a Central: {taxi_fleet}")

def main():
    """Punto de entrada principal: inicializa el taxi, registra, autentica y lanza los hilos de operación."""
    global current_x, current_y, status

    send_new_taxi_data_to_central()  # Enviar datos iniciales del taxi a Kafka al iniciar

    print(f"Iniciando Taxi Digital Engine (EC_DE) con ID: {TAXI_ID}")

    # Paso 1: Registrarse en el Registry
    if not register_taxi_in_registry():
        print(f"Taxi {TAXI_ID}: No se pudo registrar en Registry. Saliendo...")
        sys.exit(1) # Termina la ejecución si no se registra

    # Paso 2: Autenticarse con la Central
    if not authenticate_with_central():
        print(f"Taxi {TAXI_ID}: Fallo en la autenticación. Saliendo...")
        sys.exit(1) # Termina la ejecución si la autenticación falla

    # Lanzar hilos para procesar comandos y actualizaciones de mapa de forma asíncrona
    threading.Thread(target=process_taxi_commands, daemon=True).start()
    threading.Thread(target=process_map_updates, daemon=True).start()

    # Bucle principal de simulación del taxi
    while True:
        simulate_movement() # Simula el movimiento del taxi
        simulate_sensor_data() # Simula datos de sensores (comentado en el original, pero incluido por completitud)
        time.sleep(1) # Espera 1 segundo antes de la siguiente iteración

if __name__ == "__main__":
    main()
