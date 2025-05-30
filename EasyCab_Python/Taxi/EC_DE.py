import socket
import time
import random
import json
import sys
import threading
import os # Importar os para limpiar la consola
from kafka import KafkaProducer, KafkaConsumer

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


def send_kafka_message(topic, message):
    """Envía un mensaje JSON a un tema de Kafka."""
    try:
        taxi_producer.send(topic, message)
        taxi_producer.flush() 
        # print(f"Taxi {TAXI_ID} enviado a {topic}: {message}") # Descomentar para depuración
    except Exception as e:
        print(f"Taxi {TAXI_ID}: Error enviando mensaje a Kafka ({topic}): {e}")

def authenticate_with_central():
    """Intenta autenticarse con la Central via socket."""
    global status
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
                    status = "free"
                    print(f"Taxi {TAXI_ID}: Autenticación exitosa. Estado: {status}")
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
    """Envía la posición actual del taxi a Kafka."""
    position_msg = MessageProtocol.create_taxi_position_update(TAXI_ID, current_x, current_y, status)
    send_kafka_message('taxi_movements', MessageProtocol.parse_message(position_msg))

def calculate_next_step(current_x, current_y, target_x, target_y):
    """Calcula el siguiente paso para acercarse al objetivo."""
    dx = target_x - current_x
    dy = target_y - current_y

    next_x, next_y = current_x, current_y

    if dx != 0:
        next_x += 1 if dx > 0 else -1
    if dy != 0:
        next_y += 1 if dy > 0 else -1
    
    return next_x, next_y

def simulate_movement():
    """Simula el movimiento dirigido del taxi o movimiento aleatorio si está libre."""
    global current_x, current_y, status, target_x, target_y, assigned_service_id, final_destination_id

    if status == "disconnected" or status == "disabled" or status == "stopped":
        return 

    if target_x is not None and target_y is not None:
        if (current_x, current_y) == (target_x, target_y):
            if status == "moving_to_customer":
                print(f"Taxi {TAXI_ID}: Llegó al cliente {assigned_service_id} en ({current_x},{current_y}).")
                # Aquí el taxi espera el siguiente comando para ir al destino final.
                # La Central debería enviar el comando "GOTO_DEST" después de esto.
                # Por ahora, el taxi simplemente limpia su target y espera un nuevo comando.
                status = "picked_up" # Un nuevo estado para indicar que el cliente ha sido recogido
                target_x = None 
                target_y = None
                send_current_position() # Actualizar status a la Central
            elif status == "moving_to_destination":
                print(f"Taxi {TAXI_ID}: Llegó al destino final {final_destination_id} para cliente {assigned_service_id}.")
                service_completed_msg = MessageProtocol.create_service_completed(
                    client_id=assigned_service_id, taxi_id=TAXI_ID, destination_id=final_destination_id
                )
                send_kafka_message('service_notifications', MessageProtocol.parse_message(service_completed_msg))
                status = "free" 
                assigned_service_id = None
                target_x = None
                target_y = None
                final_destination_id = None
                send_current_position() 

        else:
            new_x, new_y = calculate_next_step(current_x, current_y, target_x, target_y)
            if (new_x, new_y) != (current_x, current_y):
                current_x = new_x
                current_y = new_y
                # print(f"Taxi {TAXI_ID}: Movido a ({current_x},{current_y}). Estado: {status}. Hacia ({target_x},{target_y})")
                send_current_position()
    elif status == "free":
        if random.random() < 0.3: 
            delta_x = random.choice([-1, 0, 1])
            delta_y = random.choice([-1, 0, 1])
            new_x = max(0, min(10, current_x + delta_x)) 
            new_y = max(0, min(10, current_y + delta_y)) 

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
                elif command == "RETURN_TO_BASE":
                    target_x, target_y = 1, 1 
                    status = "moving_to_base"
                    assigned_service_id = None
                    final_destination_id = None
                    print(f"Taxi {TAXI_ID}: Regresando a base en ({target_x},{target_y}).")
                    send_current_position() # Notificar a la Central el cambio de estado

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
    """Dibuja el mapa ASCII en la consola."""
    clear_console()
    print("-" * 30)
    print(f"Taxi {TAXI_ID} | Estado: {status} | Pos: ({current_x},{current_y})")
    print("-" * 30)

    # Determinar las dimensiones del mapa
    max_x = max([loc['x'] for loc in current_city_map.values()] + [taxi['x'] for taxi in current_taxi_fleet_state.values()] + [cust['origin_coords']['x'] for cust in current_customer_requests_state.values()])
    max_y = max([loc['y'] for loc in current_city_map.values()] + [taxi['y'] for taxi in current_taxi_fleet_state.values()] + [cust['origin_coords']['y'] for cust in current_customer_requests_state.values()])

    grid_width = max_x + 1 # +1 porque las coordenadas son 0-indexadas
    grid_height = max_y + 1

    grid = [[' . ' for _ in range(grid_width)] for _ in range(grid_height)]

    # Colocar ubicaciones de la ciudad
    for loc_id, coords in current_city_map.items():
        if 0 <= coords['y'] < grid_height and 0 <= coords['x'] < grid_width:
            grid[coords['y']][coords['x']] = f" {loc_id} " # Letra mayúscula para ubicaciones

    # Colocar clientes (puntos de recogida)
    for client_id, req_data in current_customer_requests_state.items():
        cx, cy = req_data['origin_coords']['x'], req_data['origin_coords']['y']
        if 0 <= cy < grid_height and 0 <= cx < grid_width:
            # Representar cliente como 'c' o la primera letra de su ID en minúscula si es posible
            if grid[cy][cx] == ' . ': # Solo si no hay una ubicación fija
                grid[cy][cx] = f" {client_id[0].lower()} "

    # Colocar taxis (¡último para que se vean sobre otros elementos si coinciden!)
    for taxi_id, taxi_data in current_taxi_fleet_state.items():
        tx, ty = taxi_data['x'], taxi_data['y']
        # El taxi actual se representa con su ID, otros taxis con 'T'
        if taxi_id == TAXI_ID:
            grid[ty][tx] = f"[{TAXI_ID}]"
        else:
            grid[ty][tx] = f"[T{taxi_id}]" # Otros taxis con "T" y su ID

    # Imprimir el grid (invertir Y para que (0,0) sea abajo izquierda)
    for row in reversed(grid):
        print("".join(row))
    print("-" * 30)

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