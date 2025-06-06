import json

class MessageProtocol:
    # --- Operation Codes ---
    OP_AUTH_REQUEST = "AUTH_REQ"
    OP_AUTH_RESPONSE = "AUTH_RESP"
    OP_TAXI_POSITION = "TAXI_POS"
    OP_SENSOR_UPDATE = "SENSOR_UPD"
    OP_CUSTOMER_REQUEST = "CUST_REQ"
    OP_SERVICE_NOTIFICATION = "SERV_NOTIF" # General service notifications (e.g., accepted, denied)
    OP_TAXI_COMMAND = "TAXI_CMD" # Commands from Central to Taxi (PICKUP, GOTO_DEST, STOP, RESUME, etc.)
    OP_SERVICE_COMPLETED = "SERV_COMPL" # Notification from Taxi to Central when a service is completed
    OP_MAP_UPDATE = "MAP_UPD" # New: Central sends full map state

    # --- Status Codes ---
    STATUS_OK = "OK"
    STATUS_KO = "KO"

    @staticmethod
    def create_message(operation_code, data):
        """Crea un mensaje en formato JSON."""
        return json.dumps({"operation_code": operation_code, "data": data})

    @staticmethod
    def parse_message(json_string):
        """Parsea un mensaje JSON a un diccionario de Python."""
        try:
            return json.loads(json_string)
        except json.JSONDecodeError as e:
            print(f"Error al parsear mensaje JSON: {e}")
            return None

    # --- Specific Message Creators ---

    @staticmethod
    def create_auth_request(taxi_id):
        return MessageProtocol.create_message(
            MessageProtocol.OP_AUTH_REQUEST,
            {"taxi_id": taxi_id}
        )

    @staticmethod
    def create_auth_response(taxi_id, status, message):
        return MessageProtocol.create_message(
            MessageProtocol.OP_AUTH_RESPONSE,
            {"taxi_id": taxi_id, "status": status, "message": message}
        )

    @staticmethod
    def create_taxi_position_update(taxi_id, x, y, status):
        return MessageProtocol.create_message(
            MessageProtocol.OP_TAXI_POSITION,
            {"taxi_id": taxi_id, "x": x, "y": y, "status": status}
        )

    @staticmethod
    def create_sensor_update(taxi_id, status, anomaly_details=""):
        return MessageProtocol.create_message(
            MessageProtocol.OP_SENSOR_UPDATE,
            {"taxi_id": taxi_id, "status": status, "anomaly_details": anomaly_details}
        )
    
    @staticmethod
    def create_customer_request(client_id, destination_id):
        return MessageProtocol.create_message(
            MessageProtocol.OP_CUSTOMER_REQUEST,
            {"client_id": client_id, "destination_id": destination_id}
        )
    
    @staticmethod
    def create_service_notification(client_id, status, taxi_id=None, message=""):
        data = {"client_id": client_id, "status": status, "message": message}
        if taxi_id is not None:
            data["taxi_id"] = taxi_id
        return MessageProtocol.create_message(
            MessageProtocol.OP_SERVICE_NOTIFICATION,
            data
        )

    @staticmethod
    def create_taxi_command(taxi_id, command, new_destination_coords=None, client_id=None, final_destination_id=None):
        data = {"taxi_id": taxi_id, "command": command}
        if new_destination_coords is not None:
            data["new_destination_coords"] = new_destination_coords
        if client_id is not None:
            data["client_id"] = client_id
        if final_destination_id is not None:
            data["final_destination_id"] = final_destination_id
        return MessageProtocol.create_message(
            MessageProtocol.OP_TAXI_COMMAND,
            data
        )

    @staticmethod
    def create_service_completed(client_id, taxi_id, destination_id):
        return MessageProtocol.create_message(
            MessageProtocol.OP_SERVICE_COMPLETED,
            {"client_id": client_id, "taxi_id": taxi_id, "destination_id": destination_id}
        )
    
    @staticmethod
    def create_map_update(city_map, taxi_fleet, customer_requests):
        """
        Crea un mensaje de actualización del mapa.
        city_map: dict de ubicaciones.
        taxi_fleet: dict de info de taxis (incluye x,y,status,service_id).
        customer_requests: dict de solicitudes de clientes (incluye origin_coords, status).
        """
        # Simplificar customer_requests para el mapa: solo mostrar clientes pendientes o asignados.
        # Las coordenadas de los clientes pendientes/asignados se usarán para dibujarlos.
        active_customers_on_map = {}
        for client_id, req_data in customer_requests.items():
            if req_data["status"] in ["pending", "assigned", "picked_up"]:
                active_customers_on_map[client_id] = {
                    "origin_coords": req_data["origin_coords"],
                    "status": req_data["status"]
                }

        # Adaptar taxi_fleet para el mapa: solo lo esencial
        simplified_taxi_fleet = {}
        for taxi_id, taxi_data in taxi_fleet.items():
            simplified_taxi_fleet[taxi_id] = {
                "x": taxi_data["x"],
                "y": taxi_data["y"],
                "status": taxi_data["status"]
            }

        return MessageProtocol.create_message(
            MessageProtocol.OP_MAP_UPDATE,
            {
                "city_map": city_map, # Las ubicaciones fijas (A, B, C...)
                "taxi_fleet": simplified_taxi_fleet, # Posiciones y estados de taxis
                "customer_requests": active_customers_on_map # Posiciones y estados de clientes (puntos de recogida)
            }
        )
