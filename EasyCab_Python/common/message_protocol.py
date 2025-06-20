import json

class MessageProtocol:
    # Operation Codes
    OP_AUTH_REQUEST = "AUTH_REQ"
    OP_AUTH_RESPONSE = "AUTH_RESP"
    OP_TAXI_POSITION = "TAXI_POS"
    OP_SENSOR_UPDATE = "SENSOR_UPD"
    OP_CUSTOMER_REQUEST = "CUST_REQ"
    OP_SERVICE_NOTIFICATION = "SERV_NOTIF"
    OP_TAXI_COMMAND = "TAXI_CMD"
    OP_SERVICE_COMPLETED = "SERV_COMPL"
    OP_MAP_UPDATE = "MAP_UPD"
    OP_TAXI_PICKUP = "TAXI_PICKUP"
    OP_TAXI_DROPOFF = "TAXI_DROPOFF"

    # Status Codes
    STATUS_OK = "OK"
    STATUS_KO = "KO"

    @staticmethod
    def create_message(operation_code, data):
        """Crea un mensaje JSON con código de operación y datos."""
        return json.dumps({"operation_code": operation_code, "data": data})

    @staticmethod
    def parse_message(json_string):
        """Convierte un mensaje JSON a diccionario de Python."""
        try:
            return json.loads(json_string)
        except json.JSONDecodeError as e:
            print(f"Error al parsear mensaje JSON: {e}")
            return None

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
        Crea un mensaje de actualización del mapa con ubicaciones, taxis y clientes activos.
        """
        active_customers_on_map = {}
        for client_id, req_data in customer_requests.items():
            if req_data["status"] in ["pending", "assigned", "picked_up"]:
                active_customers_on_map[client_id] = {
                    "origin_coords": req_data["origin_coords"],
                    "status": req_data["status"]
                }

        simplified_taxi_fleet = {}
        for taxi_id, taxi_data in taxi_fleet.items():
            simplified_taxi_fleet[taxi_id] = {
                "x": taxi_data["x"],
                "y": taxi_data["y"],
                "status": taxi_data["status"],
                "service_id": taxi_data.get("service_id")  # Añade el cliente asignado si existe
            }

        return MessageProtocol.create_message(
            MessageProtocol.OP_MAP_UPDATE,
            {
                "city_map": city_map,
                "taxi_fleet": simplified_taxi_fleet,
                "customer_requests": active_customers_on_map
            }
        )

    @staticmethod
    def create_taxi_pickup(taxi_id, client_id, pickup_coords):
        return MessageProtocol.create_message(
            MessageProtocol.OP_TAXI_PICKUP,
            {"taxi_id": taxi_id, "client_id": client_id, "pickup_coords": pickup_coords}
        )

    @staticmethod
    def create_taxi_dropoff(taxi_id, client_id, destination_id, dropoff_coords):
        return MessageProtocol.create_message(
            MessageProtocol.OP_TAXI_DROPOFF,
            {"taxi_id": taxi_id, "client_id": client_id, "destination_id": destination_id, "dropoff_coords": dropoff_coords}
        )
