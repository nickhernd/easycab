package es.easycab.common.protocol;

public class ProtocolConstants {

    // Comandos de la Central al Taxi/Cliente
    public static final String CMD_ACK_CONNECTION = "ACK_CONNECTION"; // Reconocimiento de conexión
    public static final String CMD_ASSIGN_TRIP = "ASSIGN_TRIP";       // Asignar un viaje al taxi
    public static final String CMD_MAP_UPDATE = "MAP_UPDATE";         // Actualización del mapa
    public static final String CMD_STOP_TAXI = "STOP_TAXI";           // Detener un taxi
    public static final String CMD_RESUME_TAXI = "RESUME_TAXI";       // Reanudar un taxi
    public static final String CMD_RETURN_TO_BASE = "RETURN_TO_BASE"; // Enviar taxi a base
    public static final String CMD_TRAFFIC_ALERT = "TRAFFIC_ALERT";   // Alerta de tráfico desde CTC

    // Comandos del Taxi a la Central
    public static final String CMD_POSITION_UPDATE = "POSITION_UPDATE"; // Actualización de posición del taxi
    public static final String CMD_SENSOR_ALERT = "SENSOR_ALERT";       // Alerta del sensor del taxi
    public static final String CMD_LOGIN_REQUEST = "LOGIN_REQUEST";     // Solicitud de login del taxi
    public static final String CMD_LOGIN_SUCCESS = "LOGIN_SUCCESS";     // Login exitoso
    public static final String CMD_LOGIN_FAILED = "LOGIN_FAILED";       // Login fallido
    public static final String CMD_LOGOUT_REQUEST = "LOGOUT_REQUEST";   // Solicitud de logout

    // Comandos del Cliente a la Central
    public static final String CMD_TRIP_REQUEST = "TRIP_REQUEST";       // Solicitud de viaje por el cliente
    public static final String CMD_TRIP_STATUS_UPDATE = "TRIP_STATUS_UPDATE"; // Actualización de estado del viaje

    // Otros
    public static final int CENTRAL_PORT = 12345; // Puerto para la comunicación con la Central
    public static final String LOCALHOST = "localhost"; // Dirección para pruebas locales

    // Nuevos comandos
    public static final String CMD_TRIP_ASSIGNED = "TRIP_ASSIGNED";
    public static final String CMD_TRIP_ACKNOWLEDGED = "TRIP_ACKNOWLEDGED";
    public static final String CMD_TRIP_COMPLETED = "TRIP_COMPLETED";

    // Para la auditoría (aunque el contenido será más complejo)
    public static final String AUDIT_AUTH_SUCCESS = "AUTH_SUCCESS";
    public static final String AUDIT_AUTH_FAILED = "AUTH_FAILED";
    public static final String AUDIT_TAXI_MOVE = "TAXI_MOVE";
    public static final String AUDIT_TRIP_ASSIGNED = "TRIP_ASSIGNED";
    public static final String AUDIT_SENSOR_ALERT = "SENSOR_ALERT";
    public static final String AUDIT_TRAFFIC_STATUS_CHANGE = "TRAFFIC_STATUS_CHANGE";


    private ProtocolConstants() {
        // Constructor privado para evitar instanciación
    }
}