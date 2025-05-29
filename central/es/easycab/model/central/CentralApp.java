package es.easycab.model.central;

// Importaciones necesarias para el funcionamiento de la aplicación central
import es.easycab.common.model.Taxi;
import es.easycab.common.model.TripRequest;
import es.easycab.common.protocol.ProtocolConstants;
import com.fasterxml.jackson.databind.ObjectMapper; 
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Duration;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class CentralApp {
    private static final String TAXI_POSITIONS_TOPIC = "taxi-positions";

    private ServerSocket serverSocket;
    private ExecutorService clientThreadPool; // Gestiona los hilos para las conexiones de sockets
    private KafkaConsumer<String, String> kafkaConsumer;
    private ObjectMapper objectMapper; // Objeto Jackson para serialización/deserialización JSON

    // Mapas para gestionar los clientes conectados y sus estados
    // Almacena el ClientHandler de cada cliente conectado (Taxi o Cliente) por su ID
    public Map<String, ClientHandler> connectedClients = new ConcurrentHashMap<>();
    // Almacena el estado más reciente de todos los taxis registrados, actualizado por mensajes de Kafka
    private Map<String, Taxi> registeredTaxis = new ConcurrentHashMap<>();

    public CentralApp() {
        this.clientThreadPool = Executors.newFixedThreadPool(10);
        this.objectMapper = new ObjectMapper();
        setupKafkaConsumer(); // Inicializa el consumidor de Kafka
    }

    private void setupKafkaConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "easycab-central-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()); // Sigue siendo String, se deserializa manualmente
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // Empieza a leer desde el principio
        this.kafkaConsumer = new KafkaConsumer<>(props);
        this.kafkaConsumer.subscribe(Collections.singletonList(TAXI_POSITIONS_TOPIC));
        System.out.println("CentralApp: Consumidor Kafka suscrito al tópico '" + TAXI_POSITIONS_TOPIC + "'");
    }

    public void startServer() { // Renombrado desde startSocketServer para mayor claridad
        try {
            serverSocket = new ServerSocket(ProtocolConstants.CENTRAL_PORT);
            System.out.println("CentralApp: Servidor de sockets escuchando en el puerto " + ProtocolConstants.CENTRAL_PORT);

            // Inicia el consumidor de Kafka en un hilo separado
            new Thread(this::startKafkaConsumerLoop).start(); // Renombrado para mayor claridad

            // Acepta nuevas conexiones de clientes en el bucle principal
            while (true) {
                Socket clientSocket = serverSocket.accept();
                System.out.println("CentralApp: Nuevo cliente conectado: " + clientSocket.getInetAddress().getHostAddress());
                ClientHandler clientHandler = new ClientHandler(clientSocket, this);
                clientThreadPool.submit(clientHandler); // Asigna el handler a un hilo del pool
            }
        } catch (IOException e) {
            System.err.println("CentralApp Error en el servidor de sockets: " + e.getMessage());
        } finally {
            closeConnections(); // Asegura que los recursos se cierren al apagar
        }
    }

    private void startKafkaConsumerLoop() { // Renombrado desde startKafkaConsumer
        try {
            while (true) {
                ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100)); // Espera nuevos mensajes
                for (ConsumerRecord<String, String> record : records) {
                    try {
                        // Deserializa el valor JSON en un objeto Taxi
                        Taxi taxiUpdate = objectMapper.readValue(record.value(), Taxi.class);
                        registeredTaxis.put(taxiUpdate.getTaxiId(), taxiUpdate); // Actualiza el estado del taxi en el mapa

                        System.out.printf("CentralApp Kafka: Recibida posición de taxi -> Key: %s, Taxi ID: %s, Ubicación: %s, Tópico: %s, Partición: %d, Offset: %d%n",
                                record.key(), taxiUpdate.getTaxiId(), taxiUpdate.getCurrentLocation(),
                                record.topic(), record.partition(), record.offset());

                    } catch (IOException e) {
                        System.err.println("CentralApp Kafka: Error deserializando JSON de posición de taxi para el valor: " + record.value() + " - " + e.getMessage());
                        // e.printStackTrace(); // Descomentar para ver el stack trace completo si es necesario depurar
                    }
                }
            }
        } catch (Exception e) {
            System.err.println("CentralApp Error en el consumidor Kafka: " + e.getMessage());
            e.printStackTrace();
        } finally {
            kafkaConsumer.close(); // Cierra el consumidor cuando el bucle termina
        }
    }

    // --- Métodos de gestión de clientes ---

    // Registra un cliente general (por ejemplo, Cliente) y almacena su handler
    public void registerClient(String clientId, ClientHandler handler) {
        connectedClients.put(clientId, handler);
        System.out.println("CentralApp: Cliente " + clientId + " registrado vía socket.");
    }

    // Registra un cliente Taxi y almacena su handler


    // Elimina un handler de cliente cuando se desconecta
    public void unregisterClient(String clientId) {
        connectedClients.remove(clientId);
        System.out.println("CentralApp: Cliente " + clientId + " eliminado del registro.");
    }

    // --- Lógica de gestión y asignación de viajes ---

    // Gestiona una nueva solicitud de viaje de un Cliente
    public void handleTripRequest(TripRequest tripRequest) {
        System.out.println("CentralApp: Recibida solicitud de viaje del cliente " + tripRequest.getCustomer().getCustomerId() + ": " + tripRequest);

        // Lógica simple para encontrar el taxi DISPONIBLE más cercano
        Optional<Taxi> availableTaxi = registeredTaxis.values().stream()
                .filter(taxi -> !taxi.isAssigned()) // Solo considera taxis que no estén en viaje
                .min(Comparator.comparingDouble(taxi ->
                        taxi.getCurrentLocation().distanceTo(tripRequest.getPickupLocation()))); // Busca el más cercano al punto de recogida

        if (availableTaxi.isPresent()) {
            Taxi assignedTaxi = availableTaxi.get();
            assignedTaxi.setAssigned(true); // Marca el taxi como asignado
            registeredTaxis.put(assignedTaxi.getTaxiId(), assignedTaxi); // Actualiza su estado en el mapa

            System.out.println("CentralApp: Asignando viaje " + tripRequest.getTripId() + " al taxi " + assignedTaxi.getTaxiId());

            // 1. Envía el objeto TripRequest al taxi asignado vía su socket
            ClientHandler taxiHandler = connectedClients.get(assignedTaxi.getTaxiId());
            if (taxiHandler != null) {
                try {
                    taxiHandler.sendObject(tripRequest); // Envía el objeto TripRequest
                    System.out.println("CentralApp: Solicitud de viaje enviada al taxi " + assignedTaxi.getTaxiId());
                } catch (IOException e) {
                    System.err.println("CentralApp: Error enviando la asignación al taxi " + assignedTaxi.getTaxiId() + ": " + e.getMessage());
                }
            } else {
                System.err.println("CentralApp: No se encontró el handler para el taxi asignado " + assignedTaxi.getTaxiId());
            }

            // 2. Envía confirmación al cliente
            ClientHandler customerHandler = connectedClients.get(tripRequest.getCustomer().getCustomerId());
            if (customerHandler != null) {
                try {
                    // Envía un mensaje de confirmación (puedes cambiarlo por un objeto más adelante)
                    customerHandler.sendObject(ProtocolConstants.CMD_TRIP_ASSIGNED + ":" + assignedTaxi.getTaxiId() + ":" + tripRequest.getTripId());
                    System.out.println("CentralApp: Confirmación de asignación enviada al cliente " + tripRequest.getCustomer().getCustomerId());
                } catch (IOException e) {
                    System.err.println("CentralApp: Error enviando la confirmación al cliente " + tripRequest.getCustomer().getCustomerId() + ": " + e.getMessage());
                }
            } else {
                System.err.println("CentralApp: No se encontró el handler para el cliente " + tripRequest.getCustomer().getCustomerId());
            }
        } else {
            System.out.println("CentralApp: No se encontró taxi disponible para la solicitud de viaje " + tripRequest.getTripId());
            // Opcionalmente, envía un mensaje de "no hay taxi disponible" al cliente
            ClientHandler customerHandler = connectedClients.get(tripRequest.getCustomer().getCustomerId());
            if (customerHandler != null) {
                try {
                    customerHandler.sendObject("NO_TAXI_AVAILABLE:" + tripRequest.getTripId());
                } catch (IOException e) {
                    System.err.println("CentralApp: Error notificando al cliente que no hay taxi: " + e.getMessage());
                }
            }
        }
    }

    // Gestiona el acuse de recibo de un taxi al recibir una asignación de viaje
    public void handleTaxiTripAcknowledged(String taxiId, String tripId) {
        System.out.println("CentralApp: El taxi " + taxiId + " ha confirmado el viaje " + tripId);
        // Aquí se puede implementar lógica adicional, por ejemplo, actualizar el estado del viaje en un mapa de viajes
    }

    // Gestiona la notificación de un taxi al completar un viaje
    public void handleTaxiTripCompleted(String taxiId) {
        Taxi taxi = registeredTaxis.get(taxiId);
        if (taxi != null) {
            taxi.setAssigned(false); // Marca el taxi como disponible de nuevo
            System.out.println("CentralApp: El taxi " + taxiId + " ha completado su viaje y ahora está disponible.");
        } else {
            System.err.println("CentralApp: Se recibió notificación de viaje completado de un taxi desconocido: " + taxiId);
        }
        // Aquí se puede añadir lógica adicional, por ejemplo, notificar al cliente que su viaje ha finalizado
    }

    // --- Gestión de recursos ---

    public void closeConnections() {
        try {
            if (serverSocket != null && !serverSocket.isClosed()) {
                serverSocket.close();
            }
            if (kafkaConsumer != null) {
                kafkaConsumer.close();
            }
            // Apaga el pool de hilos de forma ordenada
            clientThreadPool.shutdown();
            if (!clientThreadPool.awaitTermination(5, TimeUnit.SECONDS)) {
                clientThreadPool.shutdownNow(); // Fuerza el apagado si no termina de forma ordenada
            }
            System.out.println("CentralApp: Todas las conexiones cerradas y recursos liberados.");
        } catch (IOException | InterruptedException e) {
            System.err.println("Error cerrando los recursos de CentralApp: " + e.getMessage());
        }
    }

    public static void main(String[] args) {
        CentralApp app = new CentralApp();
        app.startServer(); // Inicia el servidor
    }

    public void registerTaxi(String taxiId, String clientIp, ClientHandler handler){
    }
}
