package es.easycab.model.central; // Corrected package name

import es.easycab.common.model.Taxi;
import es.easycab.common.model.TripRequest;
import es.easycab.common.protocol.ProtocolConstants;
import com.fasterxml.jackson.databind.ObjectMapper; // For JSON (make sure common and central POMs have it)
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
    private ExecutorService clientThreadPool; // Manages threads for socket connections
    private KafkaConsumer<String, String> kafkaConsumer;
    private ObjectMapper objectMapper; // Jackson object for JSON serialization/deserialization

    // Maps to manage connected clients and their states
    // Stores the ClientHandler for each connected client (Taxi or Customer) by their ID
    public Map<String, ClientHandler> connectedClients = new ConcurrentHashMap<>();
    // Stores the latest state of all registered taxis, updated by Kafka messages
    private Map<String, Taxi> registeredTaxis = new ConcurrentHashMap<>();

    public CentralApp() {
        this.clientThreadPool = Executors.newFixedThreadPool(10);
        this.objectMapper = new ObjectMapper();
        setupKafkaConsumer(); // Initialize Kafka consumer
    }

    private void setupKafkaConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "easycab-central-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()); // Still String for now, will deserialize manually
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // Start reading from the beginning
        this.kafkaConsumer = new KafkaConsumer<>(props);
        this.kafkaConsumer.subscribe(Collections.singletonList(TAXI_POSITIONS_TOPIC));
        System.out.println("CentralApp: Kafka Consumer subscribed to topic '" + TAXI_POSITIONS_TOPIC + "'");
    }

    public void startServer() { // Renamed from startSocketServer for clarity
        try {
            serverSocket = new ServerSocket(ProtocolConstants.CENTRAL_PORT);
            System.out.println("CentralApp: Socket Server listening on port " + ProtocolConstants.CENTRAL_PORT);

            // Start Kafka consumer in a separate thread
            new Thread(this::startKafkaConsumerLoop).start(); // Renamed for clarity

            // Accept new client connections in the main loop
            while (true) {
                Socket clientSocket = serverSocket.accept();
                System.out.println("CentralApp: New client connected: " + clientSocket.getInetAddress().getHostAddress());
                ClientHandler clientHandler = new ClientHandler(clientSocket, this);
                clientThreadPool.submit(clientHandler); // Assign handler to a thread from the pool
            }
        } catch (IOException e) {
            System.err.println("CentralApp Socket Server error: " + e.getMessage());
        } finally {
            closeConnections(); // Ensure resources are closed on shutdown
        }
    }

    private void startKafkaConsumerLoop() { // Renamed from startKafkaConsumer
        try {
            while (true) {
                ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100)); // Poll for new messages
                for (ConsumerRecord<String, String> record : records) {
                    try {
                        // Deserialize the JSON string value into a Taxi object
                        Taxi taxiUpdate = objectMapper.readValue(record.value(), Taxi.class);
                        registeredTaxis.put(taxiUpdate.getTaxiId(), taxiUpdate); // Update the taxi's current state in our map

                        System.out.printf("CentralApp Kafka: Received taxi-position -> Key: %s, Taxi ID: %s, Location: %s, Topic: %s, Partition: %d, Offset: %d%n",
                                record.key(), taxiUpdate.getTaxiId(), taxiUpdate.getCurrentLocation(),
                                record.topic(), record.partition(), record.offset());

                    } catch (IOException e) {
                        System.err.println("CentralApp Kafka: Error deserializing taxi-position JSON for value: " + record.value() + " - " + e.getMessage());
                        // e.printStackTrace(); // Uncomment for full stack trace if debugging is needed
                    }
                }
            }
        } catch (Exception e) {
            System.err.println("CentralApp Kafka Consumer error: " + e.getMessage());
            e.printStackTrace();
        } finally {
            kafkaConsumer.close(); // Close consumer when loop exits
        }
    }

    // --- Client Management Methods ---

    // Registers a general client (e.g., Customer) and stores its handler
    public void registerClient(String clientId, ClientHandler handler) {
        connectedClients.put(clientId, handler);
        System.out.println("CentralApp: Client " + clientId + " registered via socket.");
    }

    // Registers a Taxi client and stores its handler


    // Removes a client handler when they disconnect
    public void unregisterClient(String clientId) {
        connectedClients.remove(clientId);
        System.out.println("CentralApp: Client " + clientId + " unregistered.");
    }

    // --- Trip Management and Assignment Logic ---

    // Handles a new trip request from a Customer
    public void handleTripRequest(TripRequest tripRequest) {
        System.out.println("CentralApp: Received Trip Request from Customer " + tripRequest.getCustomer().getCustomerId() + ": " + tripRequest);

        // Simple logic to find the closest AVAILABLE taxi
        Optional<Taxi> availableTaxi = registeredTaxis.values().stream()
                .filter(taxi -> !taxi.isAssigned()) // Only consider taxis not currently on a trip
                .min(Comparator.comparingDouble(taxi ->
                        taxi.getCurrentLocation().distanceTo(tripRequest.getPickupLocation()))); // Find closest to pickup

        if (availableTaxi.isPresent()) {
            Taxi assignedTaxi = availableTaxi.get();
            assignedTaxi.setAssigned(true); // Mark the taxi as assigned
            registeredTaxis.put(assignedTaxi.getTaxiId(), assignedTaxi); // Update its state in the map

            System.out.println("CentralApp: Assigning Trip " + tripRequest.getTripId() + " to Taxi " + assignedTaxi.getTaxiId());

            // 1. Send the TripRequest object to the assigned Taxi via its socket
            ClientHandler taxiHandler = connectedClients.get(assignedTaxi.getTaxiId());
            if (taxiHandler != null) {
                try {
                    taxiHandler.sendObject(tripRequest); // Send the actual TripRequest object
                    System.out.println("CentralApp: Sent TripRequest to Taxi " + assignedTaxi.getTaxiId());
                } catch (IOException e) {
                    System.err.println("CentralApp: Error sending trip assignment to Taxi " + assignedTaxi.getTaxiId() + ": " + e.getMessage());
                }
            } else {
                System.err.println("CentralApp: Taxi handler not found for assigned taxi " + assignedTaxi.getTaxiId());
            }

            // 2. Send confirmation to the Customer
            ClientHandler customerHandler = connectedClients.get(tripRequest.getCustomer().getCustomerId());
            if (customerHandler != null) {
                try {
                    // Send a confirmation message (you can make this an object later)
                    customerHandler.sendObject(ProtocolConstants.CMD_TRIP_ASSIGNED + ":" + assignedTaxi.getTaxiId() + ":" + tripRequest.getTripId());
                    System.out.println("CentralApp: Sent trip assignment confirmation to Customer " + tripRequest.getCustomer().getCustomerId());
                } catch (IOException e) {
                    System.err.println("CentralApp: Error sending assignment confirmation to Customer " + tripRequest.getCustomer().getCustomerId() + ": " + e.getMessage());
                }
            } else {
                System.err.println("CentralApp: Customer handler not found for customer " + tripRequest.getCustomer().getCustomerId());
            }
        } else {
            System.out.println("CentralApp: No available taxi found for Trip Request " + tripRequest.getTripId());
            // Optionally, send a "no taxi available" message back to the customer
            ClientHandler customerHandler = connectedClients.get(tripRequest.getCustomer().getCustomerId());
            if (customerHandler != null) {
                try {
                    customerHandler.sendObject("NO_TAXI_AVAILABLE:" + tripRequest.getTripId());
                } catch (IOException e) {
                    System.err.println("CentralApp: Error notifying customer about no taxi: " + e.getMessage());
                }
            }
        }
    }

    // Handles acknowledgement from a Taxi that it received a trip assignment
    public void handleTaxiTripAcknowledged(String taxiId, String tripId) {
        System.out.println("CentralApp: Taxi " + taxiId + " acknowledged trip " + tripId);
        // Implement further logic here, e.g., update trip status in a dedicated trips map
    }

    // Handles notification from a Taxi that it completed a trip
    public void handleTaxiTripCompleted(String taxiId) {
        Taxi taxi = registeredTaxis.get(taxiId);
        if (taxi != null) {
            taxi.setAssigned(false); // Mark taxi as available again
            System.out.println("CentralApp: Taxi " + taxiId + " completed its trip and is now available.");
        } else {
            System.err.println("CentralApp: Received trip completed from unknown taxi ID: " + taxiId);
        }
        // Additional logic could be added here, e.g., notifying the customer that their trip is finished
    }

    // --- Resource Management ---

    public void closeConnections() {
        try {
            if (serverSocket != null && !serverSocket.isClosed()) {
                serverSocket.close();
            }
            if (kafkaConsumer != null) {
                kafkaConsumer.close();
            }
            // Shut down the thread pool gracefully
            clientThreadPool.shutdown();
            if (!clientThreadPool.awaitTermination(5, TimeUnit.SECONDS)) {
                clientThreadPool.shutdownNow(); // Force shutdown if not terminated gracefully
            }
            System.out.println("CentralApp: All connections closed and resources released.");
        } catch (IOException | InterruptedException e) {
            System.err.println("Error closing CentralApp resources: " + e.getMessage());
        }
    }

    public static void main(String[] args) {
        CentralApp app = new CentralApp();
        app.startServer(); // Start the server
    }

    public void registerTaxi(String taxiId, String clientIp, ClientHandler handler){
    }
}
