package es.easycab.taxi;

import es.easycab.common.model.Location;
import es.easycab.common.model.Taxi;
import es.easycab.common.model.TripRequest; // Import the TripRequest class
import es.easycab.common.protocol.ProtocolConstants;
import com.fasterxml.jackson.databind.ObjectMapper; // For JSON serialization/deserialization
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class TaxiApp {
    private String taxiId;
    private Taxi taxi; // Use a Taxi object to hold ID and location
    private Socket centralSocket;
    private ObjectOutputStream outToCentral;
    private ObjectInputStream inFromCentral;
    private KafkaProducer<String, String> kafkaProducer;
    private ObjectMapper objectMapper; // Jackson ObjectMapper for JSON

    // State for the taxi
    private boolean isAssignedToTrip = false;
    private TripRequest currentTrip = null;

    public TaxiApp(String id, Location initialLocation) {
        this.taxiId = id;
        this.taxi = new Taxi(id, initialLocation); // Initialize the Taxi object
        this.objectMapper = new ObjectMapper(); // Initialize ObjectMapper
        setupKafkaProducer();
    }

    private void setupKafkaProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()); // Still String, as we'll manually convert to JSON String
        this.kafkaProducer = new KafkaProducer<>(props);
        System.out.println("Taxi " + taxiId + ": Kafka Producer initialized.");
    }

    public void connectToCentral() {
        try {
            centralSocket = new Socket(ProtocolConstants.LOCALHOST, ProtocolConstants.CENTRAL_PORT);
            outToCentral = new ObjectOutputStream(centralSocket.getOutputStream());
            inFromCentral = new ObjectInputStream(centralSocket.getInputStream());

            // Send ID for Central to know who's connecting
            outToCentral.writeObject("TAXI_ID:" + taxiId);
            outToCentral.flush();

            // Receive connection confirmation
            Object response = inFromCentral.readObject();
            if (response instanceof String) {
                String msg = (String) response;
                if (msg.startsWith(ProtocolConstants.CMD_ACK_CONNECTION)) {
                    System.out.println("Taxi " + taxiId + ": Connected to Central. Central says: " + msg);
                } else {
                    System.err.println("Taxi " + taxiId + ": Failed to connect. Central response: " + msg);
                    closeConnections();
                    return;
                }
            }

            // Start a thread to simulate movement and publish to Kafka
            new Thread(this::simulateMovementAndPublish).start();

            // Start a thread to listen for commands from Central (via socket)
            new Thread(this::listenForCentralCommands).start();

        } catch (IOException | ClassNotFoundException e) {
            System.err.println("Taxi " + taxiId + " connection error: " + e.getMessage());
            closeConnections();
        }
    }

    private void simulateMovementAndPublish() {
        while (true) {
            // Simulate movement based on trip status or just random for now
            if (isAssignedToTrip && currentTrip != null) {
                // For simplicity, move towards pickup, then towards destination
                Location target = currentTrip.getPickupLocation();
                if (taxi.getCurrentLocation().equals(target)) {
                    // Reached pickup, now go to destination
                    target = currentTrip.getDestinationLocation();
                }

                // Simple movement: move one step closer to target
                int currentX = taxi.getCurrentLocation().getX();
                int currentY = taxi.getCurrentLocation().getY();

                if (currentX < target.getX()) {
                    currentX++;
                } else if (currentX > target.getX()) {
                    currentX--;
                }

                if (currentY < target.getY()) {
                    currentY++;
                } else if (currentY > target.getY()) {
                    currentY--;
                }
                taxi.setCurrentLocation(new Location(currentX, currentY));

                System.out.println("Taxi " + taxiId + ": Moving towards " + target + ". Current: " + taxi.getCurrentLocation());

                // Check if destination is reached (for now, simply destination of the trip)
                if (taxi.getCurrentLocation().equals(currentTrip.getDestinationLocation())) {
                    System.out.println("Taxi " + taxiId + ": Trip completed! Reached destination " + currentTrip.getDestinationLocation());
                    isAssignedToTrip = false;
                    currentTrip = null;
                    // Inform Central that trip is completed (via socket or another Kafka topic)
                    try {
                        outToCentral.writeObject(ProtocolConstants.CMD_TRIP_COMPLETED + ":" + taxiId);
                        outToCentral.flush();
                    } catch (IOException e) {
                        System.err.println("Error sending trip completed status: " + e.getMessage());
                    }
                }

            } else {
                // If not on a trip, just random small movement or stay put
                // For now, let's keep the simple +1,+1 for idle taxis
                taxi.setCurrentLocation(new Location(taxi.getCurrentLocation().getX() + 1, taxi.getCurrentLocation().getY() + 1));
                System.out.println("Taxi " + taxiId + ": Idle, simulated location: " + taxi.getCurrentLocation());
            }

            // Publish the entire Taxi object as JSON to Kafka
            try {
                String taxiJson = objectMapper.writeValueAsString(taxi); // Convert Taxi object to JSON string
                kafkaProducer.send(new ProducerRecord<>("taxi-positions", taxiId, taxiJson), (metadata, exception) -> {
                    if (exception == null) {
                        // System.out.println("Taxi " + taxiId + ": Position sent to Kafka: " + taxiJson);
                    } else {
                        System.err.println("Taxi " + taxiId + ": Failed to send position to Kafka: " + exception.getMessage());
                    }
                });
            } catch (IOException e) {
                System.err.println("Taxi " + taxiId + ": Error serializing Taxi object to JSON: " + e.getMessage());
            }

            try {
                TimeUnit.SECONDS.sleep(5); // Send position every 5 seconds
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                System.err.println("Taxi " + taxiId + ": Movement simulation interrupted.");
                break;
            }
        }
    }

    private void listenForCentralCommands() {
        try {
            while (true) {
                Object command = inFromCentral.readObject();
                if (command instanceof String) {
                    String cmd = (String) command;
                    System.out.println("Taxi " + taxiId + ": Received direct command from Central: " + cmd);
                    // Handle specific string commands if any
                } else if (command instanceof TripRequest) {
                    TripRequest assignedTrip = (TripRequest) command;
                    System.out.println("Taxi " + taxiId + ": Assigned to new trip! Details: " + assignedTrip);
                    this.currentTrip = assignedTrip;
                    this.isAssignedToTrip = true;
                    // You might want to send an acknowledgement back to Central here
                    outToCentral.writeObject(ProtocolConstants.CMD_TRIP_ACKNOWLEDGED + ":" + taxiId + ":" + assignedTrip.getTripId());
                    outToCentral.flush();
                }
            }
        } catch (IOException e) {
            System.out.println("Taxi " + taxiId + ": Disconnected from Central (command listener): " + e.getMessage());
        } catch (ClassNotFoundException e) {
            System.err.println("Taxi " + taxiId + ": Received unknown command object type: " + e.getMessage());
        } finally {
            closeConnections();
        }
    }

    public void closeConnections() {
        try {
            if (kafkaProducer != null) kafkaProducer.close();
            if (outToCentral != null) outToCentral.close();
            if (inFromCentral != null) inFromCentral.close();
            if (centralSocket != null) centralSocket.close();
            System.out.println("Taxi " + taxiId + ": Connections closed.");
        } catch (IOException e) {
            System.err.println("Error closing taxi " + taxiId + " resources: " + e.getMessage());
        }
    }

    public static void main(String[] args) {
        String id = "taxi-" + UUID.randomUUID().toString().substring(0, 4);
        Location initialLoc = new Location(10, 10); // Start taxis at a different location
        TaxiApp taxi = new TaxiApp(id, initialLoc);
        taxi.connectToCentral();

        Runtime.getRuntime().addShutdownHook(new Thread(taxi::closeConnections));
    }
}