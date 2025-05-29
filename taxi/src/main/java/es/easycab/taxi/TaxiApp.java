package es.easycab.taxi;

import es.easycab.common.model.Location;
import es.easycab.common.model.Taxi;
import es.easycab.common.model.TripRequest;
import es.easycab.common.protocol.ProtocolConstants;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class TaxiApp {
    private Taxi taxi; // Ahora usamos la nueva clase Taxi
    private Socket centralSocket;
    private ObjectOutputStream outToCentral;
    private ObjectInputStream inFromCentral;
    private KafkaProducer<String, String> kafkaProducer;
    private ObjectMapper objectMapper;
    private Random random = new Random();

    public TaxiApp(int id, Location initialLocation) {
        // El constructor de Taxi.java ahora es (int id, int x, int y)
        this.taxi = new Taxi(id, initialLocation.getX(), initialLocation.getY());
        this.objectMapper = new ObjectMapper();
        setupKafkaProducer();
    }

    private void setupKafkaProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        this.kafkaProducer = new KafkaProducer<>(props);
        System.out.println("Taxi " + taxi.getId() + ": Kafka Producer initialized.");
    }

    public void connectToCentral() {
        try {
            centralSocket = new Socket(ProtocolConstants.LOCALHOST, ProtocolConstants.CENTRAL_PORT);
            outToCentral = new ObjectOutputStream(centralSocket.getOutputStream());
            inFromCentral = new ObjectInputStream(centralSocket.getInputStream());

            // Enviamos el ID del taxi a la Central
            outToCentral.writeObject("TAXI_ID:" + taxi.getId());
            outToCentral.flush();

            Object response = inFromCentral.readObject();
            if (response instanceof String) {
                String msg = (String) response;
                if (msg.startsWith(ProtocolConstants.CMD_ACK_CONNECTION)) {
                    System.out.println("Taxi " + taxi.getId() + ": Connected to Central. Central says: " + msg);
                } else {
                    System.err.println("Taxi " + taxi.getId() + ": Failed to connect. Central response: " + msg);
                    closeConnections();
                    return;
                }
            }
            new Thread(this::simulateMovementAndPublish).start();
            new Thread(this::listenForCentralCommands).start();
        } catch (IOException | ClassNotFoundException e) {
            System.err.println("Taxi " + taxi.getId() + " connection error: " + e.getMessage());
            closeConnections();
        }
    }

    private void simulateMovementAndPublish() {
        while (true) {
            boolean moved = false;
            // Estado del taxi: ¿tiene un viaje asignado?
            if (taxi.getCurrentTrip() != null) {
                TripRequest currentTrip = taxi.getCurrentTrip();
                Location currentLoc = new Location(taxi.getX(), taxi.getY()); // Obtener ubicación actual del taxi

                Location targetLocation;
                // Si el taxi aún no ha llegado al punto de recogida
                if (!currentLoc.equals(currentTrip.getPickupLocation())
                        && taxi.getStatus() == Taxi.TaxiStatus.EN_ROUTE_TO_PICKUP) {
                    targetLocation = currentTrip.getPickupLocation();
                    System.out.println("Taxi " + taxi.getId() + ": Moving towards pickup: " + targetLocation
                            + ". Current: " + currentLoc);
                }
                // Si el taxi ha llegado al punto de recogida y no ha llegado al destino
                else if (currentLoc.equals(currentTrip.getPickupLocation())
                        && taxi.getStatus() == Taxi.TaxiStatus.EN_ROUTE_TO_PICKUP) {
                    // Ha llegado al punto de recogida, ahora va al destino
                    taxi.setStatus(Taxi.TaxiStatus.EN_ROUTE_TO_DESTINATION);
                    System.out.println("Taxi " + taxi.getId() + ": Picked up customer! Moving towards destination: "
                            + currentTrip.getDestinationLocation());
                    targetLocation = currentTrip.getDestinationLocation();
                }
                // Si el taxi está en ruta al destino
                else if (taxi.getStatus() == Taxi.TaxiStatus.EN_ROUTE_TO_DESTINATION) {
                    targetLocation = currentTrip.getDestinationLocation();
                    System.out.println("Taxi " + taxi.getId() + ": Moving towards destination: " + targetLocation
                            + ". Current: " + currentLoc);
                }
                // Caso por defecto (puede que el viaje haya terminado, pero el bucle aún no lo
                // ha detectado)
                else {
                    targetLocation = null;
                    System.out.println("Taxi " + taxi.getId() + ": In undefined trip state. Current: " + currentLoc);
                }

                if (targetLocation != null) {
                    Location next = nextStepTowards(currentLoc, targetLocation);
                    if (!currentLoc.equals(next)) {
                        taxi.setPosition(next.getX(), next.getY());
                        moved = true;
                    }
                }

                // Check for trip completion
                if (new Location(taxi.getX(), taxi.getY()).equals(currentTrip.getDestinationLocation())
                        && taxi.getStatus() == Taxi.TaxiStatus.EN_ROUTE_TO_DESTINATION) {
                    System.out.println("Taxi " + taxi.getId() + ": Trip completed! Reached destination "
                            + currentTrip.getDestinationLocation());
                    taxi.setCurrentTrip(null); // Borrar el viaje actual
                    taxi.setStatus(Taxi.TaxiStatus.AVAILABLE); // Poner el taxi como disponible
                    try {
                        outToCentral.writeObject(ProtocolConstants.CMD_TRIP_COMPLETED + ":" + taxi.getId());
                        outToCentral.flush();
                    } catch (IOException e) {
                        System.err.println("Error sending trip completed status: " + e.getMessage());
                    }
                }
            } else {
                // Movimiento ocioso: moverse aleatoriamente a una celda adyacente o quedarse
                // quieto
                Location currentLoc = new Location(taxi.getX(), taxi.getY());
                Location next = randomAdjacent(currentLoc);
                if (!currentLoc.equals(next)) {
                    taxi.setPosition(next.getX(), next.getY());
                    moved = true;
                }
                // Asegurarse de que el estado sea AVAILABLE si no hay viaje asignado
                if (taxi.getStatus() != Taxi.TaxiStatus.AVAILABLE) {
                    taxi.setStatus(Taxi.TaxiStatus.AVAILABLE);
                }
                System.out.println("Taxi " + taxi.getId() + ": Idle, simulated location: " + taxi.getX() + ","
                        + taxi.getY() + ". Status: " + taxi.getStatus());
            }

            // Simular anomalía del sensor (aleatoriamente)
            if (random.nextInt(20) == 0) { // 5% de probabilidad
                String anomaly = randomAnomaly();
                try {
                    // Reportar la anomalía usando el método de Taxi.java
                    taxi.reportAnomaly(anomaly);
                    outToCentral.writeObject("SENSOR_ALERT:" + taxi.getId() + ":" + anomaly);
                    outToCentral.flush();
                } catch (IOException e) {
                    System.err.println("Taxi " + taxi.getId() + ": Error reporting anomaly: " + e.getMessage());
                }
            }

            // Publicar solo si se movió o si el estado cambió
            // La clase TaxiStatusDTO es solo para serializar a JSON
            if (moved || taxi.getColor() != Taxi.TaxiColor.RED /* o GREEN, si el estado cambia */) {
                try {
                    // Creamos un DTO para enviar por Kafka, ya que la clase Taxi completa puede
                    // tener dependencias circulares
                    TaxiStatusDTO statusDTO = new TaxiStatusDTO(taxi.getId(), taxi.getColor().name().toLowerCase(),
                            new Location(taxi.getX(), taxi.getY()));
                    String taxiJson = objectMapper.writeValueAsString(statusDTO);
                    kafkaProducer.send(new ProducerRecord<>("taxi-positions", String.valueOf(taxi.getId()), taxiJson));
                } catch (IOException e) {
                    System.err.println("Taxi " + taxi.getId() + ": Error serializing TaxiStatusDTO object to JSON: "
                            + e.getMessage());
                }
            }

            try {
                TimeUnit.SECONDS.sleep(2);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                System.err.println("Taxi " + taxi.getId() + ": Movement simulation interrupted.");
                break;
            }
        }
    }

    private Location nextStepTowards(Location from, Location to) {
        int x = from.getX(), y = from.getY();
        int tx = to.getX(), ty = to.getY();
        int nx = x, ny = y;

        if (x < tx)
            nx++;
        else if (x > tx)
            nx--;

        if (y < ty)
            ny++;
        else if (y > ty)
            ny--;

        // Asegurarse de que las coordenadas estén dentro del rango [1,20]
        nx = Math.max(1, Math.min(20, nx));
        ny = Math.max(1, Math.min(20, ny));

        return new Location(nx, ny);
    }

    private Location randomAdjacent(Location loc) {
        int x = loc.getX(), y = loc.getY();
        // Incluye la opción de no moverse (0,0)
        int[][] dirs = { { 1, 0 }, { -1, 0 }, { 0, 1 }, { 0, -1 }, { 0, 0 } };
        int[] dir = dirs[random.nextInt(dirs.length)];
        int nx = Math.max(1, Math.min(20, x + dir[0]));
        int ny = Math.max(1, Math.min(20, y + dir[1]));
        return new Location(nx, ny);
    }

    private String randomAnomaly() {
        String[] anomalies = {
                "Semaforo en rojo", "Persona cruzando", "Vehiculo cercano", "Obstaculo en via"
        };
        return anomalies[random.nextInt(anomalies.length)];
    }

    private void listenForCentralCommands() {
        try {
            while (true) {
                Object command = inFromCentral.readObject();
                if (command instanceof String) {
                    String cmd = (String) command;
                    System.out.println("Taxi " + taxi.getId() + ": Received direct command from Central: " + cmd);
                } else if (command instanceof TripRequest) {
                    TripRequest assignedTrip = (TripRequest) command;
                    System.out.println("Taxi " + taxi.getId() + ": Assigned to new trip! Details: " + assignedTrip);
                    taxi.setCurrentTrip(assignedTrip); // Asignar el viaje al objeto Taxi
                    // Cambiar el estado del taxi a EN_ROUTE_TO_PICKUP
                    taxi.setStatus(Taxi.TaxiStatus.EN_ROUTE_TO_PICKUP);

                    outToCentral.writeObject(ProtocolConstants.CMD_TRIP_ACKNOWLEDGED + ":" + taxi.getId() + ":"
                            + assignedTrip.getTripId());
                    outToCentral.flush();
                }
            }
        } catch (IOException e) {
            System.out.println(
                    "Taxi " + taxi.getId() + ": Disconnected from Central (command listener): " + e.getMessage());
        } catch (ClassNotFoundException e) {
            System.err.println("Taxi " + taxi.getId() + ": Received unknown command object type: " + e.getMessage());
        } finally {
            closeConnections();
        }
    }

    public void closeConnections() {
        try {
            if (kafkaProducer != null)
                kafkaProducer.close();
            if (outToCentral != null)
                outToCentral.close();
            if (inFromCentral != null)
                inFromCentral.close();
            if (centralSocket != null)
                centralSocket.close();
            System.out.println("Taxi " + taxi.getId() + ": Connections closed.");
        } catch (IOException e) {
            System.err.println("Error closing taxi " + taxi.getId() + " resources: " + e.getMessage());
        }
    }

    public static void main(String[] args) {
        int id = new Random().nextInt(100); // [0..99]
        Location initialLoc = new Location(1 + new Random().nextInt(20), 1 + new Random().nextInt(20));
        TaxiApp taxiApp = new TaxiApp(id, initialLoc);
        taxiApp.connectToCentral();
        Runtime.getRuntime().addShutdownHook(new Thread(taxiApp::closeConnections));
    }

    // Clase auxiliar para la serialización del estado a Kafka (DTO)
    // Es buena práctica no serializar el objeto de dominio completo si no es
    // necesario,
    // o si podría causar problemas de serialización (ej. referencias circulares).
    static class TaxiStatusDTO {
        public int id;
        public String estado; // Esto mapeará al color, que representa el estado visual
        public Location posicion;

        public TaxiStatusDTO(int id, String estado, Location posicion) {
            this.id = id;
            this.estado = estado;
            this.posicion = posicion;
        }

        // Getters para que Jackson pueda serializar
        public int getId() {
            return id;
        }

        public String getEstado() {
            return estado;
        }

        public Location getPosicion() {
            return posicion;
        }
    }
}