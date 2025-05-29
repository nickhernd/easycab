package es.easycab.model.customer;

import es.easycab.common.model.Customer;
import es.easycab.common.model.Location;
import es.easycab.common.model.TripRequest;
import es.easycab.common.protocol.ProtocolConstants;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class CustomerApp {
    private String customerId; // Usaremos un UUID para el ID del cliente
    private Customer customer;
    private Socket centralSocket;
    private ObjectOutputStream outToCentral;
    private ObjectInputStream inFromCentral;
    private Random random = new Random();
    private ScheduledExecutorService scheduler;

    public CustomerApp(String id) {
        this.customerId = id;
        // Inicializar la ubicación inicial del cliente
        Location initialLoc = generateRandomLocation();
        this.customer = new Customer(customerId, initialLoc);
        System.out.println("Customer " + customerId + ": Initialized at " + initialLoc);
    }

    public void connectToCentral() {
        try {
            centralSocket = new Socket(ProtocolConstants.LOCALHOST, ProtocolConstants.CENTRAL_PORT);
            outToCentral = new ObjectOutputStream(centralSocket.getOutputStream());
            inFromCentral = new ObjectInputStream(centralSocket.getInputStream());

            // 1. Enviar ID del cliente a la Central
            outToCentral.writeObject("CUSTOMER_ID:" + customerId);
            outToCentral.flush();
            System.out.println("Customer " + customerId + ": Connected to Central. Sent ID.");

            // 2. Esperar ACK de la Central
            Object response = inFromCentral.readObject();
            if (response instanceof String) {
                String msg = (String) response;
                if (msg.startsWith(ProtocolConstants.CMD_ACK_CONNECTION)) {
                    System.out.println("Customer " + customerId + ": Central says: " + msg);
                    // Iniciar la simulación de solicitud de viajes una vez conectado
                    startTripRequestSimulation();
                    // Iniciar hilo para escuchar comandos de la Central (opcional, pero buena
                    // práctica)
                    new Thread(this::listenForCentralCommands).start();
                } else {
                    System.err.println("Customer " + customerId + ": Failed to connect. Central response: " + msg);
                    closeConnections();
                }
            }
        } catch (IOException | ClassNotFoundException e) {
            System.err.println("Customer " + customerId + " connection error: " + e.getMessage());
            closeConnections();
        }
    }

    private void startTripRequestSimulation() {
        scheduler = Executors.newSingleThreadScheduledExecutor();
        // Generar una solicitud de viaje cada 5 a 15 segundos usando
        // scheduleWithFixedDelay
        scheduler.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                try {
                    // Antes de generar la nueva solicitud, actualizar la ubicación del cliente
                    // para simular que el cliente se ha movido o está en una nueva ubicación para
                    // pedir otro taxi.
                    customer.setCurrentLocation(generateRandomLocation());

                    Location pickupLoc = customer.getCurrentLocation(); // El cliente pide un taxi desde su ubicación
                                                                        // actual
                    Location destLoc = generateRandomLocation();

                    // Asegurarse de que el destino no sea el mismo que el origen
                    while (destLoc.equals(pickupLoc)) {
                        destLoc = generateRandomLocation();
                    }

                    // Usar un identificador de viaje basado en un UUID aleatorio
                    String tripId = UUID.randomUUID().toString();
                    TripRequest tripRequest = new TripRequest(
                            tripId, // ID único para el viaje
                            customer,
                            pickupLoc,
                            destLoc);

                    outToCentral.writeObject(tripRequest);
                    outToCentral.flush();
                    System.out.println("Customer " + customerId + ": Sent trip request: " + tripRequest);

                    // Random delay between 5 and 15 seconds before next request
                    Thread.sleep((random.nextInt(11) + 5) * 1000L);

                } catch (IOException e) {
                    System.err.println("Customer " + customerId + ": Error sending trip request: " + e.getMessage());
                    closeConnections();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }, 5, 0, TimeUnit.SECONDS); // El segundo parámetro no se usa porque el delay está en Thread.sleep
    }

    private Location generateRandomLocation() {
        return new Location(1 + random.nextInt(20), 1 + random.nextInt(20));
    }

    private void listenForCentralCommands() {
        try {
            while (true) {
                Object command = inFromCentral.readObject();
                if (command instanceof String) {
                    String cmd = (String) command;
                    // Aquí podrías procesar comandos como asignación confirmada,
                    // o si la central quiere enviar un mensaje al cliente.
                    System.out.println("Customer " + customerId + ": Received command from Central: " + cmd);
                }
                // Si la Central enviara un objeto Customer asignado, podrías manejarlo aquí
                // else if (command instanceof AssignedTaxiInfo) { ... }
            }
        } catch (IOException e) {
            System.out.println(
                    "Customer " + customerId + ": Disconnected from Central (command listener): " + e.getMessage());
        } catch (ClassNotFoundException e) {
            System.err.println("Customer " + customerId + ": Received unknown command object type: " + e.getMessage());
        } finally {
            closeConnections();
        }
    }

    public void closeConnections() {
        try {
            if (scheduler != null) {
                scheduler.shutdown();
                try {
                    if (!scheduler.awaitTermination(1, TimeUnit.SECONDS)) {
                        scheduler.shutdownNow();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    scheduler.shutdownNow();
                }
            }
            if (outToCentral != null)
                outToCentral.close();
            if (inFromCentral != null)
                inFromCentral.close();
            if (centralSocket != null)
                centralSocket.close();
            System.out.println("Customer " + customerId + ": Connections closed.");
        } catch (IOException e) {
            System.err.println("Error closing customer " + customerId + " resources: " + e.getMessage());
        }
    }

    public static void main(String[] args) {
        String customerId = UUID.randomUUID().toString().substring(0, 8); // Un UUID más corto para mostrar
        CustomerApp customerApp = new CustomerApp(customerId);
        customerApp.connectToCentral();
        // Asegurarse de cerrar las conexiones al finalizar la aplicación
        Runtime.getRuntime().addShutdownHook(new Thread(customerApp::closeConnections));
    }
}