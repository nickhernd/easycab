package es.easycab.model.customer;

import es.easycab.common.model.Customer;
import es.easycab.common.model.Location;
import es.easycab.common.model.TripRequest;
import es.easycab.common.protocol.ProtocolConstants;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.UUID;
import java.util.Scanner;

public class CustomerApp {
    private String customerId;
    private Customer customer;
    private Socket centralSocket;
    private ObjectOutputStream outToCentral;
    private ObjectInputStream inFromCentral;

    public CustomerApp(String id, Location initialLocation, String name) {
        this.customerId = id;
        this.customer = new Customer(id, name, initialLocation);
    }

    public void connectToCentral() {
        try {
            centralSocket = new Socket(ProtocolConstants.LOCALHOST, ProtocolConstants.CENTRAL_PORT);
            outToCentral = new ObjectOutputStream(centralSocket.getOutputStream());
            inFromCentral = new ObjectInputStream(centralSocket.getInputStream());

            // Enviar ID para que la Central sepa quién se conecta
            outToCentral.writeObject("CUSTOMER_ID:" + customerId);
            outToCentral.flush();

            // Recibir confirmación de conexión
            Object response = inFromCentral.readObject();
            if (response instanceof String) {
                String msg = (String) response;
                if (msg.startsWith(ProtocolConstants.CMD_ACK_CONNECTION)) {
                    System.out.println("Customer " + customerId + ": Connected to Central. Central says: " + msg);
                    // Ahora podemos enviar una solicitud de viaje
                    sendTripRequest();
                } else {
                    System.err.println("Customer " + customerId + ": Failed to connect. Central response: " + msg);
                    closeConnections();
                    return;
                }
            }

            // Hilo para escuchar respuestas de la Central (ej. asignación de taxi, estado de viaje)
            new Thread(this::listenForCentralResponses).start();


        } catch (IOException | ClassNotFoundException e) {
            System.err.println("Customer " + customerId + " connection error: " + e.getMessage());
            closeConnections();
        }
    }

    private void sendTripRequest() {
        try {
            Scanner scanner = new Scanner(System.in);
            System.out.println("Customer " + customerId + ": Enter destination X coordinate:");
            int destX = scanner.nextInt();
            System.out.println("Customer " + customerId + ": Enter destination Y coordinate:");
            int destY = scanner.nextInt();
            scanner.nextLine(); // Consume newline

            Location destination = new Location(destX, destY);
            TripRequest request = new TripRequest(
                    "trip-" + UUID.randomUUID().toString().substring(0, 4),
                    this.customer, // Envía el objeto Customer
                    this.customer.getCurrentLocation(),
                    destination
            );
            System.out.println("Customer " + customerId + ": Sending trip request: " + request);

            outToCentral.writeObject(ProtocolConstants.CMD_TRIP_REQUEST); // Primero el comando
            outToCentral.writeObject(request); // Luego el objeto
            outToCentral.flush();

        } catch (IOException e) {
            System.err.println("Customer " + customerId + ": Error sending trip request: " + e.getMessage());
        }
    }

    private void listenForCentralResponses() {
        try {
            while (true) {
                Object response = inFromCentral.readObject();
                if (response instanceof String) {
                    String msg = (String) response;
                    System.out.println("Customer " + customerId + ": Received response from Central: " + msg);
                    // Aquí manejarás las respuestas de la Central, como la asignación de taxi, etc.
                } else if (response instanceof TripRequest) {
                    TripRequest updatedRequest = (TripRequest) response;
                    System.out.println("Customer " + customerId + ": Trip request updated: " + updatedRequest);
                    // Esto podría ser la confirmación de la asignación de taxi
                }
            }
        } catch (IOException e) {
            System.out.println("Customer " + customerId + ": Disconnected from Central (response listener): " + e.getMessage());
        } catch (ClassNotFoundException e) {
            System.err.println("Customer " + customerId + ": Received unknown object type from Central: " + e.getMessage());
        } finally {
            closeConnections();
        }
    }

    public void closeConnections() {
        try {
            if (outToCentral != null) outToCentral.close();
            if (inFromCentral != null) inFromCentral.close();
            if (centralSocket != null) centralSocket.close();
            System.out.println("Customer " + customerId + ": Connections closed.");
        } catch (IOException e) {
            System.err.println("Error closing customer " + customerId + " resources: " + e.getMessage());
        }
    }

    public static void main(String[] args) {
        String id = "customer-" + UUID.randomUUID().toString().substring(0, 4);
        // Ubicación inicial del cliente (podría ser aleatoria)
        Location initialLoc = new Location(5, 5);
        String name = "User-" + UUID.randomUUID().toString().substring(0, 4);
        CustomerApp customer = new CustomerApp(id, initialLoc, name);
        customer.connectToCentral();

        Runtime.getRuntime().addShutdownHook(new Thread(customer::closeConnections));
    }
}