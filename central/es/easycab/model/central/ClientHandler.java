package es.easycab.model.central; // Corrected package name

import es.easycab.common.protocol.ProtocolConstants;
import es.easycab.common.model.TripRequest; // Import TripRequest


import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

public class ClientHandler implements Runnable {
    private Socket clientSocket;
    private CentralApp centralApp;
    private ObjectInputStream in;
    private ObjectOutputStream out;
    private String clientId; // Stores the ID of the connected client (Taxi or Customer)

    public ClientHandler(Socket socket, CentralApp centralApp) {
        this.clientSocket = socket;
        this.centralApp = centralApp;
        try {
            // IMPORTANT: Output stream MUST be created before input stream for ObjectOutputStream/ObjectInputStream
            // to avoid deadlocks.
            this.out = new ObjectOutputStream(clientSocket.getOutputStream());
            this.in = new ObjectInputStream(clientSocket.getInputStream());
        } catch (IOException e) {
            System.err.println("Error setting up streams for client handler: " + e.getMessage());
            closeResources();
        }
    }

    // Method to send an object back to the client over the socket
    public void sendObject(Object obj) throws IOException {
        out.writeObject(obj);
        out.flush(); // Ensure the object is sent immediately
    }

    // For debugging/logging, optional
    public Socket getClientSocket() {
        return clientSocket;
    }

    @Override
    public void run() {
        try {
            // First message received from client is expected to be its ID (e.g., "TAXI_ID:taxi-1234" or "CUSTOMER_ID:customer-5678")
            Object initialMsg = in.readObject();

            if (initialMsg instanceof String) {
                String idMsg = (String) initialMsg;
                if (idMsg.startsWith("TAXI_ID:")) {
                    this.clientId = idMsg.substring("TAXI_ID:".length());
                    centralApp.registerTaxi(this.clientId, clientSocket.getInetAddress().getHostAddress(), this);
                    sendObject(ProtocolConstants.CMD_ACK_CONNECTION + ":" + this.clientId); // Send ACK back to Taxi
                    System.out.println("ClientHandler: Taxi " + clientId + " connected via socket.");
                } else if (idMsg.startsWith("CUSTOMER_ID:")) {
                    this.clientId = idMsg.substring("CUSTOMER_ID:".length());
                    centralApp.registerClient(this.clientId, this);
                    sendObject(ProtocolConstants.CMD_ACK_CONNECTION + ":" + this.clientId); // Send ACK back to Customer
                    System.out.println("ClientHandler: Customer " + clientId + " connected via socket.");
                } else {
                    System.err.println("ClientHandler: Unknown initial message from " + clientSocket.getInetAddress() + ": " + idMsg);
                    closeResources();
                    return;
                }
            } else {
                System.err.println("ClientHandler: Initial message was not a String from " + clientSocket.getInetAddress());
                closeResources();
                return;
            }

            // Now, continuously listen for subsequent messages/objects from this client
            Object receivedObject;
            while ((receivedObject = in.readObject()) != null) {
                if (receivedObject instanceof String) {
                    String message = (String) receivedObject;
                    System.out.println("ClientHandler for " + clientId + ": Received string message -> " + message);

                    // Handle specific string-based commands from clients (e.g., from TaxiApp)
                    if (message.startsWith(ProtocolConstants.CMD_TRIP_ACKNOWLEDGED)) {
                        String[] parts = message.split(":");
                        if (parts.length >= 3) {
                            String taxiId = parts[1];
                            String tripId = parts[2];
                            centralApp.handleTaxiTripAcknowledged(taxiId, tripId);
                        }
                    } else if (message.startsWith(ProtocolConstants.CMD_TRIP_COMPLETED)) {
                        String[] parts = message.split(":");
                        if (parts.length >= 2) {
                            String taxiId = parts[1];
                            centralApp.handleTaxiTripCompleted(taxiId);
                        }
                    }
                    // Add more string command handling here if needed
                } else if (receivedObject instanceof TripRequest) {
                    // This is how Central receives a TripRequest object from CustomerApp
                    TripRequest tripRequest = (TripRequest) receivedObject;
                    System.out.println("ClientHandler for " + clientId + ": Received TripRequest object -> " + tripRequest);
                    centralApp.handleTripRequest(tripRequest); // Pass the TripRequest object to CentralApp
                } else {
                    System.out.println("ClientHandler for " + clientId + ": Received unknown object type: " + receivedObject.getClass().getName());
                }
            }
        } catch (IOException e) {
            // Client disconnected or socket error
            System.out.println("Client " + clientId + " disconnected from Central (socket error): " + e.getMessage());
        } catch (ClassNotFoundException e) {
            // Received an object that couldn't be cast or found
            System.err.println("Client " + clientId + " received unknown object type over socket: " + e.getMessage());
        } finally {
            // Clean up: remove client from connectedClients map
            if (clientId != null) {
                centralApp.unregisterClient(clientId);
            }
            closeResources();
        }
    }

    private void closeResources() {
        try {
            if (out != null) out.close();
            if (in != null) in.close();
            if (clientSocket != null && !clientSocket.isClosed()) clientSocket.close();
        } catch (IOException e) {
            System.err.println("Error closing client handler resources for " + clientId + ": " + e.getMessage());
        }
    }
}
