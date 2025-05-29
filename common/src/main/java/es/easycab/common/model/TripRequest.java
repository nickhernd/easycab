package es.easycab.common.model;

import java.io.Serializable;

public class TripRequest implements Serializable {
    private static final long serialVersionUID = 1L;

    private String requestId;
    private Customer customer;
    private Location pickupLocation;
    private Location destinationLocation;

    public TripRequest(String requestId, Customer customer, Location pickupLocation, Location destinationLocation) {
        this.requestId = requestId;
        this.customer = customer;
        this.pickupLocation = pickupLocation;
        this.destinationLocation = destinationLocation;
    }

    public String getRequestId() {
        return requestId;
    }

    public Customer getCustomer() {
        return customer;
    }

    public Location getPickupLocation() {
        return pickupLocation;
    }

    public Location getDestinationLocation() {
        return destinationLocation;
    }

    @Override
    public String toString() {
        return "TripRequest{" +
                "requestId='" + requestId + '\'' +
                ", customerId='" + (customer != null ? customer.getId() : "N/A") + '\'' +
                ", pickupLocation=" + pickupLocation +
                ", destinationLocation=" + destinationLocation +
                '}';
    }
}
