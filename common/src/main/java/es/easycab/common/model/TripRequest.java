package es.easycab.common.model;

import java.io.Serializable;
import java.util.Objects;

public class TripRequest implements Serializable {
    private static final long serialVersionUID = 1L;

    private String tripId;
    private String requestId;
    private Customer customer;
    private Location pickupLocation;
    private Location destinationLocation;
    private String assignedTaxiId; // ID del taxi asignado a esta petición

    public TripRequest(String requestId, Customer customer, Location pickupLocation, Location destinationLocation) {
        this.tripId = tripId;
        this.requestId = requestId;
        this.customer = customer;
        this.pickupLocation = pickupLocation;
        this.destinationLocation = destinationLocation;
    }

    public String getTripId() {
        return tripId;
    }

    // Getters y Setters
    public String getRequestId() {
        return requestId;
    }

    public void setRequestId(String requestId) {
        this.requestId = requestId;
    }

    public Customer getCustomer() {
        return customer;
    }

    public void setCustomer(Customer customer) {
        this.customer = customer;
    }

    public Location getPickupLocation() {
        return pickupLocation;
    }

    public void setPickupLocation(Location pickupLocation) {
        this.pickupLocation = pickupLocation;
    }

    public Location getDestinationLocation() {
        return destinationLocation;
    }

    public void setDestinationLocation(Location destinationLocation) {
        this.destinationLocation = destinationLocation;
    }

    public String getAssignedTaxiId() {
        return assignedTaxiId;
    }

    public void setAssignedTaxiId(String assignedTaxiId) {
        this.assignedTaxiId = assignedTaxiId;
    }

    @Override
    public String toString() {
        return "TripRequest{" +
                "requestId='" + requestId + '\'' +
                ", customerId=" + (customer != null ? customer.getId() : "N/A") +
                ", pickup=" + pickupLocation +
                ", destination=" + destinationLocation +
                ", assignedTaxi='" + (assignedTaxiId != null ? assignedTaxiId : "None") + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TripRequest that = (TripRequest) o;
        return Objects.equals(requestId, that.requestId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(requestId);
    }
}