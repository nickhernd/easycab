package es.easycab.common.model;

import java.io.Serializable;
import java.util.Objects;

public class Customer implements Serializable {
    private static final long serialVersionUID = 1L;

    private String id;
    private String name;
    private Location currentLocation; // La ubicación desde donde pide el taxi
    private boolean hasActiveRequest; // Para saber si tiene una petición pendiente

    public Customer(String id, String name, Location currentLocation) {
        this.id = id;
        this.name = name;
        this.currentLocation = currentLocation;
        this.hasActiveRequest = false;
    }

    // Getters y Setters
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Location getCurrentLocation() {
        return currentLocation;
    }

    public void setCurrentLocation(Location currentLocation) {
        this.currentLocation = currentLocation;
    }

    public boolean hasActiveRequest() {
        return hasActiveRequest;
    }

    public void setHasActiveRequest(boolean hasActiveRequest) {
        this.hasActiveRequest = hasActiveRequest;
    }

    @Override
    public String toString() {
        return "Customer{" +
                "id='" + id + '\'' +
                ", name='" + name + '\'' +
                ", location=" + currentLocation +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Customer customer = (Customer) o;
        return Objects.equals(id, customer.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    public String getCustomerId() {
        return this.id;
    }
}