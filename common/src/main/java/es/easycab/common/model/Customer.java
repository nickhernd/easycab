package es.easycab.common.model;

import java.io.Serializable;
import java.util.Objects;
import java.util.UUID; // Para generar un ID único

public class Customer implements Serializable {
    private static final long serialVersionUID = 1L;

    private String id;
    private String name; // Opcional: puedes añadir un nombre si lo necesitas
    private Location currentLocation; // Podría ser útil para saber dónde está el cliente

    public Customer(String id, String name, Location currentLocation) {
        this.id = id;
        this.name = name;
        this.currentLocation = currentLocation;
    }

    public Customer(String name, Location currentLocation) {
        this.id = UUID.randomUUID().toString(); // Genera un ID único si no se proporciona
        this.name = name;
        this.currentLocation = currentLocation;
    }

    // Constructor vacío para Jackson/serialización si se necesita en el futuro
    public Customer() {
    }

    // Getters
    public String getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public Location getCurrentLocation() {
        return currentLocation;
    }

    // Setters
    public void setId(String id) {
        this.id = id;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setCurrentLocation(Location currentLocation) {
        this.currentLocation = currentLocation;
    }

    @Override
    public String toString() {
        return "Customer{" +
                "id='" + id + '\'' +
                ", name='" + (name != null ? name : "N/A") + '\'' +
                ", currentLocation=" + currentLocation +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        Customer customer = (Customer) o;
        return Objects.equals(id, customer.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }
}