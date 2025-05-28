package es.easycab.common.model;

import java.io.Serializable;
import java.util.Objects;

public class Taxi implements Serializable {
    private static final long serialVersionUID = 1L;

    public String getTaxiId() {
        return this.id;
    }

    public boolean isAssigned() {
        return true;
    }

    public void setAssigned(boolean b) {

    }

    public enum TaxiStatus {
        AVAILABLE, // Listo para un viaje
        EN_ROUTE_TO_PICKUP, // Yendo a recoger al cliente
        EN_ROUTE_TO_DESTINATION, // Yendo al destino con el cliente
        STOPPED, // Detenido por alguna contingencia
        AT_BASE // En la base, esperando instrucciones
    }

    private String id;
    private Location currentLocation;
    private TaxiStatus status;
    private TripRequest currentTrip; // El viaje que está realizando actualmente

    public Taxi(String id, Location currentLocation) {
        this.id = id;
        this.currentLocation = currentLocation;
        this.status = TaxiStatus.AVAILABLE; // Estado inicial
    }

    // Getters y Setters
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Location getCurrentLocation() {
        return currentLocation;
    }

    public void setCurrentLocation(Location currentLocation) {
        this.currentLocation = currentLocation;
    }

    public TaxiStatus getStatus() {
        return status;
    }

    public void setStatus(TaxiStatus status) {
        this.status = status;
    }

    public TripRequest getCurrentTrip() {
        return currentTrip;
    }

    public void setCurrentTrip(TripRequest currentTrip) {
        this.currentTrip = currentTrip;
    }

    @Override
    public String toString() {
        return "Taxi{" +
                "id='" + id + '\'' +
                ", location=" + currentLocation +
                ", status=" + status +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Taxi taxi = (Taxi) o;
        return Objects.equals(id, taxi.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }
}