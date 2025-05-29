package es.easycab.common.model;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

public class Taxi implements Serializable {
    private static final long serialVersionUID = 1L;

    public enum TaxiStatus {
        AVAILABLE, // Listo para un viaje
        EN_ROUTE_TO_PICKUP, // Yendo a recoger al cliente
        EN_ROUTE_TO_DESTINATION, // Yendo al destino con el cliente
        STOPPED, // Detenido por alguna contingencia
        AT_BASE // En la base, esperando instrucciones
    }

    public enum TaxiColor {
        GREEN, // En movimiento (o asignado a un viaje, pero no necesariamente moviéndose
               // instantáneamente)
        RED // Parado o en posición final, disponible o en base
    }

    private int id; // [0..99]
    private int x; // [1..20]
    private int y; // [1..20]
    private TaxiStatus status;
    private TaxiColor color;
    private TripRequest currentTrip;
    private List<Sensor> sensors; // Puedes mantener esto o inicializar una lista vacía si no usas sensores reales
                                  // por ahora

    // Constructor actualizado para aceptar Location directamente
    public Taxi(int id, Location initialLocation) {
        this.id = id;
        this.x = initialLocation.getX();
        this.y = initialLocation.getY();
        this.status = TaxiStatus.AVAILABLE;
        this.color = TaxiColor.RED; // Por defecto, empieza parado/disponible
        this.currentTrip = null;
        // Si necesitas inicializar la lista de sensores, hazlo aquí:
        // this.sensors = new ArrayList<>();
    }

    // Constructor original si prefieres mantenerlo, pero el de arriba es más
    // coherente con TaxiApp
    public Taxi(int id, int x, int y) {
        this.id = id;
        this.x = x;
        this.y = y;
        this.status = TaxiStatus.AVAILABLE;
        this.color = TaxiColor.RED;
        this.currentTrip = null;
    }

    // Getters y Setters
    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    // Este getter devuelve un objeto Location
    public Location getCurrentLocation() {
        return new Location(x, y);
    }

    // Este setter acepta un objeto Location
    public void setCurrentLocation(Location newLocation) {
        setPosition(newLocation.getX(), newLocation.getY());
    }

    public int getX() {
        return x;
    }

    public int getY() {
        return y;
    }

    public void setPosition(int x, int y) {
        // Validación de rango (opcional si ya la manejas en la lógica de movimiento)
        if (x >= 1 && x <= 20 && y >= 1 && y <= 20) {
            this.x = x;
            this.y = y;
        } else {
            System.err.println("Error: Intento de establecer posición fuera de los límites para Taxi " + id + ": (" + x
                    + "," + y + ")");
        }
    }

    public TaxiStatus getStatus() {
        return status;
    }

    public void setStatus(TaxiStatus status) {
        this.status = status;
        updateColor(); // Asegúrate de que el color se actualice con el estado
    }

    public TaxiColor getColor() {
        return color;
    }

    private void updateColor() {
        if (status == TaxiStatus.EN_ROUTE_TO_PICKUP || status == TaxiStatus.EN_ROUTE_TO_DESTINATION) {
            this.color = TaxiColor.GREEN;
        } else {
            this.color = TaxiColor.RED;
        }
    }

    public TripRequest getCurrentTrip() {
        return currentTrip;
    }

    public void setCurrentTrip(TripRequest currentTrip) {
        this.currentTrip = currentTrip;
        // Podrías poner lógica aquí para cambiar el estado inicial del viaje
        if (currentTrip != null) {
            setStatus(TaxiStatus.EN_ROUTE_TO_PICKUP); // O al estado inicial de un viaje
        } else {
            setStatus(TaxiStatus.AVAILABLE); // Si el viaje es nulo, está disponible
        }
    }

    public List<Sensor> getSensors() {
        return sensors;
    }

    public void setSensors(List<Sensor> sensors) {
        this.sensors = sensors;
    }

    // Movimiento a coordenada adyacente (ya implementado en TaxiApp.java)
    // Este método ya no es estrictamente necesario aquí si TaxiApp gestiona el
    // movimiento directo
    // pero podría ser útil si Taxi tuviera su propia lógica de navegación interna
    // más compleja.
    /*
     * public boolean moveTo(int newX, int newY) {
     * if (Math.abs(newX - x) <= 1 && Math.abs(newY - y) <= 1 &&
     * newX >= 1 && newX <= 20 && newY >= 1 && newY <= 20) {
     * this.x = newX;
     * this.y = newY;
     * return true;
     * }
     * return false;
     * }
     */

    // Simulación de sensores reportando anomalías (mantener, es útil)
    public void reportAnomaly(String anomaly) {
        System.out.println("Taxi " + id + " anomaly: " + anomaly);
        // Aquí se notificaría al Digital Engine o a la Central si fuera necesario
    }

    @Override
    public String toString() {
        return "Taxi{" +
                "id=" + id +
                ", position=(" + x + "," + y + ")" +
                ", status=" + status +
                ", color=" + color +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        Taxi taxi = (Taxi) o;
        return id == taxi.id;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }
}

// Interfaz Sensor (mantener)
interface Sensor extends Serializable {
    void detect(Taxi taxi);
}