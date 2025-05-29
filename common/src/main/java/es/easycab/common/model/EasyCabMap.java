package es.easycab.common.model;

import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;

public class EasyCabMap {
    public static final int MAP_SIZE = 20;
    private Cell[][] grid;
    private ConcurrentHashMap<String, Taxi> taxis;
    private ConcurrentHashMap<String, Location> customers;
    private ConcurrentHashMap<String, Location> destinations;

    public EasyCabMap() {
        this.grid = new Cell[MAP_SIZE][MAP_SIZE];
        this.taxis = new ConcurrentHashMap<>();
        this.customers = new ConcurrentHashMap<>();
        this.destinations = new ConcurrentHashMap<>();
        initializeGrid();
    }

    private void initializeGrid() {
        for (int i = 0; i < MAP_SIZE; i++) {
            for (int j = 0; j < MAP_SIZE; j++) {
                grid[i][j] = new Cell();
            }
        }
    }

    // Esférico: wrap de coordenadas
    private int wrap(int v) {
        return (v + MAP_SIZE) % MAP_SIZE;
    }

    public void updateTaxi(String taxiId, int x, int y, boolean moving) {
        x = wrap(x);
        y = wrap(y);
        Taxi taxi = new Taxi(taxiId, moving);
        taxis.put(taxiId, taxi);
        grid[y][x].taxi = taxi;
    }

    public void removeTaxi(String taxiId) {
        Taxi taxi = taxis.remove(taxiId);
        if (taxi != null) {
            for (int i = 0; i < MAP_SIZE; i++) {
                for (int j = 0; j < MAP_SIZE; j++) {
                    if (grid[i][j].taxi != null && grid[i][j].taxi.id.equals(taxiId)) {
                        grid[i][j].taxi = null;
                    }
                }
            }
        }
    }

    public void updateCustomer(String customerId, int x, int y) {
        x = wrap(x);
        y = wrap(y);
        Location loc = new Location(x, y);
        customers.put(customerId, loc);
        grid[y][x].customerId = customerId;
    }

    public void removeCustomer(String customerId) {
        Location loc = customers.remove(customerId);
        if (loc != null) {
            grid[loc.getY()][loc.getX()].customerId = null;
        }
    }

    public void addDestination(String destId, int x, int y) {
        x = wrap(x);
        y = wrap(y);
        Location loc = new Location(x, y);
        destinations.put(destId, loc);
        grid[y][x].destinationId = destId;
    }

    public void removeDestination(String destId) {
        Location loc = destinations.remove(destId);
        if (loc != null) {
            grid[loc.getY()][loc.getX()].destinationId = null;
        }
    }

    // Representación de la celda según reglas
    public String getCellDisplay(int x, int y) {
        x = wrap(x);
        y = wrap(y);
        Cell cell = grid[y][x];
        if (cell.taxi != null) {
            // Taxi: id y color (verde=moving, rojo=stopped)
            String color = cell.taxi.moving ? "[VERDE]" : "[ROJO]";
            return color + cell.taxi.id;
        }
        if (cell.destinationId != null) {
            // Destino: mayúscula, fondo azul
            return "[AZUL]" + cell.destinationId.toUpperCase();
        }
        if (cell.customerId != null) {
            // Cliente: minúscula, fondo amarillo
            return "[AMARILLO]" + cell.customerId.toLowerCase();
        }
        return " "; // Nada
    }

    // Mapa completo en string
    public String toConsoleString() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < MAP_SIZE; i++) {
            for (int j = 0; j < MAP_SIZE; j++) {
                sb.append(getCellDisplay(j, i)).append(" ");
            }
            sb.append("\n");
        }
        return sb.toString();
    }

    // Clases auxiliares
    public static class Cell {
        Taxi taxi;
        String customerId;
        String destinationId;
    }

    public static class Taxi {
        String id;
        boolean moving; // true=verde, false=rojo
        public Taxi(String id, boolean moving) {
            this.id = id;
            this.moving = moving;
        }
    }

    public static class Location {
        private int x, y;
        public Location(int x, int y) { this.x = x; this.y = y; }
        public int getX() { return x; }
        public int getY() { return y; }
    }
}