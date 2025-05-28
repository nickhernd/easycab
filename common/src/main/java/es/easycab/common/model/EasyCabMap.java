package es.easycab.common.model;

import java.io.Serializable; // Podría no ser necesario si solo es para uso interno
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;

public class EasyCabMap {
    private int width;
    private int height;
    private char[][] grid; // 'T' para taxi, 'C' para cliente, '.' para vacío, '#' para obstáculo

    // Un mapa para la posición de los taxis para fácil acceso
    private ConcurrentHashMap<String, Location> taxiLocations;
    private ConcurrentHashMap<String, Location> customerLocations;

    public EasyCabMap(int width, int height) {
        this.width = width;
        this.height = height;
        this.grid = new char[height][width];
        initializeGrid();
        this.taxiLocations = new ConcurrentHashMap<>();
        this.customerLocations = new ConcurrentHashMap<>();
    }

    private void initializeGrid() {
        for (int i = 0; i < height; i++) {
            for (int j = 0; j < width; j++) {
                grid[i][j] = '.'; // Inicializa todo como vacío
            }
        }
        // Opcional: añadir algunos obstáculos fijos para probar
        // grid[5][5] = '#';
    }

    public int getWidth() {
        return width;
    }

    public int getHeight() {
        return height;
    }

    public void updateTaxiLocation(String taxiId, Location oldLoc, Location newLoc) {
        if (oldLoc != null && isValidLocation(oldLoc)) {
            grid[oldLoc.getY()][oldLoc.getX()] = '.'; // Limpia la posición antigua
        }
        if (isValidLocation(newLoc)) {
            grid[newLoc.getY()][newLoc.getX()] = 'T'; // Marca la nueva posición del taxi
            taxiLocations.put(taxiId, newLoc);
        }
    }

    public void updateCustomerLocation(String customerId, Location oldLoc, Location newLoc) {
        if (oldLoc != null && isValidLocation(oldLoc)) {
            grid[oldLoc.getY()][oldLoc.getX()] = '.'; // Limpia la posición antigua
        }
        if (isValidLocation(newLoc)) {
            grid[newLoc.getY()][newLoc.getX()] = 'C'; // Marca la nueva posición del cliente
            customerLocations.put(customerId, newLoc);
        }
    }

    public void removeTaxi(String taxiId, Location loc) {
        if (isValidLocation(loc)) {
            grid[loc.getY()][loc.getX()] = '.';
        }
        taxiLocations.remove(taxiId);
    }

    public void removeCustomer(String customerId, Location loc) {
        if (isValidLocation(loc)) {
            grid[loc.getY()][loc.getX()] = '.';
        }
        customerLocations.remove(customerId);
    }

    public boolean isValidLocation(Location loc) {
        return loc.getX() >= 0 && loc.getX() < width &&
                loc.getY() >= 0 && loc.getY() < height;
    }

    public char getCell(int x, int y) {
        if (isValidLocation(new Location(x, y))) {
            return grid[y][x];
        }
        return ' '; // Fuera de límites
    }

    public ConcurrentHashMap<String, Location> getTaxiLocations() {
        return taxiLocations;
    }

    public ConcurrentHashMap<String, Location> getCustomerLocations() {
        return customerLocations;
    }

    // Método para una representación en String, útil para la consola
    public String toConsoleString() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < height; i++) {
            for (int j = 0; j < width; j++) {
                sb.append(grid[i][j]).append(" ");
            }
            sb.append("\n");
        }
        return sb.toString();
    }
}