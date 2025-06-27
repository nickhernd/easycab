import sqlite3
import os
import random

# Adjusted path to place easycab.db inside a 'data' subdirectory within 'Central'
# os.path.dirname(__file__) es el directorio actual del script (Central/)
DATABASE_FILE = os.path.join(os.path.dirname(__file__), 'data', 'easycab.db')

def initialize_database():
    """
    Inicializa la base de datos SQLite, creando las tablas necesarias si no existen.
    Crea las tablas para taxis, clientes, ubicaciones y para el log de auditoría.
    """
    print("Inicializando la base de datos SQLite...")
    print(f"Archivo de base de datos: {DATABASE_FILE}")
    try:
        # Ensure the directory for the database file exists
        os.makedirs(os.path.dirname(DATABASE_FILE), exist_ok=True) # Esta línea crea la carpeta 'data' si no existe

        conn = sqlite3.connect(DATABASE_FILE)
        conn.row_factory = sqlite3.Row # Permite acceder a las columnas por nombre
        cursor = conn.cursor()

        # Crear tabla para los taxis
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS taxis (
                taxi_id TEXT PRIMARY KEY,
                x INTEGER NOT NULL,
                y INTEGER NOT NULL,
                status TEXT NOT NULL,
                service_id TEXT,
                current_destination_x INTEGER,
                current_destination_y INTEGER,
                initial_x INTEGER,
                initial_y INTEGER
            )
        ''')

        # Crear tabla para los clientes
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS customers (
                client_id TEXT PRIMARY KEY,
                destination_id TEXT,
                assigned_taxi_id TEXT,
                status TEXT NOT NULL, -- idle, pending, assigned, picked_up, completed, denied, cancelled
                origin_x INTEGER NOT NULL,
                origin_y INTEGER NOT NULL
            )
        ''')

        # Crear tabla para las ubicaciones/destinos
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS locations (
                location_id TEXT PRIMARY KEY,
                x INTEGER NOT NULL,
                y INTEGER NOT NULL
            )
        ''')

        # Crear tabla para el log de auditoría
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS audit_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                who TEXT NOT NULL,
                ip TEXT NOT NULL,
                action TEXT NOT NULL,
                description TEXT
            )
        ''')

        # Opcional: Insertar algunas ubicaciones por defecto si la tabla está vacía
        cursor.execute("SELECT COUNT(*) FROM locations")
        if cursor.fetchone()[0] == 0:
            print("Insertando ubicaciones por defecto en la tabla 'locations'...")
            default_locations = [
                ("A", 0, 0), ("B", 19, 0), ("C", 0, 19), ("D", 19, 19),
                ("E", 10, 10), ("F", 5, 15), ("G", 15, 5)
            ]
            cursor.executemany("INSERT INTO locations (location_id, x, y) VALUES (?, ?, ?)", default_locations)
            print("Ubicaciones por defecto insertadas.")


        conn.commit()
        conn.close()
        print(f"Base de datos SQLite '{DATABASE_FILE}' inicializada correctamente con tablas de taxis, clientes, ubicaciones y auditoría.")
    except sqlite3.Error as e:
        print(f"Error al inicializar la base de datos: {e}")
        exit(1)

if __name__ == "__main__":
    initialize_database()
    print("Base de datos inicializada correctamente.")

