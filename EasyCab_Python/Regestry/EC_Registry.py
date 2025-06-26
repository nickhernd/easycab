from flask import Flask, request, jsonify
import sqlite3
import os

app = Flask(__name__)

# Ruta al archivo de la base de datos SQLite (asume que está en ../Central/easycab.db)
# Ajusta esta ruta si tu estructura de carpetas es diferente para el Registry
script_dir = os.path.dirname(__file__)
DATABASE_FILE = os.path.abspath(os.path.join(script_dir, '..', 'Central', 'easycab.db'))

def get_db_connection():
    """Establece una conexión a la base de datos SQLite."""
    conn = sqlite3.connect(DATABASE_FILE)
    conn.row_factory = sqlite3.Row # Permite acceder a las columnas por nombre
    return conn

@app.route('/register', methods=['POST'])
def register_taxi():
    """
    Registra un taxi en la base de datos.
    Si el taxi ya existe, actualiza su estado.
    Asigna una posición inicial aleatoria si es un nuevo registro.
    """
    data = request.get_json()
    taxi_id = data.get("taxi_id")
    if taxi_id is None:
        return jsonify({"status": "KO", "msg": "Falta taxi_id"}), 400

    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT taxi_id FROM taxis WHERE taxi_id = ?", (taxi_id,))
        existing_taxi = cursor.fetchone()

        if existing_taxi:
            # Si el taxi ya existe, actualizar su estado a 'free' y service_id a None
            cursor.execute(
                """UPDATE taxis SET status = ?, service_id = ?, current_destination_x = ?, current_destination_y = ? WHERE taxi_id = ?""",
                ("free", None, None, None, taxi_id)
            )
            msg = f"Taxi {taxi_id} actualizado (ya registrado)."
        else:
            # Si es un nuevo taxi, insertarlo con una posición inicial aleatoria y estado 'free'
            # Asumiendo un mapa de 20x20 como en EC_Central
            initial_x = random.randint(0, 19)
            initial_y = random.randint(0, 19)
            cursor.execute(
                """INSERT INTO taxis (taxi_id, x, y, status, service_id, current_destination_x, current_destination_y, initial_x, initial_y)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (taxi_id, initial_x, initial_y, "free", None, None, None, initial_x, initial_y)
            )
            msg = f"Taxi {taxi_id} registrado por primera vez en ({initial_x},{initial_y})."
        
        conn.commit()
        return jsonify({"status": "OK", "msg": msg}), 200
    except sqlite3.Error as e:
        print(f"Error en la base de datos al registrar/actualizar taxi: {e}")
        return jsonify({"status": "KO", "msg": f"Error interno: {e}"}), 500
    finally:
        conn.close()

@app.route('/unregister', methods=['POST'])
def unregister_taxi():
    """Da de baja un taxi de la base de datos."""
    data = request.get_json()
    taxi_id = data.get("taxi_id")
    if taxi_id is None:
        return jsonify({"status": "KO", "msg": "Falta taxi_id"}), 400
    
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("DELETE FROM taxis WHERE taxi_id = ?", (taxi_id,))
        if cursor.rowcount > 0:
            conn.commit()
            return jsonify({"status": "OK", "msg": f"Taxi {taxi_id} dado de baja"}), 200
        else:
            return jsonify({"status": "KO", "msg": f"Taxi {taxi_id} no encontrado"}), 404
    except sqlite3.Error as e:
        print(f"Error en la base de datos al dar de baja taxi: {e}")
        return jsonify({"status": "KO", "msg": f"Error interno: {e}"}), 500
    finally:
        conn.close()

@app.route('/is_registered/<taxi_id>', methods=['GET'])
def is_registered(taxi_id):
    """Verifica si un taxi está registrado en la base de datos."""
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT taxi_id FROM taxis WHERE taxi_id = ?", (taxi_id,))
        taxi = cursor.fetchone()
        return jsonify({"registered": taxi is not None}), 200
    except sqlite3.Error as e:
        print(f"Error en la base de datos al verificar registro de taxi: {e}")
        return jsonify({"status": "KO", "msg": f"Error interno: {e}"}), 500
    finally:
        conn.close()

if __name__ == "__main__":
    try:
        app.run(host="0.0.0.0", port=5002, ssl_context=('cert.pem', 'key.pem'))
    except FileNotFoundError:
        print("Advertencia: cert.pem o key.pem no encontrados. Se requiere SSL para el Registry.")
        print("Intentando ejecutar el Registry sin SSL (SOLO PARA DESARROLLO/PRUEBAS):")
        app.run(host="0.0.0.0", port=5002)
    except Exception as e:
        print(f"Error al iniciar EC_Registry: {e}")
