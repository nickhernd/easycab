from flask import Flask, request, jsonify

app = Flask(__name__)

REGISTERED_TAXIS = set()

@app.route('/register', methods=['POST'])
def register_taxi():
    data = request.get_json()
    taxi_id = data.get("taxi_id")
    if taxi_id is None:
        return jsonify({"status": "KO", "msg": "Falta taxi_id"}), 400
    REGISTERED_TAXIS.add(taxi_id)
    return jsonify({"status": "OK", "msg": f"Taxi {taxi_id} registrado"})

@app.route('/unregister', methods=['POST'])
def unregister_taxi():
    data = request.get_json()
    taxi_id = data.get("taxi_id")
    if taxi_id is None:
        return jsonify({"status": "KO", "msg": "Falta taxi_id"}), 400
    REGISTERED_TAXIS.discard(taxi_id)
    return jsonify({"status": "OK", "msg": f"Taxi {taxi_id} dado de baja"})

@app.route('/is_registered/<taxi_id>', methods=['GET'])
def is_registered(taxi_id):
    return jsonify({"registered": taxi_id in REGISTERED_TAXIS})

if __name__ == "__main__":
    app.run(port=5002, ssl_context=('cert.pem', 'key.pem'))