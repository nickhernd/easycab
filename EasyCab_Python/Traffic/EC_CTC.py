from flask import Flask, jsonify, request
import requests

app = Flask(__name__)

CITY = "Madrid"  # Ciudad por defecto
OPENWEATHER_API_KEY = "f23280b7dd0ea5985e196bac7746037b"

@app.route('/set_city', methods=['POST'])
def set_city():
    global CITY
    data = request.get_json()
    CITY = data.get("city", "Madrid")
    return jsonify({"result": "ok", "city": CITY})

@app.route('/traffic_status', methods=['GET'])
def traffic_status():
    # Consulta OpenWeather
    url = f"https://api.openweathermap.org/data/2.5/weather?q={CITY}&appid={OPENWEATHER_API_KEY}&units=metric"
    try:
        r = requests.get(url)
        temp = r.json()["main"]["temp"]
        if temp < 0:
            return jsonify({"status": "KO", "city": CITY, "temp": temp})
        else:
            return jsonify({"status": "OK", "city": CITY, "temp": temp})
    except Exception as e:
        return jsonify({"status": "UNKNOWN", "error": str(e)}), 500

if __name__ == "__main__":
    app.run(port=5001)