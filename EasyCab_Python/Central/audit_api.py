from flask import Flask, jsonify
import threading
import time

app = Flask(__name__)

# Auditoría en memoria (importada del módulo principal)
AUDIT_LOG = []

def add_audit_event(event):
    AUDIT_LOG.append(event)
    # Limita el tamaño del log en memoria (opcional)
    if len(AUDIT_LOG) > 1000:
        AUDIT_LOG.pop(0)

@app.route('/audit', methods=['GET'])
def get_audit_log():
    return jsonify(AUDIT_LOG)

def run_audit_api():
    app.run(host="0.0.0.0", port=6000, debug=False, use_reloader=False)

# Para pruebas manuales
if __name__ == "__main__":
    app.run(port=6000)