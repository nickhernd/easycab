import asyncio
import json
import websockets
from kafka import KafkaConsumer
from aiohttp import web 
import os 

# --- Configuración ---
KAFKA_BROKER = 'localhost:9092' 
WEBSOCKET_HOST = '0.0.0.0'       
WEBSOCKET_PORT = 8765            

HTTP_HOST = '0.0.0.0'            
HTTP_PORT = 8080                 

KAFKA_TOPIC = 'map_updates'      

print(f"Iniciando EasyCab Web Visualization Proxy...")
print(f"Conectando a Kafka en {KAFKA_BROKER} (Tópico: {KAFKA_TOPIC})")
print(f"Servidor WebSocket escuchando en ws://{WEBSOCKET_HOST}:{WEBSOCKET_PORT}")
print(f"Servidor HTTP para la visualización en http://{HTTP_HOST}:{HTTP_PORT}")


connected_clients = set()

async def register(websocket):
    connected_clients.add(websocket)
    print(f"Cliente conectado: {websocket.remote_address}. Total clientes: {len(connected_clients)}")

async def unregister(websocket):
    connected_clients.discard(websocket) 
    print(f"Cliente desconectado: {websocket.remote_address}. Total clientes: {len(connected_clients)}")

async def kafka_consumer_handler():
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=[KAFKA_BROKER],
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='latest', 
        group_id='map_viewer_proxy_group' 
    )

    print(f"Consumidor de Kafka para '{KAFKA_TOPIC}' iniciado.")

    for message in consumer:
        # print(f"Recibido de Kafka: {message.value}") # <-- DESCOMENTA ESTA LÍNEA PARA VER SI RECIBE DE KAFKA
        message_json = json.dumps(message.value)
        
        clients_to_send = list(connected_clients) 
        
        if clients_to_send:
            send_tasks = []
            for client in clients_to_send:
                if not client.closed: 
                    send_tasks.append(client.send(message_json))
            
            results = await asyncio.gather(*send_tasks, return_exceptions=True)

            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    await unregister(clients_to_send[i]) 

async def websocket_server_handler(websocket, path):
    await register(websocket)
    try:
        await websocket.wait_closed()
    finally:
        await unregister(websocket)

# --- Handler para servir index.html directamente en la raíz ---
async def index_handler(request):
    script_dir = os.path.dirname(__file__)
    web_client_dir = os.path.join(script_dir, 'web_client')
    
    # Comprobar si index.html existe en la ruta esperada
    index_path = os.path.join(web_client_dir, 'index.html')
    if not os.path.exists(index_path):
        print(f"ERROR HTTP: index.html no encontrado en {index_path}")
        raise web.HTTPNotFound() # O web.HTTPForbidden() si quieres ser más específico

    return web.FileResponse(index_path)

async def main():
    # 1. Configurar y iniciar el servidor WebSocket
    ws_server = await websockets.serve(websocket_server_handler, WEBSOCKET_HOST, WEBSOCKET_PORT)

    # 2. Configurar y iniciar el servidor HTTP para servir los archivos estáticos (HTML, CSS, JS)
    script_dir = os.path.dirname(__file__)
    web_client_dir = os.path.abspath(os.path.join(script_dir, 'web_client')) # Usar ruta absoluta para mayor robustez

    print(f"DEBUG: El servidor buscará archivos web en el directorio: {web_client_dir}") # <-- DEBUG PRINT

    if not os.path.isdir(web_client_dir):
        print(f"ERROR: El directorio '{web_client_dir}' NO se encontró. Asegúrate de la estructura de carpetas: 'WebVisualization/web_client'.")
        # No salimos de aquí para que los otros servicios puedan seguir, pero el HTTP no funcionará
        # return 

    app = web.Application()
    # Servir index.html directamente en la raíz '/'
    app.router.add_get('/', index_handler)
    # Servir los demás archivos estáticos (CSS, JS) desde el mismo directorio 'web_client'
    # Esto significa que en index.html, los enlaces deben ser directos: style.css, script.js
    # No es necesario /static/ si los enlaces en HTML son directos.
    app.router.add_static('/', web_client_dir, name='static_files') 

    runner = web.AppRunner(app)
    await runner.setup()
    http_site = web.TCPSite(runner, HTTP_HOST, HTTP_PORT)
    await http_site.start()
    print(f"DEBUG: Servidor HTTP iniciado en http://{HTTP_HOST}:{HTTP_PORT}") # <-- DEBUG PRINT

    # 3. Iniciar la tarea del consumidor de Kafka en segundo plano
    kafka_task = asyncio.create_task(kafka_consumer_handler())

    print("\nTodos los servicios están activos. Accede a la visualización en:")
    print(f"-> http://{HTTP_HOST}:{HTTP_PORT}")
    print("\nPulsa Ctrl+C para detener todos los servicios.")

    try:
        await ws_server.serve_forever() 
    except asyncio.CancelledError:
        pass 
    finally:
        print("\nCerrando servicios...")
        await ws_server.close() 
        await runner.cleanup() 
        
        kafka_task.cancel() 
        try:
            await kafka_task 
        except asyncio.CancelledError:
            pass
        print("Todos los servicios han sido cerrados.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nPrograma terminado por el usuario (Ctrl+C).")
    except Exception as e:
        print(f"Error inesperado en el programa principal: {e}")