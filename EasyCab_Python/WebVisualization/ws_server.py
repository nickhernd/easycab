import asyncio
import websockets
import json
import logging
import os

logging.basicConfig(level=logging.INFO, format='[%(name)s] %(message)s')
logger = logging.getLogger('WebSocketServer')

connected_clients = set()

async def broadcast_message(message):
    if not connected_clients:
        logger.debug("No hay clientes públicos conectados para enviar el mensaje.")
        return

    message_str = json.dumps(message)
    disconnected_clients = set()
    for websocket in connected_clients:
        try:
            await websocket.send(message_str)
            logger.debug(f"Mensaje enviado a {websocket.remote_address}: {message_str[:50]}...")
        except websockets.exceptions.ConnectionClosed:
            logger.info(f"Cliente público desconectado al intentar enviar: {websocket.remote_address}")
            disconnected_clients.add(websocket)
        except Exception as e:
            logger.error(f"Error al enviar mensaje a cliente público {websocket.remote_address}: {e}")
            disconnected_clients.add(websocket)

    for client in disconnected_clients:
        connected_clients.discard(client)

async def handle_tcp_message(reader, writer):
    peername = writer.get_extra_info('peername')
    logger.info(f"Conexión TCP IPC recibida desde {peername}")

    try:
        data = await reader.readuntil(b'\n')
        message_str = data.decode('utf-8').strip()

        try:
            message = json.loads(message_str)
            logger.info(f"[TCP_IPC] Mensaje recibido: {message_str[:100]}...")
            await broadcast_message(message)
        except json.JSONDecodeError as e:
            logger.error(f"Error al decodificar JSON de mensaje TCP: {e} - Data: {message_str}")
        except Exception as e:
            logger.error(f"Error procesando mensaje TCP: {e}")

    except asyncio.IncompleteReadError:
        logger.warning(f"Conexión TCP IPC cerrada inesperadamente desde {peername}")
    except Exception as e:
        logger.error(f"Error en el manejador TCP IPC: {e}")
    finally:
        logger.info(f"Cerrando conexión TCP IPC desde {peername}")
        writer.close()
        await writer.wait_closed()

async def ws_handler(websocket):
    logger.info(f"Cliente público WebSocket conectado desde {websocket.remote_address}")
    connected_clients.add(websocket)
    try:
        await websocket.wait_closed()
    except websockets.exceptions.ConnectionClosed:
        logger.info(f"Cliente público desconectado: {websocket.remote_address}")
    finally:
        connected_clients.discard(websocket)

async def main():
    public_ws_server = await websockets.serve(ws_handler, '0.0.0.0', 8765)
    logger.info("Servidor WebSocket público escuchando en 0.0.0.0:8765")

    ipc_tcp_server = await asyncio.start_server(
        handle_tcp_message, '0.0.0.0', 8766
    )
    logger.info("Servidor TCP IPC (interno) escuchando en 0.0.0.0:8766")

    await asyncio.gather(
        public_ws_server.wait_closed(),
        ipc_tcp_server.serve_forever()
    )

if __name__ == "__main__":
    asyncio.run(main())

