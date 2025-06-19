const websocketUrl = 'ws://' + window.location.hostname + ':8765';
const mapContainer = document.getElementById('map-container');

let currentCityMap = {};
let currentTaxiFleetState = {};
let currentCustomerRequestsState = {};

function connectWebSocket() {
    const ws = new WebSocket(websocketUrl);

    ws.onopen = () => {
        console.log('Conectado al servidor WebSocket');
    };

    ws.onmessage = function(event) {
        console.log('Mensaje recibido del servidor:', event.data);
        try {
            const message = JSON.parse(event.data);
            if (message.operation_code === 'MAP_UPDATE' || message.operation_code === 'MAP_UPD') {
                const data = message.data;
                currentCityMap = data.city_map;
                currentTaxiFleetState = data.taxi_fleet;
                currentCustomerRequestsState = data.customer_requests;
                drawMap();
            }
        } catch (error) {
            console.error('Error al parsear el mensaje JSON:', error);
        }
    };

    ws.onclose = (event) => {
        console.log('Desconectado del servidor WebSocket:', event.code, event.reason);
        console.log('Intentando reconectar en 3 segundos...');
        setTimeout(connectWebSocket, 3000); 
    };

    ws.onerror = (error) => {
        console.error('Error en WebSocket:', error);
        ws.close();
    };
}

// Dibuja el mapa en el contenedor HTML
function drawMap() {
    const MAP_SIZE = 20;
    let mapHtml = '';

    // Primera fila: celda vacía + eje X
    mapHtml += `<span></span>`;
    for (let x = 1; x <= MAP_SIZE; x++) {
        mapHtml += `<span style="font-weight:bold;color:#888;display:flex;align-items:center;justify-content:center;">${x}</span>`;
    }

    // Crear grid de entidades por celda
    const grid = Array(MAP_SIZE).fill(null).map(() => Array(MAP_SIZE).fill(null).map(() => []));
    for (const locId in currentCityMap) {
        const coords = currentCityMap[locId];
        if (coords.y >= 0 && coords.y < MAP_SIZE && coords.x >= 0 && coords.x < MAP_SIZE) {
            grid[coords.y][coords.x].push({ char: locId, type: 'location' });
        }
    }
    for (const clientId in currentCustomerRequestsState) {
        const reqData = currentCustomerRequestsState[clientId];
        const cx = reqData.origin_coords.x;
        const cy = reqData.origin_coords.y;
        if (cy >= 0 && cy < MAP_SIZE && cx >= 0 && cx < MAP_SIZE) {
            grid[cy][cx].push({ char: clientId.toLowerCase().charAt(0), type: 'customer' });
        }
    }
    for (const taxiId in currentTaxiFleetState) {
        const taxiData = currentTaxiFleetState[taxiId];
        const tx = taxiData.x;
        const ty = taxiData.y;
        let taxiStatusClass = 'taxi-free';
        if (taxiData.status === 'moving_to_customer' || taxiData.status === 'moving_to_destination') {
            taxiStatusClass = 'taxi-moving';
        } else if (taxiData.status === 'disabled' || taxiData.status === 'stopped') {
            taxiStatusClass = 'taxi-disabled';
        } else if (taxiData.status === 'picked_up') {
            taxiStatusClass = 'taxi-picked-up';
        }
        if (ty >= 0 && ty < MAP_SIZE && tx >= 0 && tx < MAP_SIZE) {
            grid[ty][tx].push({ char: `T${taxiId}`, type: taxiStatusClass });
        }
    }
    // Renderizar filas de la cuadrícula (de arriba a abajo)
    for (let y = MAP_SIZE - 1; y >= 0; y--) {
        mapHtml += `<span style="font-weight:bold;color:#888;display:flex;align-items:center;justify-content:center;">${y+1}</span>`;
        for (let x = 0; x < MAP_SIZE; x++) {
            const cellEntities = grid[y][x];
            if (cellEntities.length === 0) {
                mapHtml += `<span class="map-cell empty"></span>`;
            } else {
                let cellContent = '';
                if (cellEntities.length <= 3) {
                    cellContent = cellEntities.map(e => `<div class="map-icon ${e.type}">${e.char}</div>`).join('');
                } else {
                    cellContent = cellEntities.slice(0,2).map(e => `<div class="map-icon ${e.type}">${e.char}</div>`).join('');
                    cellContent += `<div class="map-icon" style="background:#eee;color:#333;">+${cellEntities.length-2}</div>`;
                }
                mapHtml += `<span class="map-cell">${cellContent}</span>`;
            }
        }
    }
    mapContainer.innerHTML = mapHtml;
    mapContainer.style.display = 'block';
}

document.addEventListener('DOMContentLoaded', connectWebSocket);
