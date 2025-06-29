console.log('EasyCab script.js cargado');

// Usa window.location.hostname para ser adaptable en entornos distribuidos
const websocketUrl = 'ws://' + window.location.hostname + ':8765';
const mapContainer = document.getElementById('map-container');
const taxiStatusTableBody = document.querySelector('#taxi-status-table tbody');

let currentCityMap = {};
let currentTaxiFleetState = {};
let currentCustomerRequestsState = {};

function connectWebSocket() {
    // Usa la variable websocketUrl definida globalmente
    const wsUrl = websocketUrl; 
    const statusDiv = document.getElementById('ws-status');
    if (statusDiv) statusDiv.textContent = `Conectando a ${wsUrl}`;
    console.log('[WS] Intentando conectar a', wsUrl);
    const ws = new WebSocket(wsUrl);

    ws.onopen = () => {
        console.log('[WS] Conectado al servidor WebSocket');
        if (statusDiv) statusDiv.textContent = 'WebSocket conectado';
    };

    ws.onmessage = function(event) {
        console.log('[WS] Mensaje recibido del servidor (raw):', event.data);
        try {
            const message = JSON.parse(event.data);
            console.log('[WS] Mensaje parseado:', message);
            if (message.operation_code === 'MAP_UPDATE' || message.operation_code === 'MAP_UPD') {
                const data = message.data;
                console.log('[WS] Datos de mapa recibidos:', data);
                currentCityMap = data.city_map;
                currentTaxiFleetState = data.taxi_fleet;
                currentCustomerRequestsState = data.customer_requests;
                console.log('[drawMap] Llamando a drawMap con:', {
                    currentCityMap, currentTaxiFleetState, currentCustomerRequestsState
                });
                drawMap();
                updateTaxiStatusTable();
            } else {
                console.warn('[WS] Mensaje recibido sin operation_code esperado:', message);
            }
        } catch (error) {
            console.error('[WS] Error al parsear el mensaje JSON:', error, event.data);
        }
    };

    ws.onclose = (event) => {
        console.log('[WS] Desconectado del servidor WebSocket:', event.code, event.reason);
        if (statusDiv) statusDiv.textContent = `Desconectado. Reintentando en 2s... (${event.code} ${event.reason})`;
        setTimeout(connectWebSocket, 2000);
    };

    ws.onerror = (error) => {
        console.error('[WS] Error en WebSocket:', error);
        if (statusDiv) statusDiv.textContent = 'Error en WebSocket. Cerrando...';
        ws.close();
    };
}

function drawMap() {
    const MAP_SIZE = 20;
    console.log('[drawMap] Renderizando mapa...');

    mapContainer.innerHTML = '';

    const grid = Array(MAP_SIZE).fill(null).map(() => Array(MAP_SIZE).fill(null).map(() => []));

    for (const locId in currentCityMap) {
        const coords = currentCityMap[locId];
        if (coords.y >= 0 && coords.y < MAP_SIZE && coords.x >= 0 && coords.x < MAP_SIZE) {
            grid[coords.y][coords.x].push({ char: locId, type: 'location' });
        }
    }
    console.log('[drawMap] Id del cliente con el taxi asignado:', currentCustomerRequestsState);
    for (const clientId in currentCustomerRequestsState) {
        console.log('[drawMap] Procesando cliente:', clientId);
        const reqData = currentCustomerRequestsState[clientId];
        const cx = reqData.origin_coords.x;
        const cy = reqData.origin_coords.y;
        if (cy >= 0 && cy < MAP_SIZE && cx >= 0 && cx < MAP_SIZE) {
            grid[cy][cx].push({ char: clientId.charAt(0), type: 'customer' });
        }
    }
    for (const taxiId in currentTaxiFleetState) {
        const taxiData = currentTaxiFleetState[taxiId];
        const tx = taxiData.x;
        const ty = taxiData.y;
        let taxiStatusClass = 'taxi-free'; // Default
        let displayTaxiId = taxiId;

        // Buscar el cliente asignado a este taxi
        let assignedClientId = null;
        for (const clientId in currentCustomerRequestsState) {
            const reqData = currentCustomerRequestsState[clientId];
            if (reqData.assigned_taxi_id === taxiId) {
                assignedClientId = clientId;
                break;
            }
        }

        console.log('[drawMap] Taxi estado:', {
            id: taxiId,
            status: taxiData.status,
            assignedClientId
        });

        console.log('[drawMap] Service ID del taxi:', taxiData.service_id);
        // Si el taxi está recogiendo cliente, mostrar como 1X (X = id del cliente)
        if (taxiData.status === 'moving_to_destination' && taxiData.service_id !== null) {
            displayTaxiId = taxiId + taxiData.service_id;
            taxiStatusClass = 'taxi-picked-up';
        } else if (taxiData.status === 'moving_to_customer' || taxiData.status === 'returning_to_base') {
            taxiStatusClass = 'taxi-moving';
        } else if (taxiData.status === 'disabled' || taxiData.status === 'stopped') {
            taxiStatusClass = 'taxi-disabled';
        }

        if (ty >= 0 && ty < MAP_SIZE && tx >= 0 && tx < MAP_SIZE) {
            grid[ty][tx].push({ char: `T${displayTaxiId}`, type: taxiStatusClass });
        } else {
            console.warn(`[drawMap] Taxi T${taxiId} tiene coordenadas fuera de rango: (${tx}, ${ty})`);
        }
    }

    const emptyCornerCell = document.createElement('span');
    emptyCornerCell.classList.add('map-cell', 'empty');
    mapContainer.appendChild(emptyCornerCell);

    for (let x = 0; x < MAP_SIZE; x++) {
        const span = document.createElement('span');
        span.classList.add('map-cell');
        span.style.fontWeight = 'bold';
        span.style.color = '#888';
        span.textContent = x;
        mapContainer.appendChild(span);
    }

    for (let y = 0; y < MAP_SIZE; y++) {
        const yLabelSpan = document.createElement('span');
        yLabelSpan.classList.add('map-cell');
        yLabelSpan.style.fontWeight = 'bold';
        yLabelSpan.style.color = '#888';
        yLabelSpan.textContent = y;
        mapContainer.appendChild(yLabelSpan);

        for (let x = 0; x < MAP_SIZE; x++) {
            const cellEntities = grid[y][x];
            const cellSpan = document.createElement('span');
            cellSpan.classList.add('map-cell');

            if (cellEntities.length === 0) {
                cellSpan.classList.add('empty');
            } else {
                let cellHtml = '';
                cellEntities.sort((a, b) => {
                    const order = { 'location': 1, 'customer': 2, 'taxi-free': 3, 'taxi-moving': 3, 'taxi-picked-up': 3, 'taxi-disabled': 3 };
                    return (order[a.type] || 99) - (order[b.type] || 99);
                });

                for (const entity of cellEntities) {
                    cellHtml += `<span class="map-icon ${entity.type}">${entity.char}</span>`;
                }
                cellSpan.innerHTML = cellHtml;
            }
            mapContainer.appendChild(cellSpan);
        }
    }
    console.log('[drawMap] Mapa renderizado.');
}

function updateTaxiStatusTable() {
    taxiStatusTableBody.innerHTML = '';

    const sortedTaxiIds = Object.keys(currentTaxiFleetState).sort((a, b) => parseInt(a) - parseInt(b));

    for (const taxiId of sortedTaxiIds) {
        const taxiData = currentTaxiFleetState[taxiId];
        const row = document.createElement('tr');

        let assignedClient = 'N/A';
        // Primero busca en customer_requests
        for (const clientId in currentCustomerRequestsState) {
            const reqData = currentCustomerRequestsState[clientId];
            if (reqData.assigned_taxi_id === taxiId) {
                assignedClient = clientId;
                break;
            }
        }
        // Si no lo encuentra, usa el campo service_id del taxi
        if (assignedClient === 'N/A' && taxiData.service_id) {
            assignedClient = taxiData.service_id;
        }

        const statusClass = `status-${taxiData.status.replace(/ /g, '_')}`;

        row.innerHTML = `
            <td>T${taxiId}</td>
            <td class="${statusClass}">${formatTaxiStatus(taxiData.status)}</td>
            <td>(${taxiData.x}, ${taxiData.y})</td>
            <td>${assignedClient}</td>
        `;
        taxiStatusTableBody.appendChild(row);
    }
    console.log('[updateTaxiStatusTable] Tabla de estado de taxis actualizada.');
}

function formatTaxiStatus(status) {
    switch (status) {
        case 'free': return 'Libre';
        case 'moving_to_customer': return 'En camino a cliente';
        case 'moving_to_destination': return 'En camino a destino';
        case 'picked_up': return 'Cliente a bordo';
        case 'occupied': return 'Ocupado'; // Keep if your backend sends it, otherwise remove
        case 'disabled': return 'Deshabilitado';
        case 'stopped': return 'Detenido'; // Keep if your backend sends it, otherwise remove
        case 'returning_to_base': return 'Volviendo a base'; // NEW: Add this status
        default: return status;
    }
}

document.addEventListener('DOMContentLoaded', () => {
    let statusDiv = document.createElement('div');
    statusDiv.id = 'ws-status';
    statusDiv.style = 'color: #007bff; font-weight: bold; margin-bottom: 10px;';
    document.body.insertBefore(statusDiv, document.body.firstChild);
    connectWebSocket();
});
