const websocketUrl = 'ws://localhost:8765'; // URL de tu servidor WebSocket
const mapContainer = document.getElementById('map-container');

let currentCityMap = {};
let currentTaxiFleetState = {};
let currentCustomerRequestsState = {};

// Función para inicializar la conexión WebSocket
function connectWebSocket() {
    const ws = new WebSocket(websocketUrl);

    ws.onopen = () => {
        console.log('Conectado al servidor WebSocket');
    };

    ws.onmessage = (event) => {
        // console.log('Mensaje recibido del servidor:', event.data); // Para depuración
        try {
            const message = JSON.parse(event.data);
            if (message.operation_code === 'MAP_UPDATE') {
                const data = message.data;
                currentCityMap = data.city_map;
                currentTaxiFleetState = data.taxi_fleet;
                currentCustomerRequestsState = data.customer_requests;
                drawMap(); // Dibuja el mapa con los datos actualizados
            }
        } catch (error) {
            console.error('Error al parsear el mensaje JSON:', error);
        }
    };

    ws.onclose = (event) => {
        console.log('Desconectado del servidor WebSocket:', event.code, event.reason);
        console.log('Intentando reconectar en 3 segundos...');
        setTimeout(connectWebSocket, 3000); // Intenta reconectar después de 3 segundos
    };

    ws.onerror = (error) => {
        console.error('Error en WebSocket:', error);
        ws.close(); // Cierra la conexión y dispara onclose para intentar reconectar
    };
}

// Función para dibujar el mapa en el contenedor HTML
function drawMap() {
    // Determinar las dimensiones del mapa
    let maxX = 0;
    let maxY = 0;

    // Calcular las dimensiones máximas basándose en todos los elementos del mapa
    Object.values(currentCityMap).forEach(loc => {
        if (loc.x > maxX) maxX = loc.x;
        if (loc.y > maxY) maxY = loc.y;
    });
    Object.values(currentTaxiFleetState).forEach(taxi => {
        if (taxi.x > maxX) maxX = taxi.x;
        if (taxi.y > maxY) maxY = taxi.y;
    });
    // Los clientes pueden estar en la misma posición que un punto de la ciudad
    Object.values(currentCustomerRequestsState).forEach(customer => {
        if (customer.origin_coords.x > maxX) maxX = customer.origin_coords.x;
        if (customer.origin_coords.y > maxY) maxY = customer.origin_coords.y;
        if (customer.destination_coords.x > maxX) maxX = customer.destination_coords.x;
        if (customer.destination_coords.y > maxY) maxY = customer.destination_coords.y;
    });

    const gridWidth = maxX + 1;
    const gridHeight = maxY + 1;

    // Crear una matriz 2D para representar el mapa
    // Invertimos el eje Y para que (0,0) sea abajo-izquierda como en los sistemas de coordenadas cartesianos
    const grid = Array(gridHeight).fill(null).map(() => Array(gridWidth).fill({ char: '.', type: 'empty' }));

    // 1. Colocar ubicaciones de la ciudad
    for (const locId in currentCityMap) {
        const coords = currentCityMap[locId];
        if (coords.y >= 0 && coords.y < gridHeight && coords.x >= 0 && coords.x < gridWidth) {
            grid[coords.y][coords.x] = { char: locId, type: 'location' };
        }
    }

    // 2. Colocar clientes (puntos de recogida)
    for (const clientId in currentCustomerRequestsState) {
        const reqData = currentCustomerRequestsState[clientId];
        const cx = reqData.origin_coords.x;
        const cy = reqData.origin_coords.y;

        if (cy >= 0 && cy < gridHeight && cx >= 0 && cx < gridWidth) {
            // Solo si la celda no está ocupada por un taxi, lo colocamos.
            // Los clientes tienen menor prioridad visual que los taxis,
            // pero mayor que las ubicaciones de ciudad si no hay taxi.
            // Para evitar sobrescribir locations o taxis si el cliente está en su misma coordenada,
            // podemos poner el cliente si la celda es "empty" o si ya es un "customer".
            if (grid[cy][cx].type === 'empty' || grid[cy][cx].type === 'customer') {
                grid[cy][cx] = { char: clientId.toLowerCase().charAt(0), type: 'customer' };
            }
        }
    }

    // 3. Colocar taxis (¡ÚLTIMO para que se vean sobre otros elementos si coinciden!)
    for (const taxiId in currentTaxiFleetState) {
        const taxiData = currentTaxiFleetState[taxiId];
        const tx = taxiData.x;
        const ty = taxiData.y;
        let taxiStatusClass = 'taxi-free'; // Default

        if (taxiData.status === 'moving_to_customer' || taxiData.status === 'moving_to_destination') {
            taxiStatusClass = 'taxi-moving';
        } else if (taxiData.status === 'disabled' || taxiData.status === 'stopped') {
            taxiStatusClass = 'taxi-disabled';
        } else if (taxiData.status === 'picked_up') {
            taxiStatusClass = 'taxi-picked-up';
        }
        // "free" ya está asignado por defecto

        if (ty >= 0 && ty < gridHeight && tx >= 0 && tx < gridWidth) {
            grid[ty][tx] = { char: `T${taxiId}`, type: taxiStatusClass };
        }
    }

    // Construir el HTML del mapa
    let mapHtml = '';
    // Iterar en orden inverso para que (0,0) esté en la esquina inferior izquierda
    for (let y = gridHeight - 1; y >= 0; y--) {
        for (let x = 0; x < gridWidth; x++) {
            const cell = grid[y][x];
            mapHtml += `<span class="map-cell ${cell.type}">${cell.char}</span>`;
        }
        mapHtml += '\n'; // Salto de línea al final de cada fila
    }
    mapContainer.innerHTML = mapHtml;
}

// Iniciar la conexión WebSocket cuando la página se cargue
document.addEventListener('DOMContentLoaded', connectWebSocket);