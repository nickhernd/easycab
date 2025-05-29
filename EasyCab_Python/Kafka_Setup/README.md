# Instructions for setting up Kafka with Docker
# Configuración de Kafka para EasyCab

Este directorio contiene el archivo `docker-compose.yml` para levantar un entorno de Kafka y Zookeeper, que serán utilizados para el streaming de eventos en el sistema EasyCab.

## Requisitos

* Docker Desktop (o Docker Engine) instalado y en ejecución.

## Pasos para iniciar Kafka y Zookeeper

1.  Navega a este directorio en tu terminal:
    ```bash
    cd EasyCab_Python/Kafka_Setup
    ```

2.  Levanta los servicios de Zookeeper y Kafka usando Docker Compose:
    ```bash
    docker-compose up -d
    ```
    Esto iniciará los contenedores en segundo plano.

3.  Para verificar que los contenedores están en ejecución:
    ```bash
    docker-compose ps
    ```
    Deberías ver `zookeeper` y `kafka` con el estado `Up`.

4.  Para detener los servicios:
    ```bash
    docker-compose down
    ```

## Temas de Kafka (Topics)

Los temas de Kafka se crearán automáticamente gracias a `KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"` en el `docker-compose.yml`. Algunos de los temas clave que se usarán son:

* `taxi_movements`: Para que los taxis envíen su posición constantemente.
* `sensor_data`: Para que los sensores envíen información sobre el estado del taxi (OK/KO).
* `customer_requests`: Para que los clientes soliciten servicios de taxi.
* `central_updates`: Para que la CENTRAL envíe el estado del mapa y otras actualizaciones a todos los taxis y clientes.
* `service_notifications`: Para notificaciones de la CENTRAL a los clientes (aceptación/denegación de servicio).
* `taxi_commands`: Para que la CENTRAL envíe comandos a los taxis (parar, reanudar, cambiar destino, etc.).