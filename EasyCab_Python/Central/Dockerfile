# Usa una imagen base de Python
FROM python:3.9-slim-buster

# Establece el directorio de trabajo dentro del contenedor
WORKDIR /app

# Copia los archivos de requirements.txt y luego instala las dependencias.
# Esto optimiza el caché de Docker.
COPY Central/requirements.txt /app/Central/requirements.txt
COPY common/requirements.txt /app/common/requirements.txt
RUN pip install --no-cache-dir -r /app/Central/requirements.txt \
    && pip install --no-cache-dir -r /app/common/requirements.txt

# Crea explícitamente el directorio para Central si no existe (aunque COPY suele hacerlo)
RUN mkdir -p /app/Central

# Copia los scripts Python a sus ubicaciones esperadas dentro de /app/Central
COPY Central/EC_Central.py /app/Central/EC_Central.py
COPY Central/audit_api.py /app/Central/audit_api.py
COPY Central/database.py /app/Central/database.py

# Copia el módulo common que es una dependencia, en la raíz de /app
COPY common /app/common

# Ejecuta el script de inicialización de la base de datos después de copiarlo
# Esto asegurará que la DB y sus tablas existan antes de que EC_Central intente acceder a ellas
RUN python3 /app/Central/database.py 

# Puerto que la aplicación Central expone para la autenticación de taxis
EXPOSE 65432

# Puerto que la API de auditoría expone
EXPOSE 6000

# Comando para ejecutar la aplicación.
# Inicia audit_api.py en segundo plano y luego EC_Central.py.
# Asegúrate de que KAFKA_BROKER se pase como variable de entorno desde docker-compose.
CMD ["/bin/bash", "-c", "python3 /app/Central/audit_api.py & python3 /app/Central/EC_Central.py --listen_port 65432 --ip_port_broker ${KAFKA_BROKER}"]
