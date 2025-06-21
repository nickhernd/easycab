@echo off
echo Iniciando el programa...
start "" "C:\Program Files\Docker\Docker\Docker Desktop.exe"
timeout /t 30 /nobreak  
cd pc1
docker compose up --build -d
if %errorlevel% neq 0 (
    echo Error al iniciar pc1. Verifique el archivo docker-compose.yml.
    exit /b %errorlevel%
)
timeout /t 30 /nobreak
start http://172.22.161.64:8080
cd ..\pc3
docker compose up taxi1 --build -d
docker compose up taxi2 --build -d
docker compose up taxi3 --build -d
timeout /t 30 /nobreak
cd ..\pc2
docker compose up customera --build -d
docker compose up customerb --build -d
docker compose up customerc --build -d
echo Presione cualquier tecla para detener todos los contenedores y salir...
pause >nul
docker compose down -v 
echo Todos los contenedores han sido detenidos.
echo Saliendo del programa...
exit /b 0