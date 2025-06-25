@echo off
echo Iniciando el programa...
start "" "C:\Program Files\Docker\Docker\Docker Desktop.exe"
timeout /t 30 /nobreak  
cd pc1
docker compose up --build -d

:check_pc1_status
echo Comprobando el estado de los servicios de pc1...
docker compose ps --services --filter "status=running" | findstr /i "pc1_service_name_here" >nul
if %errorlevel% neq 0 (
    echo Algunos servicios de pc1 no están levantados. Reintentando...
    docker compose up --build -d
    timeout /t 10 /nobreak
    goto check_pc1_status
) else (
    echo Todos los servicios de pc1 están levantados.
)

timeout /t 30 /nobreak
start http://172.22.161localhost.64:8080
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