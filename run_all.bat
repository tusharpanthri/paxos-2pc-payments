@echo off
setlocal

REM Starts the whole system in separate windows, in dependency order:
REM transaction id service, gateway, banks, then clients.
REM
REM Every service is launched from the repository root, which is where the
REM data files and the certs directory live.

cd /d "%~dp0"

if not exist certs\ca.pem (
  echo No certificates found; generating them first...
  call generate_certs.bat
  if errorlevel 1 exit /b 1
)

echo Building...
go build ./... || exit /b 1

echo Starting the transaction id service on :50055...
start "txnid" cmd /k "go run ./transaction_id_server"
timeout /t 2 >nul

echo Starting the gateway on :50051...
start "gateway" cmd /k "go run ./gateway"
timeout /t 3 >nul

echo Starting bank ICICI on :50052...
start "bank ICICI" cmd /k "go run ./bank -bank=ICICI -port=50052"
timeout /t 2 >nul

echo Starting bank SBI on :50053...
start "bank SBI" cmd /k "go run ./bank -bank=SBI -port=50053"
timeout /t 2 >nul

echo Starting clients...
start "client varun" cmd /k "go run ./client -username=varun -password=varun -account=ACC1 -bank=ICICI -register"
timeout /t 2 >nul
start "client pavan" cmd /k "go run ./client -username=pavan -password=pavan -account=ACC2 -bank=SBI -register"
timeout /t 2 >nul
start "client aditya" cmd /k "go run ./client -username=aditya -password=aditya -account=ACC3 -bank=ICICI -register"

echo All services started.
endlocal
