@echo off
setlocal

REM Starts the whole system with each bank running as a 3-member Paxos
REM group, in separate windows, in dependency order: transaction id service,
REM gateway, both banks' three replicas each, then clients.
REM
REM This is the same system run_all.bat starts, except every bank can now
REM survive one replica crashing: prepares and commits are replicated across
REM the group before they take effect, and whichever replica is leader
REM re-announces itself to the gateway, including after a failover election.
REM Type "down" in a leader's window to watch that happen.
REM
REM Every service is launched from the repository root, which is where the
REM certs directory lives; each replica gets its own subdirectory under
REM data\ for its account store, ledger and Paxos log.

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

echo Starting bank ICICI as a 3-member group (grpc :50110-:50112, consensus :50210-:50212)...
start "bank ICICI-1" cmd /k "go run ./bank -bank=ICICI -id=ICICI-1 -port=50110 -data=data\ICICI-1 -peers=ICICI-1=localhost:50210,ICICI-2=localhost:50211,ICICI-3=localhost:50212"
start "bank ICICI-2" cmd /k "go run ./bank -bank=ICICI -id=ICICI-2 -port=50111 -data=data\ICICI-2 -peers=ICICI-1=localhost:50210,ICICI-2=localhost:50211,ICICI-3=localhost:50212"
start "bank ICICI-3" cmd /k "go run ./bank -bank=ICICI -id=ICICI-3 -port=50112 -data=data\ICICI-3 -peers=ICICI-1=localhost:50210,ICICI-2=localhost:50211,ICICI-3=localhost:50212"
timeout /t 2 >nul

echo Starting bank SBI as a 3-member group (grpc :50120-:50122, consensus :50220-:50222)...
start "bank SBI-1" cmd /k "go run ./bank -bank=SBI -id=SBI-1 -port=50120 -data=data\SBI-1 -peers=SBI-1=localhost:50220,SBI-2=localhost:50221,SBI-3=localhost:50222"
start "bank SBI-2" cmd /k "go run ./bank -bank=SBI -id=SBI-2 -port=50121 -data=data\SBI-2 -peers=SBI-1=localhost:50220,SBI-2=localhost:50221,SBI-3=localhost:50222"
start "bank SBI-3" cmd /k "go run ./bank -bank=SBI -id=SBI-3 -port=50122 -data=data\SBI-3 -peers=SBI-1=localhost:50220,SBI-2=localhost:50221,SBI-3=localhost:50222"
timeout /t 3 >nul

echo Starting clients (each account is seeded at every ICICI/SBI replica)...
start "client varun" cmd /k "go run ./client -username=varun -password=varun -account=ACC1 -bank=ICICI -register -replica-data=data\ICICI-1,data\ICICI-2,data\ICICI-3"
timeout /t 2 >nul
start "client pavan" cmd /k "go run ./client -username=pavan -password=pavan -account=ACC2 -bank=SBI -register -replica-data=data\SBI-1,data\SBI-2,data\SBI-3"
timeout /t 2 >nul
start "client aditya" cmd /k "go run ./client -username=aditya -password=aditya -account=ACC3 -bank=ICICI -register -replica-data=data\ICICI-1,data\ICICI-2,data\ICICI-3"

echo All services started. Type "down"/"up" in any bank replica's window to
echo simulate that replica crashing; its group elects a new leader and
echo re-registers with the gateway automatically.
endlocal
