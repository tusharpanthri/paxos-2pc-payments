@echo off
setlocal

:: Check if protoc is installed.
where protoc >nul 2>nul
if %errorlevel% neq 0 (
    echo ❌ Error: 'protoc' is not installed or not in PATH. Install Protocol Buffers and try again.
    exit /b 1
)

:: Compile gRPC Proto Files.
echo 📌 Compiling Proto Files...
protoc --go_out=. --go-grpc_out=. --proto_path=proto proto/payment.proto
protoc --go_out=. --go-grpc_out=. --proto_path=proto proto/transaction_id.proto
if %errorlevel% neq 0 (
    echo ❌ Error: Failed to compile proto files.
    exit /b 1
)

:: Run go mod tidy.
echo 📌 Running Go Mod Tidy...
go mod tidy
if %errorlevel% neq 0 (
    echo ❌ Error: Go mod tidy failed.
    exit /b 1
)

:: Create temporary SAN config files for Gateway and Client.
echo 📌 Creating SAN config files...

(
echo [req]
echo distinguished_name = req_distinguished_name
echo req_extensions = v3_req
echo prompt = no
echo [req_distinguished_name]
echo [v3_req]
echo subjectAltName = @alt_names
echo [alt_names]
echo DNS.1 = localhost
echo IP.1 = 127.0.0.1
) > san_gateway.cnf

(
echo [req]
echo distinguished_name = req_distinguished_name
echo req_extensions = v3_req
echo prompt = no
echo [req_distinguished_name]
echo [v3_req]
echo subjectAltName = @alt_names
echo [alt_names]
echo DNS.1 = client
echo IP.1 = 127.0.0.1
) > san_client.cnf

:: Generate Certificates using the SAN config files.
echo 📌 Generating Certificates...
if not exist certs (
    mkdir certs
)

echo Generating CA key and certificate...
openssl req -x509 -nodes -newkey rsa:2048 -days 3650 -keyout certs\ca.key -out certs\ca.pem -subj "/CN=MyCA"
if %errorlevel% neq 0 (
    echo ❌ Error: Failed to generate CA certificate.
    exit /b 1
)

echo Generating Gateway CSR with SAN...
openssl req -nodes -newkey rsa:2048 -keyout certs\gateway.key -out certs\gateway.csr -subj "/CN=localhost" -config san_gateway.cnf -extensions v3_req
if %errorlevel% neq 0 (
    echo ❌ Error: Failed to generate Gateway CSR.
    exit /b 1
)

echo Signing Gateway certificate with CA...
openssl x509 -req -in certs\gateway.csr -CA certs\ca.pem -CAkey certs\ca.key -CAcreateserial -out certs\gateway.pem -days 365 -extfile san_gateway.cnf -extensions v3_req
if %errorlevel% neq 0 (
    echo ❌ Error: Failed to sign Gateway certificate.
    exit /b 1
)

echo Generating Client CSR with SAN...
openssl req -nodes -newkey rsa:2048 -keyout certs\client.key -out certs\client.csr -subj "/CN=client" -config san_client.cnf -extensions v3_req
if %errorlevel% neq 0 (
    echo ❌ Error: Failed to generate Client CSR.
    exit /b 1
)

echo Signing Client certificate with CA...
openssl x509 -req -in certs\client.csr -CA certs\ca.pem -CAkey certs\ca.key -CAcreateserial -out certs\client.pem -days 365 -extfile san_client.cnf -extensions v3_req
if %errorlevel% neq 0 (
    echo ❌ Error: Failed to sign Client certificate.
    exit /b 1
)

echo Cleaning up temporary files...
del certs\*.csr
del certs\*.srl
del san_gateway.cnf
del san_client.cnf

echo Certificate generation complete.
timeout /t 2 >nul

:: Start the Transaction ID Server.
echo 🚀 Starting Transaction ID Server on port 50055...
start "Transaction ID Server" cmd /k "cd transaction_id_server && go run transaction_id_server.go"
timeout /t 2 >nul

:: Start the Payment Gateway.
echo 🚀 Starting Gateway on port 50051...
start "Gateway" cmd /k "cd gateway && go run ."
timeout /t 3 >nul

:: Start Bank Servers.
echo 🚀 Starting Bank Server 1 on port 50052...
start "Bank Server 1" cmd /k "cd bank && go run bank.go -port=50052 -bank=ICICI"
timeout /t 2 >nul

echo 🚀 Starting Bank Server 2 on port 50053...
start "Bank Server 2" cmd /k "cd bank && go run bank.go -port=50053 -bank=SBI"
timeout /t 2 >nul

:: Start Client Instances.
echo 🚀 Starting Client 1...
start "Client 1" cmd /k "cd client && go run client.go -username=kevin -password=kevin -account=ACC1 -bank=ICICI -register=true"
timeout /t 2 >nul

echo 🚀 Starting Client 2...
start "Client 2" cmd /k "cd client && go run client.go -username=neel -password=neel -account=ACC2 -bank=SBI -register=true"
timeout /t 2 >nul

echo 🚀 Starting Client 3 (with registration)...
start "Client 3" cmd /k "cd client && go run client.go -username=divu -password=divu -account=ACC3 -bank=ICICI -register=true"
timeout /t 2 >nul

echo ✅ All services started successfully!
endlocal
pause
