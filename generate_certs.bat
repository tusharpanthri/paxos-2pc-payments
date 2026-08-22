@echo off
setlocal

REM Generates the private CA and the four leaf identities used by the system:
REM
REM   ca       signs everything; each service trusts this and nothing else
REM   gateway  server identity for the payment gateway
REM   bank     server identity for the bank servers
REM   txnid    server identity for the transaction id service
REM   client   client identity presented by clients, banks and the gateway
REM
REM The certificate profiles live in certs\openssl.cnf.

cd /d "%~dp0"

where openssl >nul 2>nul
if errorlevel 1 (
  echo Error: openssl is not on PATH.
  exit /b 1
)

if not exist certs mkdir certs

REM See the note in certs\openssl.cnf for why the config is pinned.
set "OPENSSL_CONF=%~dp0certs\openssl.cnf"

if not exist "%OPENSSL_CONF%" (
  echo Error: %OPENSSL_CONF% is missing. It holds the certificate profiles and
  echo is part of the repository; restore it with "git checkout certs/openssl.cnf".
  exit /b 1
)

echo Generating the certificate authority...
openssl req -x509 -nodes -newkey rsa:2048 -days 3650 ^
  -keyout certs\ca.key -out certs\ca.pem -subj "/CN=PaymentGatewayCA" ^
  -extensions v3_ca
if errorlevel 1 exit /b 1

for %%S in (gateway bank txnid) do (
  echo Generating the %%S server certificate...
  call :sign %%S "/CN=localhost" v3_server
  if errorlevel 1 exit /b 1
)

echo Generating the client certificate...
call :sign client "/CN=client" v3_client
if errorlevel 1 exit /b 1

del /q certs\*.csr certs\*.srl 2>nul
echo Done. Certificates are in .\certs
endlocal
exit /b 0

:sign
openssl req -nodes -newkey rsa:2048 -keyout certs\%1.key -out certs\%1.csr -subj %2
if errorlevel 1 exit /b 1
openssl x509 -req -in certs\%1.csr -CA certs\ca.pem -CAkey certs\ca.key ^
  -CAcreateserial -out certs\%1.pem -days 365 ^
  -extfile "%OPENSSL_CONF%" -extensions %3
if errorlevel 1 exit /b 1
exit /b 0
