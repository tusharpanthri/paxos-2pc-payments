@echo off
REM Create the certs folder if it doesn't exist
if not exist certs (
  mkdir certs
)

echo Generating CA key and certificate...
openssl req -x509 -nodes -newkey rsa:2048 -days 3650 -keyout certs\ca.key -out certs\ca.pem -subj "/CN=MyCA"

echo Generating Gateway key and certificate signing request (CSR) with SAN...
openssl req -nodes -newkey rsa:2048 -keyout certs\gateway.key -out certs\gateway.csr -subj "/CN=localhost" -addext "subjectAltName=DNS:localhost"

echo Signing Gateway certificate with CA...
openssl x509 -req -in certs\gateway.csr -CA certs\ca.pem -CAkey certs\ca.key -CAcreateserial -out certs\gateway.pem -days 365

echo Generating Client key and certificate signing request (CSR) with SAN...
openssl req -nodes -newkey rsa:2048 -keyout certs\client.key -out certs\client.csr -subj "/CN=client" -addext "subjectAltName=DNS:client"

echo Signing Client certificate with CA...
openssl x509 -req -in certs\client.csr -CA certs\ca.pem -CAkey certs\ca.key -CAcreateserial -out certs\client.pem -days 365

echo Cleaning up temporary CSR and serial files...
del certs\*.csr
del certs\*.srl

echo Certificate generation complete.
pause
