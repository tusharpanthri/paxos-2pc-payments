#!/usr/bin/env bash
# Generates the private CA and the four leaf identities used by the system:
#
#   ca       signs everything; each service trusts this and nothing else
#   gateway  server identity for the payment gateway
#   bank     server identity for the bank servers
#   txnid    server identity for the transaction id service
#   client   client identity presented by clients, banks and the gateway
#
# The certificate profiles live in certs/openssl.cnf. See generate_certs.bat
# for the Windows equivalent.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
mkdir -p "$root/certs"

# Under Git Bash the native openssl.exe cannot read an MSYS-style path, so
# translate it. See the note in certs/openssl.cnf for why it is pinned at all.
conf="$root/certs/openssl.cnf"
if [ ! -f "$conf" ]; then
  echo "Error: $conf is missing. It holds the certificate profiles and is" >&2
  echo "part of the repository; restore it with 'git checkout certs/openssl.cnf'." >&2
  exit 1
fi
if command -v cygpath >/dev/null 2>&1; then
  conf="$(cygpath -w "$conf")"
fi
export OPENSSL_CONF="$conf"

# Git Bash also rewrites arguments that look like Unix paths, which mangles
# the "/CN=..." subject. This is a no-op on Linux and macOS.
export MSYS_NO_PATHCONV=1

cd "$root"

echo "Generating the certificate authority..."
openssl req -x509 -nodes -newkey rsa:2048 -days 3650 \
  -keyout certs/ca.key -out certs/ca.pem -subj "/CN=PaymentGatewayCA" \
  -extensions v3_ca 2>/dev/null

sign() { # sign <name> <subject> <extension section>
  openssl req -nodes -newkey rsa:2048 -keyout "certs/$1.key" -out "certs/$1.csr" \
    -subj "$2" 2>/dev/null
  openssl x509 -req -in "certs/$1.csr" -CA certs/ca.pem -CAkey certs/ca.key \
    -CAcreateserial -out "certs/$1.pem" -days 365 \
    -extfile "$OPENSSL_CONF" -extensions "$3" 2>/dev/null
}

for name in gateway bank txnid; do
  echo "Generating the $name server certificate..."
  sign "$name" "/CN=localhost" v3_server
done

echo "Generating the client certificate..."
sign client "/CN=client" v3_client

rm -f certs/*.csr certs/*.srl
echo "Done. Certificates are in ./certs"
