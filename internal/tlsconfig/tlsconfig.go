// Package tlsconfig builds the mutual-TLS credentials every hop in the
// system uses.
//
// All certificates are signed by one private CA (certs/ca.pem), so both
// sides of every connection can verify each other:
//
//	client  --mTLS-->  gateway     (client.pem  / gateway.pem)
//	bank    --mTLS-->  gateway     (client.pem  / gateway.pem)
//	gateway --mTLS-->  bank        (client.pem  / bank.pem)
package tlsconfig

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"path/filepath"

	"google.golang.org/grpc/credentials"
)

// Files locates the key material for one identity.
type Files struct {
	Dir  string // directory holding the certificates, e.g. "../certs"
	Cert string // leaf certificate file name, e.g. "gateway.pem"
	Key  string // matching private key file name, e.g. "gateway.key"
}

// ServerCreds returns credentials for a gRPC server that requires and
// verifies a client certificate signed by our CA.
func ServerCreds(f Files) (credentials.TransportCredentials, error) {
	cert, pool, err := load(f)
	if err != nil {
		return nil, err
	}
	return credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientCAs:    pool,
		ClientAuth:   tls.RequireAndVerifyClientCert,
		MinVersion:   tls.VersionTLS13,
	}), nil
}

// ClientCreds returns credentials for a gRPC client that presents its own
// certificate and verifies the server against our CA. serverName must match
// a SAN on the server's certificate.
func ClientCreds(f Files, serverName string) (credentials.TransportCredentials, error) {
	cert, pool, err := load(f)
	if err != nil {
		return nil, err
	}
	return credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      pool,
		ServerName:   serverName,
		MinVersion:   tls.VersionTLS13,
	}), nil
}

func load(f Files) (tls.Certificate, *x509.CertPool, error) {
	certPath := filepath.Join(f.Dir, f.Cert)
	keyPath := filepath.Join(f.Dir, f.Key)
	caPath := filepath.Join(f.Dir, "ca.pem")

	cert, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		return tls.Certificate{}, nil, fmt.Errorf("load key pair %s/%s: %w (run generate_certs.bat)", certPath, keyPath, err)
	}
	ca, err := os.ReadFile(caPath)
	if err != nil {
		return tls.Certificate{}, nil, fmt.Errorf("read CA %s: %w (run generate_certs.bat)", caPath, err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(ca) {
		return tls.Certificate{}, nil, fmt.Errorf("no valid certificates found in %s", caPath)
	}
	return cert, pool, nil
}
