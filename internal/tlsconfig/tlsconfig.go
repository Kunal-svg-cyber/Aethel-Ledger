// Package tlsconfig builds server TLS configuration from certificate files.
package tlsconfig

import (
	"crypto/tls"
	"errors"
	"fmt"
)

// Load returns a TLS config (TLS 1.2 minimum) for the given PEM files.
func Load(certFile, keyFile string) (*tls.Config, error) {
	if certFile == "" || keyFile == "" {
		return nil, errors.New("tlsconfig: both certificate and key files are required")
	}
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("tlsconfig: load key pair: %w", err)
	}
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	}, nil
}
