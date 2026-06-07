package main

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"strings"
	"time"

	"google.golang.org/grpc/credentials"
)

func bootstrapToken(value, file, env string) (string, error) {
	if value != "" {
		return strings.TrimSpace(value), nil
	}
	if file != "" {
		raw, err := os.ReadFile(file)
		if err != nil {
			return "", err
		}
		return strings.TrimSpace(string(raw)), nil
	}
	if env != "" {
		return strings.TrimSpace(os.Getenv(env)), nil
	}
	return "", nil
}

func waitAuthEnabled(n interface{ AuthEnabled() (bool, error) }, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		enabled, err := n.AuthEnabled()
		if err != nil {
			return err
		}
		if enabled {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("auth bootstrap token was supplied, but replicated auth state was not visible within %s", timeout)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func serverTransportCredentials(certFile, keyFile, clientCAFile string) (credentials.TransportCredentials, error) {
	if certFile == "" && keyFile == "" && clientCAFile == "" {
		return nil, nil
	}
	if certFile == "" || keyFile == "" {
		return nil, fmt.Errorf("--tls-cert and --tls-key must be set together")
	}
	if clientCAFile == "" {
		return credentials.NewServerTLSFromFile(certFile, keyFile)
	}
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, err
	}
	caPEM, err := os.ReadFile(clientCAFile)
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		return nil, fmt.Errorf("no client CA certificates found in %s", clientCAFile)
	}
	return credentials.NewTLS(&tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{cert},
		ClientCAs:    pool,
		ClientAuth:   tls.RequireAndVerifyClientCert,
	}), nil
}
