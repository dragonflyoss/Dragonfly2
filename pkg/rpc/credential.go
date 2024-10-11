/*
 *     Copyright 2022 The Dragonfly Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package rpc

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"time"

	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

// NewServerCredentials creates a new server credentials with the given ca certificate, certificate and key.
func NewServerCredentials(caCertFile string, certFile string, keyFile string) (credentials.TransportCredentials, error) {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("failed to load server certificate: %v", err)
	}

	caCertPool := x509.NewCertPool()
	caCertBytes, err := os.ReadFile(caCertFile)
	if err != nil {
		return nil, fmt.Errorf("failed to read ca certificate: %v", err)
	}

	if !caCertPool.AppendCertsFromPEM(caCertBytes) {
		return nil, errors.New("failed to append ca certificate")
	}

	return credentials.NewTLS(&tls.Config{
		ClientCAs:    caCertPool,
		Certificates: []tls.Certificate{cert},
		ClientAuth:   tls.RequireAndVerifyClientCert,
	}), nil
}

// NewClientCredentials creates a new client credentials with the given ca certificate, certificate and key.
func NewClientCredentials(caCertFile string, certFile string, keyFile string) (credentials.TransportCredentials, error) {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("failed to load client certificate: %v", err)
	}

	caCertPool := x509.NewCertPool()
	caCertBytes, err := os.ReadFile(caCertFile)
	if err != nil {
		return nil, fmt.Errorf("failed to read ca certificate: %v", err)
	}

	if !caCertPool.AppendCertsFromPEM(caCertBytes) {
		return nil, errors.New("failed to append ca certificate")
	}

	return credentials.NewTLS(&tls.Config{
		RootCAs:      caCertPool,
		Certificates: []tls.Certificate{cert},
		VerifyPeerCertificate: func(rawCerts [][]byte, verifiedChains [][]*x509.Certificate) error {
			// Parse the raw certificates.
			certs := make([]*x509.Certificate, len(rawCerts))
			for i, rawCert := range rawCerts {
				cert, err := x509.ParseCertificate(rawCert)
				if err != nil {
					return err
				}

				certs[i] = cert
			}

			// Verify the certificate chain.
			opts := x509.VerifyOptions{
				Roots:       caCertPool,
				CurrentTime: time.Now(),
				// Skip hostname verification.
				DNSName:       "",
				Intermediates: x509.NewCertPool(),
			}

			for i, cert := range certs {
				if i == 0 {
					continue
				}

				opts.Intermediates.AddCert(cert)
			}

			_, err := certs[0].Verify(opts)
			return err
		},
	}), nil
}

// NewInsecureCredentials creates a new insecure credentials.
func NewInsecureCredentials() credentials.TransportCredentials {
	return insecure.NewCredentials()
}
