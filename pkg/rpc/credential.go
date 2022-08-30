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

	"github.com/johanbrandhorst/certify"
	"google.golang.org/grpc/credentials"
)

const (
	// ForceTLSPolicy is both ClientHandshake and
	// ServerHandshake are only support tls.
	ForceTLSPolicy = "force"

	// PreferTLSPolicy is ServerHandshake supports tls and
	// insecure (non-tls), ClientHandshake will only support tls.
	PreferTLSPolicy = "prefer"

	// DefaultTLSPolicy is ServerHandshake supports tls
	// and insecure (non-tls), ClientHandshake will only support tls.
	DefaultTLSPolicy = "default"
)

// NewServerCredentialsByCertify returns server transport credentials by certify.
func NewServerCredentialsByCertify(tlsPolicy string, tlsVerify bool, tlsCACert *tls.Certificate, certifyClient *certify.Certify) (credentials.TransportCredentials, error) {
	tlsConfig := &tls.Config{
		GetCertificate: certifyClient.GetCertificate,
	}

	switch tlsPolicy {
	case DefaultTLSPolicy, PreferTLSPolicy:
		return NewMuxTransportCredentials(tlsConfig,
			WithTLSPreferClientHandshake(tlsPolicy == PreferTLSPolicy)), nil
	case ForceTLSPolicy:
		if !tlsVerify {
			return credentials.NewTLS(tlsConfig), nil
		}

		certPool := x509.NewCertPool()
		for _, cert := range tlsCACert.Certificate {
			if !certPool.AppendCertsFromPEM(cert) {
				return nil, errors.New("invalid CA Cert")
			}
		}

		tlsConfig.ClientAuth = tls.RequireAndVerifyClientCert
		tlsConfig.ClientCAs = certPool
		return credentials.NewTLS(tlsConfig), nil
	default:
		return nil, fmt.Errorf("invalid tlsPolicy: %s", tlsPolicy)
	}
}
