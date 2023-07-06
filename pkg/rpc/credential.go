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
	"net"

	"github.com/johanbrandhorst/certify"
	"google.golang.org/grpc/credentials"

	logger "d7y.io/dragonfly/v2/internal/dflog"
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
func NewServerCredentialsByCertify(tlsPolicy string, tlsVerify bool, pemClientCAs []byte, certifyClient *certify.Certify) (credentials.TransportCredentials, error) {
	certPool := x509.NewCertPool()
	if !certPool.AppendCertsFromPEM(pemClientCAs) {
		return nil, errors.New("invalid CA Cert")
	}

	tlsConfig := &tls.Config{
		GetCertificate: func(hello *tls.ClientHelloInfo) (*tls.Certificate, error) {
			if hello.ServerName == "" {
				host, _, err := net.SplitHostPort(hello.Conn.LocalAddr().String())
				if err == nil {
					hello.ServerName = host
				} else {
					logger.Warnf("failed to get host from %s: %s", hello.Conn.LocalAddr().String(), err)
				}
			}

			return certifyClient.GetCertificate(hello)
		},
		ClientCAs: certPool,
		RootCAs:   certPool,
	}

	if tlsVerify {
		tlsConfig.ClientAuth = tls.RequireAndVerifyClientCert
	}

	switch tlsPolicy {
	case DefaultTLSPolicy, PreferTLSPolicy:
		return NewMuxTransportCredentials(tlsConfig,
			WithTLSPreferClientHandshake(tlsPolicy == PreferTLSPolicy)), nil
	case ForceTLSPolicy:
		return credentials.NewTLS(tlsConfig), nil
	default:
		return nil, fmt.Errorf("invalid tlsPolicy: %s", tlsPolicy)
	}
}

// NewClientCredentialsByCertify returns client transport credentials by certify.
func NewClientCredentialsByCertify(tlsPolicy string, pemRootCAs []byte, certifyClient *certify.Certify) (credentials.TransportCredentials, error) {
	certPool := x509.NewCertPool()
	if !certPool.AppendCertsFromPEM(pemRootCAs) {
		return nil, errors.New("invalid CA Cert")
	}

	tlsConfig := &tls.Config{
		GetClientCertificate: certifyClient.GetClientCertificate,
		RootCAs:              certPool,
	}

	switch tlsPolicy {
	case DefaultTLSPolicy, PreferTLSPolicy:
		return NewMuxTransportCredentials(tlsConfig,
			WithTLSPreferClientHandshake(tlsPolicy == PreferTLSPolicy)), nil
	case ForceTLSPolicy:
		return credentials.NewTLS(tlsConfig), nil
	default:
		return nil, fmt.Errorf("invalid tlsPolicy: %s", tlsPolicy)
	}
}

// NewClientCredentials returns client transport credentials.
func NewClientCredentials(tlsPolicy string, certs []tls.Certificate, pemRootCAs []byte) (credentials.TransportCredentials, error) {
	certPool := x509.NewCertPool()
	if !certPool.AppendCertsFromPEM(pemRootCAs) {
		return nil, errors.New("invalid CA Cert")
	}

	tlsConfig := &tls.Config{
		RootCAs: certPool,
	}

	if len(certs) > 0 {
		tlsConfig.Certificates = certs
	}

	switch tlsPolicy {
	case DefaultTLSPolicy, PreferTLSPolicy:
		return NewMuxTransportCredentials(tlsConfig,
			WithTLSPreferClientHandshake(tlsPolicy == PreferTLSPolicy)), nil
	case ForceTLSPolicy:
		return credentials.NewTLS(tlsConfig), nil
	default:
		return nil, fmt.Errorf("invalid tlsPolicy: %s", tlsPolicy)
	}
}
