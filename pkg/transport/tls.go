/*
 *     Copyright 2020 The Dragonfly Authors
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

package transport

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
)
import (
	perrors "github.com/pkg/errors"
)

// TlsConfigBuilder  tls config builder interface
type TlsConfigBuilder interface {
	BuildTlsConfig() (*tls.Config, error)
}

// ServerTlsConfigBuilder impl TlsConfigBuilder for server
type ServerTlsConfigBuilder struct {
	ServerKeyCertChainPath        string
	ServerPrivateKeyPath          string
	ServerKeyPassword             string
	ServerTrustCertCollectionPath string
}

// BuildTlsConfig impl TlsConfigBuilder method
func (s *ServerTlsConfigBuilder) BuildTlsConfig() (*tls.Config, error) {
	var (
		err         error
		certPem     []byte
		certificate tls.Certificate
		certPool    *x509.CertPool
		config      *tls.Config
	)
	if certificate, err = tls.LoadX509KeyPair(s.ServerKeyCertChainPath, s.ServerPrivateKeyPath); err != nil {
		log.Error(fmt.Sprintf("tls.LoadX509KeyPair(certs{%s}, privateKey{%s}) = err:%+v",
			s.ServerKeyCertChainPath, s.ServerPrivateKeyPath, perrors.WithStack(err)))
		return nil, err
	}
	config = &tls.Config{
		InsecureSkipVerify: true, // do not verify peer certs
		ClientAuth:         tls.RequireAnyClientCert,
		Certificates:       []tls.Certificate{certificate},
	}

	if s.ServerTrustCertCollectionPath != "" {
		certPem, err = ioutil.ReadFile(s.ServerTrustCertCollectionPath)
		if err != nil {
			log.Error(fmt.Errorf("ioutil.ReadFile(certFile{%s}) = err:%+v", s.ServerTrustCertCollectionPath, perrors.WithStack(err)))
			return nil, err
		}
		certPool = x509.NewCertPool()
		if ok := certPool.AppendCertsFromPEM(certPem); !ok {
			log.Error("failed to parse root certificate file")
			return nil, err
		}
		config.ClientCAs = certPool
		config.ClientAuth = tls.RequireAnyClientCert
		config.InsecureSkipVerify = false
	}
	return config, nil
}

// ClientTlsConfigBuilder impl TlsConfigBuilder for client
type ClientTlsConfigBuilder struct {
	ClientKeyCertChainPath        string
	ClientPrivateKeyPath          string
	ClientKeyPassword             string
	ClientTrustCertCollectionPath string
}

// BuildTlsConfig impl TlsConfigBuilder method
func (c *ClientTlsConfigBuilder) BuildTlsConfig() (*tls.Config, error) {

	cert, err := tls.LoadX509KeyPair(c.ClientTrustCertCollectionPath, c.ClientPrivateKeyPath)
	if err != nil {
		log.Error(fmt.Sprintf("Unable to load X509 Key Pair %v", err))
		return nil, err
	}
	certBytes, err := ioutil.ReadFile(c.ClientTrustCertCollectionPath)
	if err != nil {
		log.Error(fmt.Sprintf("Unable to read pem file: %s", c.ClientTrustCertCollectionPath))
		return nil, err
	}
	clientCertPool := x509.NewCertPool()
	ok := clientCertPool.AppendCertsFromPEM(certBytes)
	if !ok {
		log.Error("failed to parse root certificate")
		return nil, err
	}
	return &tls.Config{
		RootCAs:            clientCertPool,
		Certificates:       []tls.Certificate{cert},
		InsecureSkipVerify: true,
	}, nil
}
