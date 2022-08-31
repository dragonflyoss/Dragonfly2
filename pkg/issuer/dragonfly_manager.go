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

package issuer

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"net"
	"time"

	"github.com/johanbrandhorst/certify"
)

// GC provides issuer function.
type dragonflyManagerIssuer struct {
	tlsCACert        *tls.Certificate
	dnsNames         []string
	ipAddresses      []net.IP
	validityDuration time.Duration
}

// Option is a functional option for configuring the dragonflyManagerIssuer.
type Option func(i *dragonflyManagerIssuer)

// WithDNSNames set the dnsNames for dragonflyManagerIssuer.
func WithDNSNames(dnsNames []string) Option {
	return func(i *dragonflyManagerIssuer) {
		i.dnsNames = dnsNames
	}
}

// WithIPAddresses set the ipAddresses for dragonflyManagerIssuer.
func WithIPAddresses(rawIPAddrs []string) Option {
	return func(i *dragonflyManagerIssuer) {
		var ipAddrs []net.IP
		for _, rawIPAddr := range rawIPAddrs {
			ipAddrs = append(ipAddrs, net.ParseIP(rawIPAddr))
		}

		i.ipAddresses = ipAddrs
	}
}

// WithValidityDuration set the validityDuration for dragonflyManagerIssuer.
func WithValidityDuration(d time.Duration) Option {
	return func(i *dragonflyManagerIssuer) {
		i.validityDuration = d
	}
}

// NewDragonflyManagerIssuer returns a new certify.Issuer instence.
func NewDragonflyManagerIssuer(tlsCACert *tls.Certificate, opts ...Option) certify.Issuer {
	i := &dragonflyManagerIssuer{
		tlsCACert: tlsCACert,
	}

	for _, opt := range opts {
		opt(i)
	}

	return i
}

// Issue returns tls Certificate of issuing.
func (i *dragonflyManagerIssuer) Issue(ctx context.Context, commonName string, certConfig *certify.CertConfig) (*tls.Certificate, error) {
	x509CACert, err := x509.ParseCertificate(i.tlsCACert.Certificate[0])
	if err != nil {
		return nil, err
	}

	pk, err := rsa.GenerateKey(rand.Reader, 4096)
	if err != nil {
		return nil, err
	}

	serial, err := rand.Int(rand.Reader, (&big.Int{}).Exp(big.NewInt(2), big.NewInt(159), nil))
	if err != nil {
		return nil, err
	}

	now := time.Now()
	template := x509.Certificate{
		SerialNumber: serial,
		Subject: pkix.Name{
			CommonName:   commonName,
			Organization: defaultSubjectOrganization,
		},
		DNSNames:              append(certConfig.SubjectAlternativeNames, i.dnsNames...),
		IPAddresses:           append(certConfig.IPSubjectAlternativeNames, i.ipAddresses...),
		URIs:                  certConfig.URISubjectAlternativeNames,
		NotBefore:             now.Add(-10 * time.Minute).UTC(),
		NotAfter:              now.Add(i.validityDuration).UTC(),
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageDataEncipherment | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	}

	cert, err := x509.CreateCertificate(rand.Reader, &template, x509CACert, &pk.PublicKey, i.tlsCACert.PrivateKey)
	if err != nil {
		return nil, err
	}

	leaf, err := x509.ParseCertificate(cert)
	if err != nil {
		return nil, err
	}

	return &tls.Certificate{
		Certificate: append([][]byte{cert}, i.tlsCACert.Certificate...),
		PrivateKey:  pk,
		Leaf:        leaf,
	}, nil
}
