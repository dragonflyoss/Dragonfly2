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
	"time"

	"github.com/johanbrandhorst/certify"

	logger "d7y.io/dragonfly/v2/internal/dflog"
)

const (
	// defaultManagerValidityPeriod is default manager validity period of certificate.
	defaultManagerValidityPeriod = 10 * 365 * 24 * time.Hour
)

// dragonflyManagerIssuer provides manager issuer function.
type dragonflyManagerIssuer struct {
	tlsCACert      *tls.Certificate
	validityPeriod time.Duration
}

// ManagerOption is a functional option for configuring the dragonflyManagerIssuer.
type ManagerOption func(i *dragonflyManagerIssuer)

// WithManagerValidityPeriod set the manager validityPeriod for dragonflyManagerIssuer.
func WithManagerValidityPeriod(d time.Duration) ManagerOption {
	return func(i *dragonflyManagerIssuer) {
		i.validityPeriod = d
	}
}

// NewDragonflyManagerIssuer returns a new certify.Issuer instence.
func NewDragonflyManagerIssuer(tlsCACert *tls.Certificate, opts ...ManagerOption) certify.Issuer {
	i := &dragonflyManagerIssuer{
		tlsCACert:      tlsCACert,
		validityPeriod: defaultManagerValidityPeriod,
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
		DNSNames:              certConfig.SubjectAlternativeNames,
		IPAddresses:           certConfig.IPSubjectAlternativeNames,
		URIs:                  certConfig.URISubjectAlternativeNames,
		NotBefore:             now.Add(-10 * time.Minute).UTC(),
		NotAfter:              now.Add(i.validityPeriod).UTC(),
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
	logger.Debugf("issue certificate from manager, common name: %s, issuer: %s", commonName, leaf.Issuer.CommonName)

	return &tls.Certificate{
		Certificate: append([][]byte{cert}, i.tlsCACert.Certificate...),
		PrivateKey:  pk,
		Leaf:        leaf,
	}, nil
}
