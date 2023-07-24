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
	"crypto"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"time"

	"github.com/johanbrandhorst/certify"
	"google.golang.org/protobuf/types/known/durationpb"

	securityv1 "d7y.io/api/v2/pkg/apis/security/v1"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	securityclient "d7y.io/dragonfly/v2/pkg/rpc/security/client"
)

var (
	// defaultSubjectOrganization is default organization of subject.
	defaultSubjectOrganization = []string{"Dragonfly"}

	// defaultValidityPeriod is default validity period of certificate.
	defaultValidityPeriod time.Duration = 180 * 24 * time.Hour
)

// dragonflyIssuer provides issuer function.
type dragonflyIssuer struct {
	securityClient securityclient.V1
	validityPeriod time.Duration
}

// Option is a functional option for configuring the dragonflyIssuer.
type Option func(i *dragonflyIssuer)

// WithValidityPeriod set the validityPeriod for dragonflyIssuer.
func WithValidityPeriod(d time.Duration) Option {
	return func(i *dragonflyIssuer) {
		i.validityPeriod = d
	}
}

// NewDragonflyIssuer returns a new certify.Issuer instence.
func NewDragonflyIssuer(securityClient securityclient.V1, opts ...Option) certify.Issuer {
	i := &dragonflyIssuer{
		securityClient: securityClient,
		validityPeriod: defaultValidityPeriod,
	}

	for _, opt := range opts {
		opt(i)
	}

	return i
}

// Issue returns tls Certificate of issuing.
func (i *dragonflyIssuer) Issue(ctx context.Context, commonName string, certConfig *certify.CertConfig) (*tls.Certificate, error) {
	csr, privateKey, err := fromCertifyCertConfig(commonName, certConfig)
	if err != nil {
		return nil, err
	}

	resp, err := i.securityClient.IssueCertificate(ctx, &securityv1.CertificateRequest{
		Csr:            csr,
		ValidityPeriod: durationpb.New(i.validityPeriod),
	})
	if err != nil {
		return nil, err
	}

	leaf, err := x509.ParseCertificate(resp.CertificateChain[0])
	if err != nil {
		return nil, err
	}
	logger.Debugf("issue certificate from manager, common name: %s, issuer: %s", commonName, leaf.Issuer.CommonName)

	return &tls.Certificate{
		Certificate: resp.CertificateChain,
		PrivateKey:  privateKey,
		Leaf:        leaf,
	}, nil
}

// TODO this func is copied from internal package github.com/johanbrandhorst/certify@v1.9.0/internal/csr/csr.go
// fromCertifyCertConfig creates a CSR and private key from the input config and common name.
// It returns the CSR and private key in PEM format.
func fromCertifyCertConfig(commonName string, conf *certify.CertConfig) ([]byte, crypto.PrivateKey, error) {
	pk, err := conf.KeyGenerator.Generate()
	if err != nil {
		return nil, nil, err
	}

	template := &x509.CertificateRequest{
		Subject: pkix.Name{
			CommonName:   commonName,
			Organization: defaultSubjectOrganization,
		},
	}

	if conf != nil {
		template.DNSNames = conf.SubjectAlternativeNames
		template.IPAddresses = conf.IPSubjectAlternativeNames
		template.URIs = conf.URISubjectAlternativeNames
	}

	csr, err := x509.CreateCertificateRequest(rand.Reader, template, pk)
	if err != nil {
		return nil, nil, err
	}

	return csr, pk, nil
}
