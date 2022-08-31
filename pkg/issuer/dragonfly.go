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
	"crypto/ecdsa"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"strings"
	"time"

	"github.com/johanbrandhorst/certify"

	securityv1 "d7y.io/api/pkg/apis/security/v1"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	managerclient "d7y.io/dragonfly/v2/pkg/rpc/manager/client"
)

var (
	// defaultSubjectOrganization is default organization of subject.
	defaultSubjectOrganization = []string{"Dragonfly"}

	// defaultValidityDuration is default validity duration of certificate.
	defaultValidityDuration = 180 * 24 * int64(time.Hour.Seconds())
)

type dragonflyIssuer struct {
	client managerclient.Client
}

func NewDragonflyIssuer(client managerclient.Client) certify.Issuer {
	return &dragonflyIssuer{client: client}
}

func (d *dragonflyIssuer) Issue(ctx context.Context, commonName string, certConfig *certify.CertConfig) (*tls.Certificate, error) {
	csrPEM, privateKeyPEM, err := fromCertifyCertConfig(commonName, certConfig)
	if err != nil {
		logger.Errorf("gen csr and private pem error: %s", err.Error())
		return nil, err
	}

	request := &securityv1.CertificateRequest{
		Csr:              string(csrPEM),
		ValidityDuration: defaultValidityDuration,
	}

	response, err := d.client.IssueCertificate(ctx, request)
	if err != nil {
		logger.Errorf("call manager IssueCertificate error: %s", err.Error())
		return nil, err
	}

	certPEM := []byte(strings.Join(response.CertificateChain, "\n"))
	tlsCert, err := tls.X509KeyPair(certPEM, privateKeyPEM)
	if err != nil {
		logger.Errorf("load x509 key pair error: %s", err.Error())
		return nil, err
	}

	tlsCert.Leaf, err = x509.ParseCertificate(tlsCert.Certificate[0])
	if err != nil {
		return nil, err
	}

	return &tlsCert, nil
}

// TODO this func is copied from internal package github.com/johanbrandhorst/certify@v1.9.0/internal/csr/csr.go
// fromCertifyCertConfig creates a CSR and private key from the input config and common name.
// It returns the CSR and private key in PEM format.
func fromCertifyCertConfig(commonName string, conf *certify.CertConfig) ([]byte, []byte, error) {
	pk, err := conf.KeyGenerator.Generate()
	if err != nil {
		return nil, nil, err
	}

	keyPEM, err := marshal(pk)
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

	csrPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE REQUEST",
		Bytes: csr,
	})

	return csrPEM, keyPEM, nil
}

// TODO this func is copied from internal package github.com/johanbrandhorst/certify@v1.9.0/internal/keys/keys.go
// marshal a private key to PEM format.
func marshal(pk crypto.PrivateKey) ([]byte, error) {
	switch pk := pk.(type) {
	case *rsa.PrivateKey:
		keyBytes := x509.MarshalPKCS1PrivateKey(pk)
		block := pem.Block{
			Type:  "RSA PRIVATE KEY",
			Bytes: keyBytes,
		}
		return pem.EncodeToMemory(&block), nil
	case *ecdsa.PrivateKey:
		keyBytes, err := x509.MarshalECPrivateKey(pk)
		if err != nil {
			return nil, err
		}
		block := pem.Block{
			Type:  "EC PRIVATE KEY",
			Bytes: keyBytes,
		}
		return pem.EncodeToMemory(&block), nil
	}

	return nil, errors.New("unsupported private key type")
}
