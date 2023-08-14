/*
 *     Copyright 2023 The Dragonfly Authors
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

package rpcserver

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"testing"
	"time"

	testifyassert "github.com/stretchr/testify/assert"
	testifyrequire "github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/durationpb"

	securityv1 "d7y.io/api/v2/pkg/apis/security/v1"
)

func TestIssueCertificate(t *testing.T) {
	assert := testifyassert.New(t)
	require := testifyrequire.New(t)
	caCert, caKey := genCA()

	testCases := []struct {
		name   string
		peerIP string
	}{
		{
			name:   "ipv4",
			peerIP: "1.1.1.1",
		},
		{
			name:   "ipv6",
			peerIP: "1::1",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			template := &x509.CertificateRequest{
				IPAddresses: []net.IP{net.ParseIP(tc.peerIP)},
				Subject: pkix.Name{
					Country:            []string{"China"},
					Organization:       []string{"Dragonfly"},
					OrganizationalUnit: []string{"Development"},
				},
			}

			pk, err := rsa.GenerateKey(rand.Reader, 4096)
			require.Nilf(err, "GenerateKey should be ok")

			csr, err := x509.CreateCertificateRequest(rand.Reader, template, pk)
			require.Nilf(err, "CreateCertificateRequest should be ok")

			ca, err := tls.X509KeyPair([]byte(caCert), []byte(caKey))
			require.Nilf(err, "parse cert and private key should be ok")

			x509CACert, err := x509.ParseCertificate(ca.Certificate[0])
			if err != nil {
				t.Fatal(err)
			}

			securityServerV1 := newSecurityServerV1(&SelfSignedCert{
				TLSCert:   &ca,
				X509Cert:  x509CACert,
				CertChain: ca.Certificate,
			})
			require.Nilf(err, "newServer should be ok")

			resp, err := securityServerV1.IssueCertificate(
				context.Background(),
				&securityv1.CertificateRequest{
					Csr:            csr,
					ValidityPeriod: durationpb.New(time.Hour),
				})

			assert.Nilf(err, "IssueCertificate should be ok")
			assert.NotNilf(resp, "IssueCertificate should not be nil")
			assert.Equal(len(resp.CertificateChain), 2)

			cert := readCert(resp.CertificateChain[0])
			assert.Equal(len(cert.IPAddresses), 1)
			assert.True(cert.IPAddresses[0].Equal(net.ParseIP(tc.peerIP)))

			assert.Equal(cert.KeyUsage, x509.KeyUsageDigitalSignature|x509.KeyUsageDataEncipherment|x509.KeyUsageKeyEncipherment)
			assert.Equal(cert.ExtKeyUsage, []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth})
		})
	}
}

func genCA() (cert, key string) {
	pk, err := rsa.GenerateKey(rand.Reader, 4096)
	if err != nil {
		panic(err)
	}

	ca := &x509.Certificate{
		SerialNumber: big.NewInt(2022),
		Subject: pkix.Name{
			Country:            []string{"China"},
			Organization:       []string{"Dragonfly"},
			OrganizationalUnit: []string{"Development"},
			Locality:           []string{"Hangzhou"},
			Province:           []string{"Zhejiang"},
		},
		NotBefore:             time.Now().Add(-10 * time.Minute).UTC(),
		NotAfter:              time.Now().Add(time.Hour).UTC(),
		BasicConstraintsValid: true,
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	}

	caBytes, err := x509.CreateCertificate(rand.Reader, ca, ca, &pk.PublicKey, pk)
	if err != nil {
		panic(err)
	}

	var (
		caCertPEM       bytes.Buffer
		caPrivateKeyPEM bytes.Buffer
	)

	if err = pem.Encode(&caCertPEM, &pem.Block{Type: "CERTIFICATE", Bytes: caBytes}); err != nil {
		panic(err)
	}

	if err = pem.Encode(&caPrivateKeyPEM, &pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(pk)}); err != nil {
		panic(err)
	}

	key, cert = caPrivateKeyPEM.String(), caCertPEM.String()
	return
}

func readCert(der []byte) *x509.Certificate {
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		panic(err)
	}

	return cert
}
