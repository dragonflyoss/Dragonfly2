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
	"context"
	"crypto/rand"
	"crypto/x509"
	"fmt"
	"math/big"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	securityv1 "d7y.io/api/v2/pkg/apis/security/v1"

	logger "d7y.io/dragonfly/v2/internal/dflog"
)

// securityServerV1 is v1 version of the security grpc server.
type securityServerV1 struct {
	// selfSignedCert is self signed certificate.
	selfSignedCert *SelfSignedCert
}

// newSecurityServerV1 returns v1 version of the security server.
func newSecurityServerV1(selfSignedCert *SelfSignedCert) securityv1.CertificateServer {
	return &securityServerV1{
		selfSignedCert: selfSignedCert,
	}
}

// IssueCertificate issues certificate for client.
func (s *securityServerV1) IssueCertificate(ctx context.Context, req *securityv1.CertificateRequest) (*securityv1.CertificateResponse, error) {
	if s.selfSignedCert == nil {
		return nil, status.Errorf(codes.Unavailable, "ca is missing for this manager instance")
	}

	// Parse csr.
	csr, err := x509.ParseCertificateRequest(req.Csr)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid csr format: %s", err.Error())
	}

	// Check csr signature.
	if err = csr.CheckSignature(); err != nil {
		return nil, err
	}
	logger.Infof("valid csr: %#v", csr.Subject)

	// Check csr ip address.
	if len(csr.IPAddresses) == 0 {
		return nil, status.Errorf(codes.InvalidArgument, "invalid csr ip address")
	}

	// Generate serial number.
	serial, err := rand.Int(rand.Reader, (&big.Int{}).Exp(big.NewInt(2), big.NewInt(159), nil))
	if err != nil {
		return nil, err
	}

	now := time.Now()
	template := x509.Certificate{
		SerialNumber:          serial,
		Subject:               csr.Subject,
		DNSNames:              csr.DNSNames,
		EmailAddresses:        csr.EmailAddresses,
		IPAddresses:           csr.IPAddresses,
		URIs:                  csr.URIs,
		NotBefore:             now.Add(-10 * time.Minute).UTC(),
		NotAfter:              now.Add(req.ValidityPeriod.AsDuration()).UTC(),
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageDataEncipherment | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	}

	cert, err := x509.CreateCertificate(rand.Reader, &template, s.selfSignedCert.X509Cert, csr.PublicKey, s.selfSignedCert.TLSCert.PrivateKey)
	if err != nil {
		return nil, fmt.Errorf("failed to generate certificate, error: %s", err)
	}

	return &securityv1.CertificateResponse{
		CertificateChain: append([][]byte{cert}, s.selfSignedCert.CertChain...),
	}, nil
}
