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

package rpcserver

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	securityv1 "d7y.io/api/pkg/apis/security/v1"

	logger "d7y.io/dragonfly/v2/internal/dflog"
)

func (s *Server) IssueCertificate(ctx context.Context, req *securityv1.CertificateRequest) (*securityv1.CertificateResponse, error) {
	if s.cert == nil {
		return nil, status.Errorf(codes.Unavailable, "ca is missing for this manager instance")
	}

	var (
		ip  string
		err error
	)
	p, ok := peer.FromContext(ctx)
	if !ok {
		return nil, status.Errorf(codes.InvalidArgument, "invalid grpc peer info")
	}

	if addr, ok := p.Addr.(*net.TCPAddr); ok {
		ip = addr.IP.String()
	} else {
		ip, _, err = net.SplitHostPort(p.Addr.String())
		if err != nil {
			return nil, err
		}
	}

	// Decode csr pem.
	block, _ := pem.Decode([]byte(req.Csr))
	if block == nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid csr format")
	}

	// Parse csr.
	csr, err := x509.ParseCertificateRequest(block.Bytes)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid csr format: %s", err.Error())
	}

	// Check csr signature.
	// TODO check csr common name and so on.
	if err = csr.CheckSignature(); err != nil {
		return nil, err
	}

	serial, err := rand.Int(rand.Reader, (&big.Int{}).Exp(big.NewInt(2), big.NewInt(159), nil))
	if err != nil {
		return nil, err
	}

	// Check certificate duration.
	now := time.Now()
	duration := time.Duration(req.ValidityDuration) * time.Second
	if duration == 0 {
		duration = time.Hour
	}

	logger.Infof("valid csr: %#v", csr.Subject)

	template := x509.Certificate{
		SerialNumber:          serial,
		Subject:               csr.Subject,
		IPAddresses:           []net.IP{net.ParseIP(ip)}, // only valid for peer ip
		NotBefore:             now.Add(-10 * time.Minute).UTC(),
		NotAfter:              now.Add(duration).UTC(),
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageDataEncipherment | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	}

	cert, err := x509.CreateCertificate(rand.Reader, &template, s.x509Cert, csr.PublicKey, s.cert.PrivateKey)
	if err != nil {
		return nil, fmt.Errorf("failed to generate certificate, error: %s", err)
	}

	// Encode into PEM format.
	var certPEM bytes.Buffer
	if err = pem.Encode(&certPEM, &pem.Block{Type: "CERTIFICATE", Bytes: cert}); err != nil {
		return nil, err
	}

	return &securityv1.CertificateResponse{
		CertificateChain: append([]string{certPEM.String()}, s.certChain...),
	}, nil
}
