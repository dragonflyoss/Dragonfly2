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

//go:generate mockgen -destination mocks/client_v1_mock.go -source client_v1.go -package mocks

package client

import (
	"context"
	"errors"
	"time"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_zap "github.com/grpc-ecosystem/go-grpc-middleware/logging/zap"
	grpc_retry "github.com/grpc-ecosystem/go-grpc-middleware/retry"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"

	securityv1 "d7y.io/api/v2/pkg/apis/security/v1"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/dfnet"
	healthclient "d7y.io/dragonfly/v2/pkg/rpc/health/client"
)

const (
	// contextTimeout is timeout of grpc invoke.
	contextTimeout = 2 * time.Minute

	// maxRetries is maximum number of retries.
	maxRetries = 3

	// backoffWaitBetween is waiting for a fixed period of
	// time between calls in backoff linear.
	backoffWaitBetween = 500 * time.Millisecond
)

// GetV1 returns v1 version of the security client.
func GetV1(ctx context.Context, target string, opts ...grpc.DialOption) (V1, error) {
	conn, err := grpc.DialContext(
		ctx,
		target,
		append([]grpc.DialOption{
			grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
			grpc.WithUnaryInterceptor(grpc_middleware.ChainUnaryClient(
				grpc_prometheus.UnaryClientInterceptor,
				grpc_zap.UnaryClientInterceptor(logger.GrpcLogger.Desugar()),
				grpc_retry.UnaryClientInterceptor(
					grpc_retry.WithMax(maxRetries),
					grpc_retry.WithBackoff(grpc_retry.BackoffLinear(backoffWaitBetween)),
				),
			)),
			grpc.WithStreamInterceptor(grpc_middleware.ChainStreamClient(
				grpc_prometheus.StreamClientInterceptor,
				grpc_zap.StreamClientInterceptor(logger.GrpcLogger.Desugar()),
			)),
		}, opts...)...,
	)
	if err != nil {
		return nil, err
	}

	return &v1{
		CertificateClient: securityv1.NewCertificateClient(conn),
		ClientConn:        conn,
	}, nil
}

// GetClientV1ByAddr returns v1 version of the security client with addresses.
func GetV1ByAddr(ctx context.Context, netAddrs []dfnet.NetAddr, opts ...grpc.DialOption) (V1, error) {
	for _, netAddr := range netAddrs {
		if err := healthclient.Check(context.Background(), netAddr.String(), opts...); err == nil {
			logger.Infof("manager address %s is reachable", netAddr.String())
			return GetV1(ctx, netAddr.Addr, opts...)
		}
		logger.Warnf("manager address %s is unreachable", netAddr.String())
	}

	return nil, errors.New("can not find reachable manager addresses")
}

// ClientV1 is the interface for v1 version of the grpc client.
type V1 interface {
	// IssueCertificate issues certificate for client.
	IssueCertificate(context.Context, *securityv1.CertificateRequest, ...grpc.CallOption) (*securityv1.CertificateResponse, error)

	// Close tears down the ClientConn and all underlying connections.
	Close() error
}

// clientV1 provides v1 version of the security grpc function.
type v1 struct {
	securityv1.CertificateClient
	*grpc.ClientConn
}

// IssueCertificate issues certificate for client.
func (v *v1) IssueCertificate(ctx context.Context, req *securityv1.CertificateRequest, opts ...grpc.CallOption) (*securityv1.CertificateResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, contextTimeout)
	defer cancel()

	return v.CertificateClient.IssueCertificate(ctx, req, opts...)
}
