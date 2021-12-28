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

package rpc

import (
	"context"
	"time"

	d7yLogger "d7y.io/dragonfly/v2/internal/dflog"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap"
	"github.com/grpc-ecosystem/go-grpc-middleware/retry"
	"github.com/grpc-ecosystem/go-grpc-middleware/validator"
	"github.com/grpc-ecosystem/go-grpc-prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

var DefaultClientOpts = []grpc.DialOption{
	grpc.WithUserAgent("Dragonfly2"),
	grpc.FailOnNonTempDialError(true),
	grpc.WithBlock(),
	grpc.WithInitialConnWindowSize(8 * 1024 * 1024),
	grpc.WithTransportCredentials(insecure.NewCredentials()),
	grpc.WithKeepaliveParams(keepalive.ClientParameters{
		Time:                2 * time.Minute,
		Timeout:             10 * time.Second,
		PermitWithoutStream: false,
	}),
	grpc.WithChainUnaryInterceptor(
		grpc_prometheus.UnaryClientInterceptor,
		grpc_zap.PayloadUnaryClientInterceptor(d7yLogger.GrpcLogger.Desugar(), func(ctx context.Context, fullMethodName string) bool {
			return true
		}),
		grpc_validator.UnaryClientInterceptor(),
		grpc_retry.UnaryClientInterceptor(
			grpc_retry.WithBackoff(grpc_retry.BackoffLinear(100*time.Millisecond)),
			grpc_retry.WithCodes(codes.NotFound, codes.Aborted, codes.ResourceExhausted, codes.Unavailable, codes.Unknown),
			grpc_retry.WithMax(3),
		),
	),
	grpc.WithChainStreamInterceptor(
		grpc_prometheus.StreamClientInterceptor,
		grpc_zap.PayloadStreamClientInterceptor(d7yLogger.GrpcLogger.Desugar(), func(ctx context.Context, fullMethodName string) bool {
			return true
		}),
		grpc_retry.StreamClientInterceptor(
			grpc_retry.WithBackoff(grpc_retry.BackoffLinear(100*time.Millisecond)),
			grpc_retry.WithCodes(codes.NotFound, codes.Aborted, codes.ResourceExhausted, codes.Unavailable, codes.Unknown),
			grpc_retry.WithMax(3),
		),
	),
}
