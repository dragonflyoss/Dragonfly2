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
	grpc_recovery "github.com/grpc-ecosystem/go-grpc-middleware/recovery"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap"
	"github.com/grpc-ecosystem/go-grpc-middleware/validator"
	"github.com/grpc-ecosystem/go-grpc-prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"

	logger "d7y.io/dragonfly/v2/internal/dflog"
)

var DefaultServerOptions = []grpc.ServerOption{
	grpc.ConnectionTimeout(10 * time.Second),
	grpc.InitialConnWindowSize(8 * 1024 * 1024),
	grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
		MinTime:             1 * time.Minute,
		PermitWithoutStream: false,
	}),
	grpc.KeepaliveParams(keepalive.ServerParameters{
		MaxConnectionIdle: 5 * time.Minute,
	}),
	grpc.MaxConcurrentStreams(100),
	grpc.ConnectionTimeout(5 * time.Second),
	grpc.ChainStreamInterceptor(
		grpc_prometheus.StreamServerInterceptor,
		grpc_zap.PayloadStreamServerInterceptor(logger.GrpcLogger.Desugar(), func(ctx context.Context, fullMethodName string, servingObject interface{}) bool {
			return true
		}),
		grpc_validator.StreamServerInterceptor(),
		grpc_recovery.StreamServerInterceptor(grpc_recovery.WithRecoveryHandler(func(p interface{}) (err error) {
			return status.Errorf(codes.Unknown, "panic triggered: %v", p)
		})),
	),
	grpc.ChainUnaryInterceptor(
		grpc_prometheus.UnaryServerInterceptor,
		grpc_zap.PayloadUnaryServerInterceptor(logger.GrpcLogger.Desugar(), func(ctx context.Context, fullMethodName string, servingObject interface{}) bool {
			return true
		}),
		grpc_validator.UnaryServerInterceptor(),
		grpc_recovery.UnaryServerInterceptor(grpc_recovery.WithRecoveryHandler(func(p interface{}) (err error) {
			return status.Errorf(codes.Unknown, "panic triggered: %v", p)
		})),
	),
}
