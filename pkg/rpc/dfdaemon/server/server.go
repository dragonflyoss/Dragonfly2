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

package server

import (
	"time"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_zap "github.com/grpc-ecosystem/go-grpc-middleware/logging/zap"
	grpc_ratelimit "github.com/grpc-ecosystem/go-grpc-middleware/ratelimit"
	grpc_recovery "github.com/grpc-ecosystem/go-grpc-middleware/recovery"
	grpc_validator "github.com/grpc-ecosystem/go-grpc-middleware/validator"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"

	dfdaemonv1 "d7y.io/api/v2/pkg/apis/dfdaemon/v1"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/rpc"
)

const (
	// DefaultQPS is default qps of grpc server.
	DefaultQPS = 10 * 1000

	// DefaultBurst is default burst of grpc server.
	DefaultBurst = 20 * 1000

	// DefaultMaxConnectionIdle is default max connection idle of grpc keepalive.
	DefaultMaxConnectionIdle = 10 * time.Minute

	// DefaultMaxConnectionAge is default max connection age of grpc keepalive.
	DefaultMaxConnectionAge = 12 * time.Hour

	// DefaultMaxConnectionAgeGrace is default max connection age grace of grpc keepalive.
	DefaultMaxConnectionAgeGrace = 5 * time.Minute
)

type DaemonServer interface {
	dfdaemonv1.DaemonServer
}

// New returns a grpc server instance and register service on grpc server.
func New(svr dfdaemonv1.DaemonServer, healthServer healthpb.HealthServer, opts ...grpc.ServerOption) *grpc.Server {
	limiter := rpc.NewRateLimiterInterceptor(DefaultQPS, DefaultBurst)

	grpcServer := grpc.NewServer(append([]grpc.ServerOption{
		grpc.KeepaliveParams(keepalive.ServerParameters{
			MaxConnectionIdle:     DefaultMaxConnectionIdle,
			MaxConnectionAge:      DefaultMaxConnectionAge,
			MaxConnectionAgeGrace: DefaultMaxConnectionAgeGrace,
		}),
		grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(
			grpc_ratelimit.UnaryServerInterceptor(limiter),
			rpc.ConvertErrorUnaryServerInterceptor,
			otelgrpc.UnaryServerInterceptor(),
			grpc_prometheus.UnaryServerInterceptor,
			grpc_zap.UnaryServerInterceptor(logger.GrpcLogger.Desugar()),
			grpc_validator.UnaryServerInterceptor(),
			grpc_recovery.UnaryServerInterceptor(),
		)),
		grpc.StreamInterceptor(grpc_middleware.ChainStreamServer(
			grpc_ratelimit.StreamServerInterceptor(limiter),
			rpc.ConvertErrorStreamServerInterceptor,
			otelgrpc.StreamServerInterceptor(),
			grpc_prometheus.StreamServerInterceptor,
			grpc_zap.StreamServerInterceptor(logger.GrpcLogger.Desugar()),
			grpc_validator.StreamServerInterceptor(),
			grpc_recovery.StreamServerInterceptor(),
		)),
	}, opts...)...)

	// Register servers on grpc server.
	dfdaemonv1.RegisterDaemonServer(grpcServer, svr)

	// Register health on grpc server.
	healthpb.RegisterHealthServer(grpcServer, healthServer)

	// Register reflection on grpc server.
	reflection.Register(grpcServer)
	return grpcServer
}
