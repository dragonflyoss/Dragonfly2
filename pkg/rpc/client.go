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
	"sync"
	"time"

	"d7y.io/dragonfly/v2/internal/dferrors"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/util/mathutils"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap"
	grpc_retry "github.com/grpc-ecosystem/go-grpc-middleware/retry"
	"github.com/grpc-ecosystem/go-grpc-middleware/validator"
	"github.com/grpc-ecosystem/go-grpc-prometheus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"

	"d7y.io/dragonfly/v2/internal/dfnet"
)

type Closer interface {
	Close() error
}

type Connection struct {
	ctx            context.Context
	cancel         context.CancelFunc
	rwMutex        sync.RWMutex
	dialOpts       []grpc.DialOption
	connExpireTime time.Duration
	dialTimeout    time.Duration
	scheme         string
	serverNodes    []dfnet.NetAddr

	resolver             *d7yResolver
	once                 sync.Once
	consistentHashClient *grpc.ClientConn
}

var DefaultClientOpts = []grpc.DialOption{
	grpc.WithUserAgent("Dragonfly2"),
	grpc.FailOnNonTempDialError(true),
	grpc.WithBlock(),
	grpc.WithInitialConnWindowSize(8 * 1024 * 1024),
	grpc.WithInsecure(),
	grpc.WithKeepaliveParams(keepalive.ClientParameters{
		Time:                2 * time.Minute,
		Timeout:             10 * time.Second,
		PermitWithoutStream: false,
	}),
	grpc.WithChainUnaryInterceptor(
		grpc_prometheus.UnaryClientInterceptor,
		grpc_zap.PayloadUnaryClientInterceptor(logger.GrpcLogger.Desugar(), func(ctx context.Context, fullMethodName string) bool {
			return true
		}),
		grpc_validator.UnaryClientInterceptor(),
		grpc_retry.UnaryClientInterceptor(
			grpc_retry.WithBackoff(grpc_retry.BackoffLinear(100*time.Millisecond)),
			grpc_retry.WithCodes(codes.NotFound, codes.Aborted, codes.ResourceExhausted, codes.Unavailable),
		),
	),
	grpc.WithChainStreamInterceptor(
		grpc_prometheus.StreamClientInterceptor,
		grpc_zap.PayloadStreamClientInterceptor(logger.GrpcLogger.Desugar(), func(ctx context.Context, fullMethodName string) bool {
			return true
		}),
		grpc_retry.StreamClientInterceptor(
			grpc_retry.WithBackoff(grpc_retry.BackoffLinear(100*time.Millisecond)),
			grpc_retry.WithCodes(codes.NotFound, codes.Aborted, codes.ResourceExhausted, codes.Unavailable),
		),
	),
}

func ExecuteWithRetry(f func() (interface{}, error), initBackoff float64, maxBackoff float64, maxAttempts int, cause error) (interface{}, error) {
	var res interface{}
	for i := 0; i < maxAttempts; i++ {
		if _, ok := cause.(*dferrors.DfError); ok {
			return res, cause
		}
		if status.Code(cause) == codes.DeadlineExceeded || status.Code(cause) == codes.Canceled {
			return res, cause
		}
		if i > 0 {
			time.Sleep(mathutils.RandBackoff(initBackoff, maxBackoff, 2.0, i))
		}

		res, cause = f()
		if cause == nil {
			break
		}
	}

	return res, cause
}
