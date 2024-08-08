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

package rpc

import (
	"context"

	"github.com/juju/ratelimit"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"d7y.io/dragonfly/v2/internal/dferrors"
	logger "d7y.io/dragonfly/v2/internal/dflog"
)

// Refresher is the interface for refreshing dynconfig.
type Refresher interface {
	Refresh() error
}

// RefresherUnaryClientInterceptor returns a new unary client interceptor that refresh dynconfig addresses when calling error.
func RefresherUnaryClientInterceptor(r Refresher) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		err := invoker(ctx, method, req, reply, cc, opts...)
		if s, ok := status.FromError(err); ok {
			if s.Code() == codes.ResourceExhausted || s.Code() == codes.Unavailable {
				logger.Errorf("refresh dynconfig addresses when unary client calling error: %s %#v %v", method, req, err)

				// nolint
				r.Refresh()
			}
		}

		return err
	}
}

// RefresherStreamClientInterceptor returns a new stream client interceptor that refresh dynconfig addresses when calling error.
func RefresherStreamClientInterceptor(r Refresher) grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		clientStream, err := streamer(ctx, desc, cc, method, opts...)
		if s, ok := status.FromError(err); ok {
			if s.Code() == codes.ResourceExhausted || s.Code() == codes.Unavailable {
				logger.Errorf("refresh dynconfig addresses when stream clinet calling error: %s %v", method, err)

				// nolint
				r.Refresh()
			}
		}
		return clientStream, err
	}
}

// RateLimiterInterceptor is the interface for ratelimit interceptor.
type RateLimiterInterceptor struct {
	// tokenBucket is token bucket of ratelimit.
	tokenBucket *ratelimit.Bucket
}

// NewRateLimiterInterceptor returns a RateLimiterInterceptor instance.
func NewRateLimiterInterceptor(qps float64, burst int64) *RateLimiterInterceptor {
	return &RateLimiterInterceptor{
		tokenBucket: ratelimit.NewBucketWithRate(qps, burst),
	}
}

// Limit is the predicate which limits the requests.
func (r *RateLimiterInterceptor) Limit() bool {
	if r.tokenBucket.TakeAvailable(1) == 0 {
		return true
	}

	return false
}

// ConvertErrorUnaryServerInterceptor returns a new unary server interceptor that convert error when trigger custom error.
func ConvertErrorUnaryServerInterceptor(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	h, err := handler(ctx, req)
	if err != nil {
		return h, dferrors.ConvertDfErrorToGRPCError(err)
	}

	return h, nil
}

// ConvertErrorStreamServerInterceptor returns a new stream server interceptor that convert error when trigger custom error.
func ConvertErrorStreamServerInterceptor(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	if err := handler(srv, ss); err != nil {
		return dferrors.ConvertDfErrorToGRPCError(err)
	}

	return nil
}

// ConvertErrorUnaryClientInterceptor returns a new unary client interceptor that convert error when trigger custom error.
func ConvertErrorUnaryClientInterceptor(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	if err := invoker(ctx, method, req, reply, cc, opts...); err != nil {
		return dferrors.ConvertGRPCErrorToDfError(err)
	}

	return nil
}

// ConvertErrorStreamClientInterceptor returns a new stream client interceptor that convert error when trigger custom error.
func ConvertErrorStreamClientInterceptor(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	s, err := streamer(ctx, desc, cc, method, opts...)
	if err != nil {
		return nil, dferrors.ConvertGRPCErrorToDfError(err)
	}

	return s, nil
}
