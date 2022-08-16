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

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Refresher is the interface for refreshing dynconfig.
type Refresher interface {
	Refresh() error
}

// UnaryClientInterceptor returns a new unary client interceptor that refresh dynconfig addresses when calling error.
func RefresherUnaryClientInterceptor(r Refresher) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		err := invoker(ctx, method, req, reply, cc, opts...)
		if s, ok := status.FromError(err); ok {
			if s.Code() == codes.ResourceExhausted || s.Code() == codes.Unavailable {
				// nolint
				r.Refresh()
			}
		}

		return err
	}
}

// StreamClientInterceptor returns a new stream client interceptor that refresh dynconfig addresses when calling error.
func RefresherStreamClientInterceptor(r Refresher) grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		clientStream, err := streamer(ctx, desc, cc, method, opts...)
		if s, ok := status.FromError(err); ok {
			if s.Code() == codes.ResourceExhausted || s.Code() == codes.Unavailable {
				// nolint
				r.Refresh()
			}
		}
		return clientStream, err
	}
}
