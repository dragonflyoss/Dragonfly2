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

package transfer

import (
	"context"

	"golang.org/x/net/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"k8s.io/apimachinery/pkg/util/sets"

	"d7y.io/dragonfly/v2/pkg/rpc/pickreq"
)

// UnaryClientInterceptor returns a new unary client interceptors that performs migrate client.
func UnaryClientInterceptor(optFuncs ...CallOption) grpc.UnaryClientInterceptor {
	intOpts := reuseOrNewWithCallOptions(defaultOptions, optFuncs)
	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		var lastErr error
		var p peer.Peer
		lastErr = invoker(ctx, method, req, reply, cc, append(opts, grpc.Peer(&p))...)
		if lastErr == nil {
			return nil
		}
		for {
			ctx = callContext(ctx, p)
			currentErr := invoker(ctx, method, req, reply, cc, append(opts, grpc.Peer(&p))...)
			if currentErr == nil {
				return nil
			}
			if isUnTransferableError(currentErr, intOpts) {
				logTrace(ctx, "grpc_transfer server addr: %s, got unable transfer err: %v", p.Addr, currentErr)
				return lastErr
			}
			lastErr = currentErr
		}
	}
}

// StreamClientInterceptor returns a new stream client interceptor that performs migrate client.
func StreamClientInterceptor(optFuncs ...CallOption) grpc.StreamClientInterceptor {
	intOpts := reuseOrNewWithCallOptions(defaultOptions, optFuncs)
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		var lastErr error
		var newStreamer grpc.ClientStream
		var p peer.Peer
		newStreamer, lastErr = streamer(ctx, desc, cc, method, append(opts, grpc.Peer(&p))...)
		if lastErr == nil {
			return newStreamer, lastErr
		}
		for {
			ctx = callContext(ctx, p)
		}

		newStreamer, lastErr := streamer(ctx, desc, cc, method, opts...)
		return newStreamer, lastErr
	}
}

func logTrace(ctx context.Context, format string, a ...interface{}) {
	tr, ok := trace.FromContext(ctx)
	if !ok {
		return
	}
	tr.LazyPrintf(format, a...)
}

func isContextError(err error) bool {
	code := status.FromContextError(err).Code()
	return code == codes.DeadlineExceeded || code == codes.Canceled
}

func isUnTransferableError(err error, callOpts *options) bool {
	errCode := status.Code(err)
	if isContextError(err) {
		return true
	}
	for _, code := range callOpts.codes {
		if code == errCode {
			return true
		}
	}
	return false
}

func callContext(ctx context.Context, failedPeer peer.Peer) context.Context {
	pr, ok := pickreq.FromContext(ctx)
	if !ok {
		pr = new(pickreq.PickRequest)
	}
	if pr.FailedNodes == nil {
		pr.FailedNodes = sets.NewString()
	}
	pr.FailedNodes.Insert(failedPeer.Addr.String())
	return pickreq.NewContext(ctx, pr)
}
