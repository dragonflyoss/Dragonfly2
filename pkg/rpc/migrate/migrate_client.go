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

package migrate

import (
	"context"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/rpc"
	"d7y.io/dragonfly/v2/pkg/rpc/dfdaemon"
	"google.golang.org/grpc"
	"google.golang.org/grpc/peer"
)

// UnaryClientInterceptor returns a new unary client interceptors that performs migrate client.
func UnaryClientInterceptor() grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		pr, ok := rpc.FromContext(ctx)
		if !ok {
			return invoker(ctx, method, req, reply, cc, opts...)
		}
		callCtx := preCallContext(ctx, pr)
		var p peer.Peer
		invoker(callCtx, method, req, reply, cc, append(opts, grpc.Peer(&p))...)
		return err
	}
}

func preCallContext(ctx context.Context, pr *rpc.PickRequest) context.Context {

	ctx := context.Background()
	pr, ok := FromContext(ctx)
	pr.FailedNodes.Insert(p.Addr.String())
	NewContext(ctx, pr)
	stream, err := rpc.ExecuteWithRetry(func() (interface{}, error) {
		var client dfdaemon.DaemonClient
		var err error
		client, err = drs.dc.getDaemonClient()
		if err != nil {
			return nil, err
		}
		return client.Download(drs.ctx, drs.req, drs.opts...)
	}, drs.InitBackoff, drs.MaxBackOff, drs.MaxAttempts, cause)
	if err != nil {
		logger.WithTaskID(drs.hashKey).Infof("replaceClient: invoke daemon Download failed: %v", err)
		return drs.replaceClient(cause)
	}
	drs.stream = stream.(dfdaemon.Daemon_DownloadClient)
	drs.StreamTimes = 1
	return nil
}

// StreamClientInterceptor returns a new stream client interceptor that performs migrate client.
func StreamClientInterceptor(migrate *migrateCtx) grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		newStreamer, lastErr := streamer(ctx, desc, cc, method, opts...)
		return newStreamer, lastErr
	}
}
