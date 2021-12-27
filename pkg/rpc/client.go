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
	"time"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_retry "github.com/grpc-ecosystem/go-grpc-middleware/retry"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

type Closer interface {
	Close() error
}

var DefaultClientOpts = []grpc.DialOption{
	grpc.FailOnNonTempDialError(true),
	grpc.WithBlock(),
	grpc.WithInitialConnWindowSize(8 * 1024 * 1024),
	grpc.WithInsecure(),
	grpc.WithKeepaliveParams(keepalive.ClientParameters{
		Time:    2 * time.Minute,
		Timeout: 10 * time.Second,
	}),
	grpc.WithStreamInterceptor(grpc_middleware.ChainStreamClient(
		streamClientInterceptor,
		grpc_retry.StreamClientInterceptor(
			grpc_retry.WithMax(3),
			grpc_retry.WithBackoff(grpc_retry.BackoffLinearWithJitter(time.Second, 0.5)),
		),
	)),
	grpc.WithUnaryInterceptor(grpc_middleware.ChainUnaryClient(
		unaryClientInterceptor,
		grpc_retry.UnaryClientInterceptor(
			grpc_retry.WithMax(3),
			grpc_retry.WithBackoff(grpc_retry.BackoffLinearWithJitter(time.Second, 0.5)),
		),
	)),
}
