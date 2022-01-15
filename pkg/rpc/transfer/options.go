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
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

var (
	// DefaultUnTransferableCodes is a set of codes that should not transfer server node.
	DefaultUnTransferableCodes = []codes.Code{codes.Code(base.Code_ServerUnavailable)}

	defaultOptions = &options{
		codes: DefaultUnTransferableCodes,
	}
)

type options struct {
	codes []codes.Code
}

// CallOption is a grpc.CallOption that is local to grpc_migrate.
type CallOption struct {
	grpc.EmptyCallOption // make sure we implement private after() and before() fields so we don't panic.
	applyFunc            func(opt *options)
}

func WithCodes(unTransferableCodes ...codes.Code) CallOption {
	return CallOption{applyFunc: func(opt *options) {
		opt.codes = append(unTransferableCodes, DefaultUnTransferableCodes...)
	}}
}

func reuseOrNewWithCallOptions(opt *options, callOptions []CallOption) *options {
	if len(callOptions) == 0 {
		return opt
	}
	optCopy := &options{}
	*optCopy = *opt
	for _, f := range callOptions {
		f.applyFunc(optCopy)
	}
	return optCopy
}

func filterCallOptions(callOptions []grpc.CallOption) (grpcOptions []grpc.CallOption, retryOptions []CallOption) {
	for _, opt := range callOptions {
		if co, ok := opt.(CallOption); ok {
			retryOptions = append(retryOptions, co)
		} else {
			grpcOptions = append(grpcOptions, opt)
		}
	}
	return grpcOptions, retryOptions
}
