/*
 *     Copyright 2023 The Dragonfly Authors
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

package rpcserver

import (
	"google.golang.org/grpc"

	"d7y.io/dragonfly/v2/pkg/rpc/trainer/server"
	"d7y.io/dragonfly/v2/trainer/config"
	"d7y.io/dragonfly/v2/trainer/storage"
	"d7y.io/dragonfly/v2/trainer/training"
)

// New creates a new grpc server.
func New(
	cfg *config.Config,
	storage storage.Storage,
	training training.Training,
	opts ...grpc.ServerOption,
) *grpc.Server {
	return server.New(
		newTrainerServerV1(cfg, storage, training),
		opts...)
}
