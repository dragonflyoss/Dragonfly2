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
	trainerv1 "d7y.io/api/pkg/apis/trainer/v1"

	"d7y.io/dragonfly/v2/trainer/config"
	"d7y.io/dragonfly/v2/trainer/service"
	storage "d7y.io/dragonfly/v2/trainer/storage"
)

// trainerServerV1 is v1 version of the trainer grpc server.
type trainerServerV1 struct {
	// Service interface.
	service *service.V1
}

// newTrainerServerV1 returns a new trainerServerV1 instance.
func newTrainerServerV1(cfg *config.Config, st storage.Storage) trainerv1.TrainerServer {
	return &trainerServerV1{service.NewV1(cfg, st)}
}

// TODO (fyx): implement Train method.
// Train handles the training request from scheduler.
func (t *trainerServerV1) Train(stream trainerv1.Trainer_TrainServer) error {
	return nil
}
