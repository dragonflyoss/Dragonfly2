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

//go:generate mockgen -destination mocks/training_mock.go -source training.go -package mocks

package training

import (
	"d7y.io/dragonfly/v2/trainer/config"
	"d7y.io/dragonfly/v2/trainer/storage"
)

type Training interface {
	// GNNTrain provides the training pipeline to GNN model.
	GNNTrain() error

	// MLPTrain provides the training pipeline to MLP model.
	MLPTrain() error
}

type training struct {
	// Storage interface.
	storage storage.Storage

	// Trainer configuration.
	config *config.Config
}

func New(cfg *config.Config, storage storage.Storage) Training {
	return &training{
		storage: storage,
		config:  cfg,
	}
}

// GNNTrain provides the training pipeline to GNN model.
func (t *training) GNNTrain() error {
	return nil
}

// MLPTrain provides the training pipeline to MLP model.
func (t *training) MLPTrain() error {
	return nil
}
