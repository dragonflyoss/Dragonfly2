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
	"context"

	"golang.org/x/sync/errgroup"

	managerv2 "d7y.io/api/pkg/apis/manager/v2"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	managerclient "d7y.io/dragonfly/v2/pkg/rpc/manager/client"
	"d7y.io/dragonfly/v2/trainer/config"
	"d7y.io/dragonfly/v2/trainer/storage"
)

type Training interface {
	// Train begins training GNN and MLP model.
	Train(hostname, ip string, clusterID uint64) error
}

type training struct {
	// Storage interface.
	storage storage.Storage

	// Manager service clent.
	managerClient managerclient.V2

	// Trainer configuration.
	config *config.Config
}

func New(cfg *config.Config, managerClient managerclient.V2, storage storage.Storage) Training {
	return &training{
		storage:       storage,
		managerClient: managerClient,
		config:        cfg,
	}
}

// TODO: implement train logic.
// Train begins training GNN and MLP model.
func (t *training) Train(hostname, ip string, clusterID uint64) error {
	logger.Infof("preprocess data set")
	if err := t.preProcess(); err != nil {
		logger.Errorf("preprocess data set error: %s", err.Error())
		return err
	}

	eg := errgroup.Group{}
	eg.Go(func() error {
		logger.Infof("begin training GNN model")
		if err := t.trainGNN(); err != nil {
			logger.Errorf("train GNN model error: %s", err.Error())
			return err
		}

		if err := t.managerClient.CreateModel(context.Background(), &managerv2.CreateModelRequest{
			Request: &managerv2.CreateModelRequest_CreateGnnRequest{},
		}); err != nil {
			logger.Errorf("create GNN model error: %s", err.Error())
			return err
		}
		return nil
	})

	eg.Go(func() error {
		logger.Infof("begin training MLP model")
		if err := t.trainMLP(); err != nil {
			logger.Errorf("train MLP model error: %s", err.Error())
			return err
		}

		if err := t.managerClient.CreateModel(context.Background(), &managerv2.CreateModelRequest{
			Request: &managerv2.CreateModelRequest_CreateMlpRequest{},
		}); err != nil {
			logger.Errorf("create MLP model error: %s", err.Error())
			return err
		}

		return nil
	})

	if err := eg.Wait(); err != nil {
		logger.Errorf("wait error %s", err.Error())
		return err
	}

	return nil
}

// TODO: implement preprocess logic.
// preProcess preprocesses the training set data.
func (t *training) preProcess() error {
	return nil
}

// TODO: implement GNN training logic.
// trainGNN provides the training pipeline to GNN model.
func (t *training) trainGNN() error {
	return nil
}

// TODO: implement MLP training logic.
// trainMLP provides the training pipeline to MLP model.
func (t *training) trainMLP() error {
	return nil
}
