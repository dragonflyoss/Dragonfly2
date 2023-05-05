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

//go:generate mockgen -destination mocks/trainer_mock.go -source trainer.go -package mocks

package trainer

import (
	"bytes"
	"context"
	"encoding/gob"
	"time"

	trainerv1 "d7y.io/api/pkg/apis/trainer/v1"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	trainerclient "d7y.io/dragonfly/v2/pkg/rpc/trainer/client"
	"d7y.io/dragonfly/v2/scheduler/config"
	"d7y.io/dragonfly/v2/scheduler/storage"
)

// Trainer is the interface used for trainer service.
type Trainer interface {
	// Serve trainer service.
	Serve() error

	// Stop trainer service.
	Stop() error
}

// trainer provides trainer function.
type trainer struct {
	config        *config.TrainerConfig
	dynconfig     config.DynconfigInterface
	trainerClient trainerclient.V1
	trainerStream trainerv1.Trainer_TrainClient
	storage       storage.Storage
	done          chan struct{}
}

func New(cfg *config.TrainerConfig, dynconfig config.DynconfigInterface, trainerClient trainerclient.V1, storage storage.Storage) (Trainer, error) {
	stream, err := trainerClient.Train(context.Background())
	if err != nil {
		return nil, err
	}
	return &trainer{
		config:        cfg,
		dynconfig:     dynconfig,
		trainerClient: trainerClient,
		trainerStream: stream,
		storage:       storage,
		done:          make(chan struct{}),
	}, nil
}

// Started trainer server.
func (t *trainer) Serve() error {
	tick := time.NewTicker(t.config.Interval)
	for {
		select {
		case <-tick.C:
			if err := t.sendDataToTrainer(); err != nil {
				logger.Error("scheduler send data to trainer error: %s", err.Error())
			}
		case <-t.done:
			return nil
		}
	}
}

// Stop trainer server.
func (t *trainer) Stop() error {
	if err := t.trainerClient.Close(); err != nil {
		return err
	}

	close(t.done)
	return nil
}

// sendDataToTrainer send dataset to trainer for training model.
func (t *trainer) sendDataToTrainer() error {
	scheduler, err := t.dynconfig.GetScheduler()
	if err != nil {
		return err
	}

	var b bytes.Buffer
	download, err := t.storage.ListDownload()
	if err != nil {
		return err
	}
	for _, data := range download {
		if err := gob.NewEncoder(&b).Encode(data); err != nil {
			return err
		}

		if err := t.trainerStream.Send(&trainerv1.TrainRequest{
			Hostname:  scheduler.Hostname,
			Ip:        scheduler.Ip,
			ClusterId: scheduler.SchedulerCluster.Id,
			Request: &trainerv1.TrainRequest_TrainMlpRequest{
				TrainMlpRequest: &trainerv1.TrainMLPRequest{
					Dataset: b.Bytes(),
				},
			},
		}); err != nil {
			return err
		}
	}

	networkTopology, err := t.storage.ListNetworkTopology()
	if err != nil {
		return err
	}
	for _, data := range networkTopology {
		if err := gob.NewEncoder(&b).Encode(data); err != nil {
			return err
		}

		if err = t.trainerStream.Send(&trainerv1.TrainRequest{
			Hostname:  scheduler.Hostname,
			Ip:        scheduler.Ip,
			ClusterId: scheduler.SchedulerCluster.Id,
			Request: &trainerv1.TrainRequest_TrainGnnRequest{
				TrainGnnRequest: &trainerv1.TrainGNNRequest{
					Dataset: b.Bytes(),
				},
			},
		}); err != nil {
			return err
		}
	}

	if _, err := t.trainerStream.CloseAndRecv(); err != nil {
		return err
	}
	return nil
}
