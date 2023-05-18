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

//go:generate mockgen -destination mocks/announcer_mock.go -source announcer.go -package mocks

package announcer

import (
	"context"
	"io"
	"time"

	managerv2 "d7y.io/api/pkg/apis/manager/v2"
	trainerv1 "d7y.io/api/pkg/apis/trainer/v1"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	managerclient "d7y.io/dragonfly/v2/pkg/rpc/manager/client"
	trainerclient "d7y.io/dragonfly/v2/pkg/rpc/trainer/client"
	"d7y.io/dragonfly/v2/scheduler/config"
	"d7y.io/dragonfly/v2/scheduler/storage"
)

const (
	// BufferSize is the size of buffer for sending dataset to trainer.
	BufferSize = 2048
)

// Announcer is the interface used for announce service.
type Announcer interface {
	// Started announcer server.
	Serve() error

	// Stop announcer server.
	Stop() error
}

// announcer provides announce function.
type announcer struct {
	config        *config.Config
	managerClient managerclient.V2
	trainerClient trainerclient.V1
	stream        trainerv1.Trainer_TrainClient
	storage       storage.Storage
	done          chan struct{}
}

// Option is a functional option for configuring the announcer.
type Option func(s *announcer)

// WithTrainerClient sets the grpc client of trainer.
func WithTrainerClient(client trainerclient.V1) Option {
	return func(a *announcer) {
		a.trainerClient = client
	}
}

// WithStorage sets the storage.
func WithStorage(storage storage.Storage) Option {
	return func(a *announcer) {
		a.storage = storage
	}
}

// New returns a new Announcer interface.
func New(cfg *config.Config, managerClient managerclient.V2, options ...Option) (Announcer, error) {
	a := &announcer{
		config:        cfg,
		managerClient: managerClient,
		done:          make(chan struct{}),
	}

	for _, opt := range options {
		opt(a)
	}

	// Register to manager.
	if _, err := a.managerClient.UpdateScheduler(context.Background(), &managerv2.UpdateSchedulerRequest{
		SourceType:         managerv2.SourceType_SCHEDULER_SOURCE,
		Hostname:           a.config.Server.Host,
		Ip:                 a.config.Server.AdvertiseIP.String(),
		Port:               int32(a.config.Server.AdvertisePort),
		Idc:                a.config.Host.IDC,
		Location:           a.config.Host.Location,
		SchedulerClusterId: uint64(a.config.Manager.SchedulerClusterID),
	}); err != nil {
		return nil, err
	}

	return a, nil
}

// Started announcer server.
func (a *announcer) Serve() error {
	if a.trainerClient != nil {
		logger.Info("announce dataset to trainer")
		if trainerStream, err := a.trainerClient.Train(context.Background()); err != nil {
			return err
		} else {
			a.stream = trainerStream
		}

		a.announceToTrainer()
	}

	logger.Info("announce scheduler to manager")
	if err := a.announceToManager(); err != nil {
		return err
	}

	return nil
}

// Stop announcer server.
func (a *announcer) Stop() error {
	close(a.done)
	return nil
}

// announceSeedPeer announces peer information to manager.
func (a *announcer) announceToManager() error {
	// Start keepalive to manager.
	a.managerClient.KeepAlive(a.config.Manager.KeepAlive.Interval, &managerv2.KeepAliveRequest{
		SourceType: managerv2.SourceType_SCHEDULER_SOURCE,
		Hostname:   a.config.Server.Host,
		Ip:         a.config.Server.AdvertiseIP.String(),
		ClusterId:  uint64(a.config.Manager.SchedulerClusterID),
	}, a.done)

	return nil
}

// announceToTrainer announces regularly dataset to trainer for training model.
func (a *announcer) announceToTrainer() {
	if a.config.Trainer.Enable {
		go func() {
			tick := time.NewTicker(a.config.Trainer.Interval)
			for {
				select {
				case <-tick.C:
					go func() {
						if err := a.transferDownloadToTrainer(); err != nil {
							logger.Error(err)
						}
					}()

					go func() {
						if err := a.transferNetworkTopologyToTrainer(); err != nil {
							logger.Error(err)
						}
					}()
				case <-a.done:
					logger.Info("announceToTrainer stopped")
					return
				}
			}
		}()
	}
}

// transferDataToTrainer transfers the download dataset to trainer.
func (a *announcer) transferDownloadToTrainer() error {
	buffer := make([]byte, BufferSize)
	downloadReadCloser, err := a.storage.OpenDownload()
	if err != nil {
		return err
	}
	defer downloadReadCloser.Close()

	for {
		_, err := downloadReadCloser.Read(buffer)
		if err != nil {
			if err == io.EOF {
				break
			}

			return err
		}

		if err := a.stream.Send(&trainerv1.TrainRequest{
			Hostname:  a.config.Server.Host,
			Ip:        a.config.Server.AdvertiseIP.String(),
			ClusterId: uint64(a.config.Manager.SchedulerClusterID),
			Request: &trainerv1.TrainRequest_TrainMlpRequest{
				TrainMlpRequest: &trainerv1.TrainMLPRequest{
					Dataset: buffer,
				},
			},
		}); err != nil {
			if _, err := a.stream.CloseAndRecv(); err != nil {
				return err
			}
		}
	}

	return nil
}

// transferDataToTrainer transfers the networkTopology dataset to trainer.
func (a *announcer) transferNetworkTopologyToTrainer() error {
	buffer := make([]byte, BufferSize)
	networkTopologyReadCloser, err := a.storage.OpenNetworkTopology()
	if err != nil {
		return err
	}
	defer networkTopologyReadCloser.Close()

	for {
		_, err := networkTopologyReadCloser.Read(buffer)

		if err != nil {
			if err == io.EOF {
				break
			}

			return err
		}

		if err := a.stream.Send(&trainerv1.TrainRequest{
			Hostname:  a.config.Server.Host,
			Ip:        a.config.Server.AdvertiseIP.String(),
			ClusterId: uint64(a.config.Manager.SchedulerClusterID),
			Request: &trainerv1.TrainRequest_TrainGnnRequest{
				TrainGnnRequest: &trainerv1.TrainGNNRequest{
					Dataset: buffer,
				},
			},
		}); err != nil {
			if _, err := a.stream.CloseAndRecv(); err != nil {
				return err
			}
		}
	}

	return nil
}
