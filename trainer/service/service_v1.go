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

package service

import (
	"context"
	"fmt"
	"io"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	trainerv1 "d7y.io/api/v2/pkg/apis/trainer/v1"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/idgen"
	"d7y.io/dragonfly/v2/trainer/config"
	"d7y.io/dragonfly/v2/trainer/storage"
	"d7y.io/dragonfly/v2/trainer/training"
)

// V1 is the interface for v1 version of the service.
type V1 struct {
	// Trainer service config.
	config *config.Config

	// Storage Interface.
	storage storage.Storage

	// Training Interface.
	training training.Training
}

// New v1 version of service instance.
func NewV1(
	cfg *config.Config,
	storage storage.Storage,
	training training.Training,
) *V1 {
	return &V1{cfg, storage, training}
}

// Train implements the Trainer.Train method.
func (v *V1) Train(stream trainerv1.Trainer_TrainServer) error {
	var (
		ip                  string
		hostname            string
		hostID              string
		networkTopologyFile io.WriteCloser
		downloadFile        io.WriteCloser
		req                 *trainerv1.TrainRequest
		initialized         bool
		err                 error
	)

	for {
		req, err = stream.Recv()
		if err != nil {
			if err == io.EOF {
				break
			}

			logger.Errorf("receive failed: %s", err.Error())
			return err
		}

		logger := logger.WithHostnameAndIP(req.Hostname, req.Ip)
		if !initialized {
			initialized = true
			ip = req.Ip
			hostname = req.Hostname
			hostID = idgen.HostIDV2(req.Ip, req.Hostname)

			// Open network topology file and store received data.
			networkTopologyFile, err = v.storage.OpenNetworkTopology(hostID)
			if err != nil {
				msg := fmt.Sprintf("open network topology failed: %s", err.Error())
				logger.Error(msg)
				return status.Error(codes.Internal, msg)
			}
			defer func() {
				networkTopologyFile.Close()

				// If error occurred, clear network topology.
				if err != nil && err != io.EOF {
					if err := v.storage.ClearNetworkTopology(hostID); err != nil {
						logger.Errorf("clear network topology failed: %s", err.Error())
					}
				}
			}()

			// Open download file and store received data.
			downloadFile, err = v.storage.OpenDownload(hostID)
			if err != nil {
				msg := fmt.Sprintf("open download failed: %s", err.Error())
				logger.Error(msg)
				return status.Error(codes.Internal, msg)
			}
			defer func() {
				downloadFile.Close()

				// If error occurred, clear download.
				if err != nil && err != io.EOF {
					if err := v.storage.ClearDownload(hostID); err != nil {
						logger.Errorf("clear download failed: %s", err.Error())
					}
				}
			}()
		}

		switch trainRequest := req.GetRequest().(type) {
		case *trainerv1.TrainRequest_TrainGnnRequest:
			// Store network topology.
			if _, err := networkTopologyFile.Write(trainRequest.TrainGnnRequest.Dataset); err != nil {
				msg := fmt.Sprintf("write network topology failed: %s", err.Error())
				logger.Error(msg)
				return status.Error(codes.Internal, msg)
			}
		case *trainerv1.TrainRequest_TrainMlpRequest:
			// Store download.
			if _, err := downloadFile.Write(trainRequest.TrainMlpRequest.Dataset); err != nil {
				msg := fmt.Sprintf("write download failed: %s", err.Error())
				logger.Error(msg)
				return status.Error(codes.Internal, msg)
			}
		default:
			msg := fmt.Sprintf("receive unknown request: %#v", trainRequest)
			logger.Error(msg)
			return status.Error(codes.FailedPrecondition, msg)
		}
	}

	// Send empty response and close stream.
	if err := stream.SendAndClose(&emptypb.Empty{}); err != nil {
		logger.Errorf("send and close failed: %s", err.Error())
		return err
	}

	// If all dataset received, start training.
	go func() {
		if err := v.training.Train(context.Background(), ip, hostname); err != nil {
			logger.Errorf("train failed: %s", err.Error())
		}
	}()

	return nil
}
