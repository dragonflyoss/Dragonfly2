/*
 *     Copyright 202 The Dragonfly Authors
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

	trainerv1 "d7y.io/api/pkg/apis/trainer/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/digest"
	"d7y.io/dragonfly/v2/trainer/config"
	"d7y.io/dragonfly/v2/trainer/storage"
)

const (
	// Algorithm is the digest algorithm used to generate the digest of the model.
	Algorithm = "sha256"
)

// V1 is the interface for v1 version of the service.
type V1 struct {
	// Trainer service config.
	config *config.Config

	// Storage Interface.
	storage storage.Storage
}

// New v1 version of service instance.
func NewV1(
	cfg *config.Config,
	storage storage.Storage,

) *V1 {
	return &V1{
		config:  cfg,
		storage: storage,
	}
}

func (v *V1) Train(stream trainerv1.Trainer_TrainServer) error {
	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()

	var (
		modelKey    string
		initialized bool
	)

	for {
		select {
		case <-ctx.Done():
			logger.Infof("context was done")
			return ctx.Err()
		default:
		}

		req, err := stream.Recv()

		if !initialized {
			initialized = true

			//Initialize modelKey.
			modelKey, err = v.createModelKey(req.Hostname, req.Ip, uint(req.ClusterId))
			if err != nil {
				logger.Errorf("create model key error: %s", err.Error())
				return err
			}
		}

		if err != nil {
			if err == io.EOF {
				logger.Infof("receive streaming requests successfully")
				err = stream.SendAndClose(new(emptypb.Empty))
				// TODDO (fyx) Add training logiic.

				if err != nil {
					logger.Infof("train error %s", err.Error())
					return err
				}
			}

			// If receive stream request fails,
			// Delete the file of downloads and network topologies according to given model key.
			logger.Errorf("receive error: %s", err.Error())
			if err := v.storage.ClearDownload(modelKey); err != nil {
				logger.Errorf("clear downloads error: %s", err.Error())
				return err
			}

			if err := v.storage.ClearNetworkTopology(modelKey); err != nil {
				logger.Errorf("clear network topologies error: %s", err.Error())
				return err
			}

			return err
		}

		switch trainRequest := req.GetRequest().(type) {

		case *trainerv1.TrainRequest_TrainGnnRequest:
			logger.Infof("receive TrainRequest_TrainGnnRequest: %#v", trainRequest.TrainGnnRequest)
			if err := v.handleTrainGNNRequest(modelKey, trainRequest.TrainGnnRequest.Dataset); err != nil {
				logger.Errorf("recieve network topologies error: %s", err.Error())

				if err := v.storage.ClearNetworkTopology(modelKey); err != nil {
					logger.Errorf("clear network topologies error: %s", err.Error())
				}

				return err
			}
		case *trainerv1.TrainRequest_TrainMlpRequest:
			logger.Infof("receive TrainRequest_TrainMlpRequest: %#v", trainRequest.TrainMlpRequest)

			if err := v.handleTrainMLPRequest(modelKey, trainRequest.TrainMlpRequest.Dataset); err != nil {
				logger.Errorf("recieve downloads error: %s", err.Error())

				if err := v.storage.ClearDownload(modelKey); err != nil {
					logger.Errorf("clear downloads error: %s", err.Error())
				}

				return err
			}
		default:
			msg := fmt.Sprintf("receive unknow request: %#v", trainRequest)
			logger.Error(msg)
			return status.Error(codes.FailedPrecondition, msg)
		}
	}
}

func (v *V1) handleTrainMLPRequest(modelKey string, dataset []byte) error {

	file, err := v.storage.OpenDownload(modelKey)
	if err != nil {
		return err
	}
	defer file.Close()

	if _, err = file.Write(dataset); err != nil {
		return err
	}

	return nil
}

func (v *V1) handleTrainGNNRequest(modelKey string, dataset []byte) error {

	file, err := v.storage.OpenNetworkTopology(modelKey)
	if err != nil {
		return err
	}
	defer file.Close()

	if _, err = file.Write(dataset); err != nil {
		return err
	}

	return nil
}

func (v *V1) createModelKey(hostname, ip string, clusterId uint) (string, error) {
	return digest.HashFile(fmt.Sprintf("%s-%s-%d", hostname, ip, clusterId), Algorithm)
}
