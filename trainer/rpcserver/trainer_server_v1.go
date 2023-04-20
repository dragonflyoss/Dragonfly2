package rpcserver

import (
	"context"

	"d7y.io/dragonfly/v2/trainer/config"

	tfservingv1 "d7y.io/api/pkg/apis/tfserving/v1"
	trainerv1 "d7y.io/api/pkg/apis/trainer/v1"
)

// TODO(fyx) Add Service interface in V1 version for trainerServerV1.
// trainerServerV1 is v1 version of the trainer grpc server.
type trainerServerV1 struct {
	// Service interface
}

// newTrainerServerV1 returns v1 version of the trainer server.
func newTrainerServerV1(
	cfg *config.Config,
) trainerv1.TrainerServer {
	return &trainerServerV1{}
}

// TODO(fyx) Implement Train by utilizing function called Predict in service package.
// Train trains models of scheduler using dataset.
func (t *trainerServerV1) Train(stream trainerv1.Trainer_TrainServer) error {

	// return t.service.Train(stream)
	return nil
}

// TODO(fyx)  Implement Predict by utilizing function called Predict in service package.
// Predict provides access to loaded TensorFlow model.
func (t *trainerServerV1) Predict(ctx context.Context, req *tfservingv1.PredictRequest) (*tfservingv1.PredictResponse, error) {
	resp := &tfservingv1.PredictResponse{}

	return resp, nil
}
