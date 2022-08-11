package training

import (
	"fmt"

	"d7y.io/dragonfly/v2/pkg/rpc/manager/client"

	"d7y.io/dragonfly/v2/scheduler/config"

	"d7y.io/dragonfly/v2/scheduler/storage"
)

type MachineLearning interface {
	Serve()
	Stop()
}

func NewML(storage storage.Storage, cfg config.DynconfigInterface, managerClient client.Client, tc *config.TrainingConfig) (MachineLearning, error) {
	switch tc.MLType {
	case config.LinearMachineLearning:
		return NewLinearTraining(storage, cfg, managerClient, tc), nil
	}
	return nil, fmt.Errorf("unrecognized ml_type, type is %s", tc.MLType)
}
