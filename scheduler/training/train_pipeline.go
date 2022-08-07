package training

import (
	"context"
	"fmt"

	"d7y.io/dragonfly/v2/pkg/pipeline1"
	"d7y.io/dragonfly/v2/scheduler/training/models"
)

type LinearModel struct {
	// Model actual model.
	Model *models.LinearRegression
	Ev    *Eval
}

type Training struct {
	*pipeline1.StepInfra
}

// GetSource actually function.
func (train *Training) GetSource(req *pipeline1.Request) (*pipeline1.Request, error) {
	dir, ok := req.KeyVal[BaseDir]
	if !ok {
		return nil, fmt.Errorf("no baseDir")
	}
	fileName, ok := req.KeyVal[FileName]
	if !ok {
		return nil, fmt.Errorf("no fileName")
	}

	trainInstance, err := New(dir.(string), fileName.(string), WithMaxBufferLine(DefaultMaxBufferLine), WithMaxRecordLine(DefaultMaxRecordLine), WithLearningRate(DefaultLearningRate))
	if err != nil {
		return nil, err
	}

	result, err := trainInstance.PreProcess()
	if err != nil {
		return nil, err
	}
	req.KeyVal[Instance] = trainInstance
	return &pipeline1.Request{
		Data:   result,
		KeyVal: req.KeyVal,
	}, nil
}

// Serve interface.
func (train *Training) Serve(req *pipeline1.Request) (*pipeline1.Request, error) {
	return train.GetSource(req)
}

func (train *Training) TrainCall(ctx context.Context, in chan *pipeline1.Request) (*pipeline1.Request, error) {
	out := &pipeline1.Request{}
	for {
		// TODO out change to the answer struct
		var err error

		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("training process has been canceled")
		case val := <-in:
			if val == nil {
				return out, nil
			}
			out, err = train.Serve(val)
			if err != nil {
				return nil, err
			}
		}
	}
}

func NewTrainStep() pipeline1.Step {
	t := Training{}
	t.StepInfra = pipeline1.New("Training", t.TrainCall)
	return t
}
