package training

import (
	"context"
	"fmt"

	"d7y.io/dragonfly/v2/pkg/pipeline1"

	"github.com/sjwhitworth/golearn/base"
)

type Evaluating struct {
	*pipeline1.StepInfra
}

// GetSource actually function.
func (eva *Evaluating) GetSource(req *pipeline1.Request) (*pipeline1.Request, error) {
	source := req.Data.(map[float64]*base.DenseInstances)
	instance, ok := req.KeyVal[Instance]
	if !ok {
		return nil, fmt.Errorf("no instance")
	}
	trainingInstance := instance.(*Train)

	// TODO if source is nil
	result, err := trainingInstance.TrainProcess(source)
	if err != nil {
		return nil, err
	}

	return &pipeline1.Request{
		Data:   result,
		KeyVal: req.KeyVal,
	}, nil
}

// Serve interface.
func (eva *Evaluating) Serve(req *pipeline1.Request) (*pipeline1.Request, error) {
	return eva.GetSource(req)
}

func (eva *Evaluating) EvalCall(ctx context.Context, in chan *pipeline1.Request) (*pipeline1.Request, error) {
	out := &pipeline1.Request{}
	for {
		// TODO out change to the answer struct
		var err error

		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("evaluating process has been canceled")
		case val := <-in:
			if val == nil {
				return out, nil
			}
			out, err = eva.Serve(val)
			if err != nil {
				return nil, err
			}
		}
	}
}

func NewEvalStep() pipeline1.Step {
	e := Evaluating{}
	e.StepInfra = pipeline1.New("Evaluating", e.EvalCall)
	return e
}
