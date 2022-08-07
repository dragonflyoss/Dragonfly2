package training

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"

	"d7y.io/dragonfly/v2/pkg/pipeline1"
)

type Loading struct {
	*pipeline1.StepInfra
}

// GetSource actually function.
func (load *Loading) GetSource(req *pipeline1.Request) (*pipeline1.Request, error) {
	source := req.Data.([]byte)

	var result map[float64]*LinearModel
	dec := gob.NewDecoder(bytes.NewBuffer(source))
	err := dec.Decode(&result)
	if err != nil {
		return nil, err
	}

	return &pipeline1.Request{
		Data:   result,
		KeyVal: req.KeyVal,
	}, nil
}

// Serve interface.
func (load *Loading) Serve(req *pipeline1.Request) (*pipeline1.Request, error) {
	return load.GetSource(req)
}

// TODO
func (l *Loading) LoadCall(ctx context.Context, in chan *pipeline1.Request) (*pipeline1.Request, error) {
	for {
		// TODO out change to the answer struct
		out := &pipeline1.Request{}
		var err error

		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("loading process has been canceled")
		case val := <-in:
			if val == nil {
				return out, nil
			}
			out, err = l.Serve(val)
			if err != nil {
				return nil, err
			}
		}
	}
}

func NewLoadingStep() pipeline1.Step {
	l := Loading{}
	l.StepInfra = pipeline1.New("Loading", l.LoadCall)
	return l
}
