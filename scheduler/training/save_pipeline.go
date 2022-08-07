package training

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"

	"d7y.io/dragonfly/v2/pkg/pipeline1"
)

type Saving struct {
	*pipeline1.StepInfra
}

// GetSource actually function.
func (save *Saving) GetSource(req *pipeline1.Request) (*pipeline1.Request, error) {
	source := req.Data.(map[float64]*LinearModel)

	buf := new(bytes.Buffer)
	enc := gob.NewEncoder(buf)
	err := enc.Encode(source)
	if err != nil {
		return nil, err
	}

	return &pipeline1.Request{
		Data:   buf.Bytes(),
		KeyVal: req.KeyVal,
	}, nil
}

// Serve interface.
func (save *Saving) Serve(req *pipeline1.Request) (*pipeline1.Request, error) {
	return save.GetSource(req)
}

func (save *Saving) SaveCall(ctx context.Context, in chan *pipeline1.Request) (*pipeline1.Request, error) {
	out := &pipeline1.Request{}
	for {
		// TODO out change to the answer struct
		var err error

		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("saving process has been canceled")
		case val := <-in:
			if val == nil {
				return out, nil
			}
			out, err = save.Serve(val)
			if err != nil {
				return nil, err
			}
		}
	}
}

func NewSavingStep() pipeline1.Step {
	s := Saving{}
	s.StepInfra = pipeline1.New("Saving", s.SaveCall)
	return s
}
