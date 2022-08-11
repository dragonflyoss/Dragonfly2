package training

import (
	"context"
	"fmt"
	"sync"

	"d7y.io/dragonfly/v2/scheduler/training/models"

	"d7y.io/dragonfly/v2/pkg/pipeline"

	"github.com/sjwhitworth/golearn/base"
)

type Training struct {
	// model preserve
	model *models.LinearRegression
	to    *TrainOptions
	*pipeline.StepInfra
}

var once sync.Once

func (t *Training) GetSource(req *pipeline.Request) error {
	source := req.Data.(*base.DenseInstances)
	once.Do(func() {
		t.model = models.NewLinearRegression()
	})

	err := TrainProcess(source, t.to, t.model)
	if err != nil {
		return err
	}

	return nil
}

func (t *Training) Serve(req *pipeline.Request, out chan *pipeline.Request) error {
	t.to = NewTrainOptions()
	err := t.GetSource(req)
	if err != nil {
		return err
	}
	return nil
}

func (t *Training) trainCall(ctx context.Context, in chan *pipeline.Request, out chan *pipeline.Request) error {
	var keyVal map[string]interface{}
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("training process has been canceled")
		case val := <-in:
			if val == nil {
				keyVal[LoadType] = LoadTest
				keyVal[OutPutModel] = t.model
				out <- &pipeline.Request{
					KeyVal: keyVal,
				}
				return nil
			}
			keyVal = val.KeyVal
			err := t.Serve(val, out)
			if err != nil {
				return err
			}
		}
	}
}

func NewTrainStep() pipeline.Step {
	e := Training{}
	e.StepInfra = pipeline.New("training", e.trainCall)
	return e
}
