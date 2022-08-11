package training

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"sync"

	"d7y.io/dragonfly/v2/manager/types"

	"github.com/sjwhitworth/golearn/base"

	"d7y.io/dragonfly/v2/scheduler/training/models"

	"d7y.io/dragonfly/v2/pkg/pipeline"
)

type Evaluating struct {
	eval  *Eval
	model *models.LinearRegression
	*pipeline.StepInfra
}

var onceEval sync.Once

func (eva *Evaluating) GetSource(req *pipeline.Request) error {
	model := req.KeyVal[OutPutModel].(*models.LinearRegression)
	if model == nil {
		return fmt.Errorf("lose model")
	}
	onceEval.Do(func() {
		eva.eval = NewEval()
	})
	eva.model = model
	source := req.Data.(*base.DenseInstances)
	predict, err := model.Predict(source)
	if err != nil {
		return err
	}
	eva.eval.EvaluateStore(predict, source)
	return nil
}

func (eva *Evaluating) Serve(req *pipeline.Request, out chan *pipeline.Request) error {
	err := eva.GetSource(req)
	if err != nil {
		return err
	}
	return nil
}

func (eva *Evaluating) encodeModelData() ([]byte, error) {
	buf := new(bytes.Buffer)
	enc := gob.NewEncoder(buf)
	err := enc.Encode(*eva.model)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (eva *Evaluating) evaCall(ctx context.Context, in chan *pipeline.Request, out chan *pipeline.Request) error {
	var keyVal map[string]interface{}
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("evaluating process has been canceled")
		case val := <-in:
			if val == nil {
				err := eva.eval.EvaluateCal()
				if err != nil {
					return err
				}

				data, err := eva.encodeModelData()
				if err != nil {
					return err
				}
				out <- &pipeline.Request{
					Data: &types.CreateModelVersionRequest{
						Data: data,
						MAE:  eva.eval.MAE,
						MSE:  eva.eval.MSE,
						RMSE: eva.eval.RMSE,
						R2:   eva.eval.R2,
					},
					KeyVal: keyVal,
				}
			}
			keyVal = val.KeyVal
			err := eva.Serve(val, out)
			if err != nil {
				return err
			}
		}
	}
}

func NewEvaStep() pipeline.Step {
	eva := Evaluating{}
	eva.StepInfra = pipeline.New("evaluating", eva.evaCall)
	return eva
}
