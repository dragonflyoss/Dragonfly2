package training

import (
	"context"
	"fmt"

	"d7y.io/dragonfly/v2/manager/types"

	managerv1 "d7y.io/api/pkg/apis/manager/v1"
	"d7y.io/dragonfly/v2/pkg/rpc/manager/client"
	"d7y.io/dragonfly/v2/scheduler/config"

	"d7y.io/dragonfly/v2/pkg/pipeline"
)

type Saving struct {
	*pipeline.StepInfra
}

// GetSource actually function.
func (save *Saving) GetSource(req *pipeline.Request) (*string, error) {
	request := req.Data.(*types.CreateModelVersionRequest)
	if request == nil {
		return nil, fmt.Errorf("request get nil data")
	}

	dynconfig := req.KeyVal[DynConfigData].(*config.DynconfigData)
	if dynconfig == nil {
		return nil, fmt.Errorf("lose keyVal dynconfig")
	}

	mc := req.KeyVal[ManagerClient].(client.Client)
	if mc == nil {
		return nil, fmt.Errorf("lose keyVal ManagerClient")
	}
	// TODO: check only need one model in one scheduler
	models, err := mc.ListModels(context.Background(), &managerv1.ListModelsRequest{
		SchedulerId: dynconfig.SchedulerCluster.ID,
	})
	if err != nil {
		return nil, err
	}

	if len(models.Models) == 0 {
		_, err = mc.CreateModel(context.Background(), &managerv1.CreateModelRequest{
			ModelId: types.ModelIDEvaluator,
			// TODO
			Name:        "TODO",
			SchedulerId: dynconfig.SchedulerCluster.ID,
			HostName:    dynconfig.Hostname,
			Ip:          dynconfig.IP,
		})
		if err != nil {
			return nil, err
		}
	}

	version, err := mc.CreateModelVersion(context.Background(), &managerv1.CreateModelVersionRequest{
		SchedulerId: dynconfig.SchedulerCluster.ID,
		ModelId:     types.ModelIDEvaluator,
		Data:        request.Data,
		Mae:         request.MAE,
		Mse:         request.MSE,
		Rmse:        request.RMSE,
		R2:          request.R2,
	})
	if err != nil {
		return nil, err
	}

	_, err = mc.UpdateModel(context.Background(), &managerv1.UpdateModelRequest{
		VersionId: version.VersionId,
	})
	if err != nil {
		return nil, err
	}
	return &version.VersionId, nil
}

// Serve interface.
func (save *Saving) Serve(req *pipeline.Request) (*string, error) {
	return save.GetSource(req)
}

func (save *Saving) SaveCall(ctx context.Context, in chan *pipeline.Request, out chan *pipeline.Request) error {
	var (
		mv  *string
		err error
	)
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("saving process has been canceled")
		case val := <-in:
			if val == nil {
				out <- &pipeline.Request{
					Data:   mv,
					KeyVal: nil,
				}
				return nil
			}
			mv, err = save.Serve(val)
			if err != nil {
				return err
			}
		}
	}
}

func NewSavingStep() pipeline.Step {
	s := Saving{}
	s.StepInfra = pipeline.New("Saving", s.SaveCall)
	return s
}
