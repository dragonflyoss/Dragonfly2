package training

import (
	"context"
	"fmt"

	logger "d7y.io/dragonfly/v2/internal/dflog"

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

	//var modelTest models2.LinearRegression
	//json.Unmarshal(request.Data, &modelTest)
	//record := storage.Record{
	//	IP:             rand.Intn(100)%2 + 1,
	//	HostName:       rand.Intn(100)%2 + 1,
	//	Tag:            rand.Intn(100)%2 + 1,
	//	Rate:           float64(rand.Intn(300) + 10),
	//	ParentPiece:    float64(rand.Intn(240) + 14),
	//	SecurityDomain: rand.Intn(100)%2 + 1,
	//	IDC:            rand.Intn(100)%2 + 1,
	//	NetTopology:    rand.Intn(100)%2 + 1,
	//	Location:       rand.Intn(100)%2 + 1,
	//	UploadRate:     float64(rand.Intn(550) + 3),
	//	CreateAt:       time.Now().Unix()/7200 + rand.Int63n(10),
	//	UpdateAt:       time.Now().Unix()/7200 + rand.Int63n(10),
	//	ParentCreateAt: time.Now().Unix()/7200 + rand.Int63n(10),
	//	ParentUpdateAt: time.Now().Unix()/7200 + rand.Int63n(10),
	//}
	//str, err := gocsv.MarshalString([]storage.Record{record})
	//if err != nil {
	//	logger.Infof("marshal model fail, error is %v", err)
	//}
	//// TODO
	//str = str[156:]
	//strReader := bytes.NewReader([]byte(str))
	//data1, err := base.ParseCSVToInstancesFromReader(strReader, false)
	//if err != nil {
	//	logger.Infof("ParseCSVToInstancesFromReader model fail, error is %v", err)
	//}
	//MissingValue(data1)
	//arr := make([]float64, 15)
	//for i := 0; i < 15; i++ {
	//	attrSpec2, _ := data1.GetAttribute(data1.AllAttributes()[i])
	//	arr[i] = base.UnpackBytesToFloat(data1.Get(attrSpec2, 0))
	//}
	////Normalize(data1, false)
	//
	//for i := 0; i < 15; i++ {
	//	attrSpec2, _ := data1.GetAttribute(data1.AllAttributes()[i])
	//	arr[i] = base.UnpackBytesToFloat(data1.Get(attrSpec2, 0))
	//}
	//
	//out, err := modelTest.Predict(data1)
	//if err != nil {
	//	logger.Infof("predict model fail, error is %v", err)
	//}
	//attrSpec1, err := out.GetAttribute(out.AllAttributes()[0])
	//if err != nil {
	//	logger.Infof("GetAttribute model fail, error is %v", err)
	//}
	//score := base.UnpackBytesToFloat(out.Get(attrSpec1, 0))
	//logger.Infof("score is %v", score)
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
			VersionId:   "TODO",
			SchedulerId: dynconfig.SchedulerCluster.ID,
			HostName:    dynconfig.Hostname,
			Ip:          dynconfig.IP,
		})
		if err != nil {
			logger.Info("create model fail")
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
		R2:          -request.R2,
	})
	if err != nil {
		logger.Infof("create modelversion fail, error is %v", err)
		return nil, err
	}

	_, err = mc.UpdateModel(context.Background(), &managerv1.UpdateModelRequest{
		ModelId:     types.ModelIDEvaluator,
		Name:        "TODO",
		SchedulerId: dynconfig.SchedulerCluster.ID,
		VersionId:   version.VersionId,
		HostName:    dynconfig.Hostname,
		Ip:          dynconfig.IP,
	})
	if err != nil {
		logger.Info("update model fail")
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
			return nil
		case val := <-in:
			logger.Info("start to save")
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
