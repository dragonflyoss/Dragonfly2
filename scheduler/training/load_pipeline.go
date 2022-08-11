package training

import (
	"context"
	"fmt"
	"math"
	"time"

	"d7y.io/dragonfly/v2/scheduler/storage"

	"d7y.io/dragonfly/v2/pkg/pipeline"
)

type Loading struct {
	dataInstance *Data
	*pipeline.StepInfra
}

type GetSource func(req *pipeline.Request) (*pipeline.Request, error)

// GetSource actually function.
func (ld *Loading) GetDataSource(req *pipeline.Request) (*pipeline.Request, error) {
	if ld.dataInstance.TotalDataRecordLine == 0 {
		return nil, nil
	}
	result, err := ld.dataInstance.PreProcess(LoadData)
	if err != nil {
		return nil, err
	}

	req.KeyVal[DataInstance] = ld.dataInstance
	return &pipeline.Request{
		Data:   result,
		KeyVal: req.KeyVal,
	}, nil
}

func (ld *Loading) NewData(req *pipeline.Request) error {
	store := req.Data.(storage.Storage)
	reader, err := store.Open()
	dataInstance, err := New(reader)
	if err != nil {
		return err
	}
	ld.dataInstance = dataInstance

	total := store.Count()
	ld.dataInstance.TotalTestRecordLine = int64(math.Ceil(float64(total) * ld.dataInstance.Options.TestPercent))
	ld.dataInstance.TotalDataRecordLine = total - ld.dataInstance.TotalTestRecordLine
	if err != nil {
		return err
	}
	return nil
}

func (ld *Loading) Process(req *pipeline.Request, out chan *pipeline.Request, gs GetSource) error {
	// limit the send rate
	ticker := time.NewTicker(LimitSendRate * time.Second)

loop:
	for {
		select {
		case <-ticker.C:
			source, err := gs(req)
			if err != nil {
				return err
			}
			if source == nil {
				close(out)
				break loop
			}
			out <- source
		}
	}
	ticker.Stop()
	return nil
}

func (ld *Loading) ProcessData(req *pipeline.Request, out chan *pipeline.Request) error {
	err := ld.NewData(req)
	if err != nil {
		return err
	}

	err = ld.Process(req, out, ld.GetDataSource)
	if err != nil {
		return err
	}
	return nil
}

func (ld *Loading) GetTestSource(req *pipeline.Request) (*pipeline.Request, error) {
	if ld.dataInstance.TotalTestRecordLine == 0 {
		return nil, nil
	}

	result, err := ld.dataInstance.PreProcess(LoadTest)
	if err != nil {
		return nil, err
	}

	return &pipeline.Request{
		Data:   result,
		KeyVal: req.KeyVal,
	}, nil
}

func (ld *Loading) ProcessTest(req *pipeline.Request, out chan *pipeline.Request) error {
	defer func() {
		// TODO error handle
		ld.dataInstance.Reader.Close()
	}()
	ld.dataInstance = req.KeyVal[DataInstance].(*Data)
	err := ld.Process(req, out, ld.GetTestSource)
	if err != nil {
		return err
	}
	return nil
}

// Serve interface.
func (ld *Loading) Serve(req *pipeline.Request, out chan *pipeline.Request) error {
	switch req.KeyVal[LoadType] {
	case LoadData:
		err := ld.ProcessData(req, out)
		if err != nil {
			return err
		}
	case LoadTest:
		err := ld.ProcessTest(req, out)
		if err != nil {
			return err
		}
	}
	return nil
}

func (ld *Loading) LoadCall(ctx context.Context, in chan *pipeline.Request, out chan *pipeline.Request) error {
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("training process has been canceled")
		case val := <-in:
			if val == nil {
				return nil
			}
			err := ld.Serve(val, out)
			if err != nil {
				return err
			}
		}
	}
}

func NewLoadStep() pipeline.Step {
	ld := Loading{}
	ld.StepInfra = pipeline.New("Loading", ld.LoadCall)
	return ld
}
