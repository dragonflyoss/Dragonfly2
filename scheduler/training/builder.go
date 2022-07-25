package training

import "d7y.io/dragonfly/v2/scheduler/pipeline"

type Strategy interface {
	Serve(req *pipeline.Request) *pipeline.Result
	Stop() error
}

type trainingStep struct {
	Name             string
	TrainingStrategy Strategy
	pipeline.StepContext
}

func (ts *trainingStep) Exec(req *pipeline.Request) *pipeline.Result {
	return ts.TrainingStrategy.Serve(req)
}

func (ts *trainingStep) Cancel() error {
	return ts.TrainingStrategy.Stop()
}
