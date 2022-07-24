package training

import "d7y.io/dragonfly/v2/scheduler/pipeline"

type Strategy interface {
	Serve()
}

type trainingStep struct {
	Stest Strategy
	pipeline.StepContext
}

func (ts *trainingStep) Exec(req *pipeline.Request) *pipeline.Result {
	ts.Stest.Serve()
	return nil
}

func (ts *trainingStep) Cancel() error {
	return nil
}
