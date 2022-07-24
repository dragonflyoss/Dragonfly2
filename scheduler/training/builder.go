package training

import "d7y.io/dragonfly/v2/scheduler/pipeline"

type Strategy interface {
	Serve(req *pipeline.Request)
}

type trainingStep struct {
	Stest Strategy
	pipeline.StepContext
}

func (ts *trainingStep) Exec(req *pipeline.Request) *pipeline.Result {
	ts.Stest.Serve(req)
	return nil
}

func (ts *trainingStep) Cancel() error {
	return nil
}
