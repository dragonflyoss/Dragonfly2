package pipeline

import (
	"context"
	"fmt"
)

type Stage struct {
	Steps      []Step
	Concurrent bool
	Name       string
	RetryTimes int
	Agg        Aggregate
}

func NewStage(name string, concurrent bool) *Stage {
	st := &Stage{Name: name, Concurrent: concurrent, RetryTimes: 1}
	return st
}

func (st *Stage) AddStep(step ...Step) {
	st.Steps = append(st.Steps, step...)
}

func (st *Stage) run(step Step, request *Request) (res *Result) {
	res = step.Exec(request)

	return
}

func (st *Stage) retry(step Step, resultChan chan *Result, request *Request) {
	var err error
	for i := 0; i < st.RetryTimes; i++ {
		res := st.run(step, request)
		err = res.Error
		if err == nil {
			resultChan <- res
			return
		}
	}
	resultChan <- &Result{Error: err}
	return
}

func (st *Stage) Start(ctx context.Context, request *Request, rp *ResultPool) *Result {
	if len(st.Steps) == 0 {
		return &Result{Error: fmt.Errorf("No steps to be executed")}
	}

	if st.Concurrent {
		resultGroup, stageCtx := NewGroup(ctx)
		resultGroup.copy = request.KeyVal
		for _, step := range st.Steps {
			resultGroup.Start(func() *Result {
				resultChan := make(chan *Result, 1)
				go st.retry(step, resultChan, request)

				select {
				case <-stageCtx.Done():
					err := step.Cancel()
					if err != nil {
						return &Result{Error: err}
					}
					return &Result{Error: fmt.Errorf("stage has been canceled")}

				case result := <-resultChan:
					if result == nil {
						result = &Result{}
					}
					rp.Add(result)
					return result
				}
			})
		}

		resultGroup.Wait()

		if resultGroup.result != nil {
			return resultGroup.result
		}
	} else {
		res := &Result{}
		for _, step := range st.Steps {
			res = step.Exec(request)
			if res != nil && res.Error != nil {
				return res
			}

			if res == nil {
				res = &Result{}
			}

			request.KeyVal = res.KeyVal
			request.Data = res.Data
		}
		return res
	}
	return &Result{}
}
