package pipeline

import (
	"fmt"

	"d7y.io/dragonfly/v2/pkg/dag"
)

type Task struct {
	DagList []*dag.DAG
}

func (t *Task) Start() error {
	request := &Request{}
	result := &Result{}
	for i := 0; i < len(t.DagList); i++ {
		p, err := NewPipeLine(fmt.Sprintf("pipeline-%v", i), t.DagList[i])
		if err != nil {
			return err
		}
		result = p.Start(request)
		if result.Error != nil {
			return result.Error
		}

		request.KeyVal = result.KeyVal
		request.Data = result.Data
	}
	return nil
}
