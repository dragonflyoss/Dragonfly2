package dag

import (
	"d7y.io/dragonfly/v2/pkg/dag"
	"d7y.io/dragonfly/v2/scheduler/pipeline"
)

type Option struct {
	functions []func() (*dag.DAG, error)
}

func (o *Option) Do() error {
	task := &pipeline.Task{}

	for _, fn := range o.functions {
		dag, err := fn()
		if err != nil {
			return err
		}
		task.DagList = append(task.DagList, dag)
	}

	return task.Start()
}

func ConstructModelPipeline(model interface{}) *Option {
	var op *Option
	switch model.(type) {
	case LinearPipeline:
		lp := model.(LinearPipeline)
		op.functions = lp.PipelineOrder()
		// more case
	}
	return op
}
