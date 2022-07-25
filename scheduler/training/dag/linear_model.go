package dag

import (
	"d7y.io/dragonfly/v2/pkg/dag"
	"d7y.io/dragonfly/v2/scheduler/pipeline"
)

type LinearPipeline struct {
}

func (lp *LinearPipeline) LoadConstruct() (*dag.DAG, error) {
	graph := dag.NewDAG()
	err := graph.AddVertex("loading", pipeline.BuildMap["loading"])
	if err != nil {
		return nil, err
	}
	return &graph, nil
}

func (lp *LinearPipeline) TrainConstruct() (*dag.DAG, error) {
	graph := dag.NewDAG()
	err := graph.AddVertex("training", pipeline.BuildMap["training"])
	if err != nil {
		return nil, err
	}
	return &graph, nil
}

func (lp *LinearPipeline) EvaluateConstruct() (*dag.DAG, error) {
	graph := dag.NewDAG()
	err := graph.AddVertex("evaluating", pipeline.BuildMap["evaluating"])
	if err != nil {
		return nil, err
	}
	return &graph, nil
}

func (lp *LinearPipeline) SaveConstruct() (*dag.DAG, error) {
	graph := dag.NewDAG()
	err := graph.AddVertex("saving", pipeline.BuildMap["saving"])
	if err != nil {
		return nil, err
	}
	return &graph, nil
}

func (lp *LinearPipeline) PipelineOrder() []func() (*dag.DAG, error) {
	order := make([]func() (*dag.DAG, error), 0)
	order = append(order, lp.LoadConstruct)
	order = append(order, lp.TrainConstruct)
	order = append(order, lp.EvaluateConstruct)
	order = append(order, lp.SaveConstruct)
	return order
}
