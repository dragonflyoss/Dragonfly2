package training

import (
	"fmt"

	"d7y.io/dragonfly/v2/pkg/dag"
	"d7y.io/dragonfly/v2/pkg/pipeline1"
)

func LinearDag() dag.DAG[pipeline1.StepConstruct] {
	graph := dag.NewDAG[pipeline1.StepConstruct]()
	//graph.AddVertex("Loading", NewLoadingStep)
	graph.AddVertex("Training", NewTrainStep)
	graph.AddVertex("Evaluating", NewEvalStep)
	graph.AddVertex("Saving", NewSavingStep)
	//graph.AddEdge("Loading", "Training")
	graph.AddEdge("Training", "Evaluating")
	graph.AddEdge("Evaluating", "Saving")
	return graph
}

// TODO move to scheduler, baseDir and fileName should read from config
func Process(baseDir, fileName string) {
	graph := LinearDag()
	p := pipeline1.NewPipeline()
	req := &pipeline1.Request{
		KeyVal: make(map[string]interface{}),
	}
	req.KeyVal[BaseDir] = baseDir
	req.KeyVal[FileName] = fileName

	req1, err := p.Exec(req, graph)
	if err != nil {
		return
	}
	fmt.Println(req1.Data)
}
