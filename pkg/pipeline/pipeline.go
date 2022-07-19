package pipeline

import (
	"context"
	"fmt"
	"reflect"

	"github.com/golang-collections/collections/queue"

	"d7y.io/dragonfly/v2/pkg/dag"
)

type Pipeline struct {
	Stages []*Stage
	Name   string
}

// AddStage adds a new stage to the pipeline
func (p *Pipeline) AddStage(stage ...*Stage) {
	for i := range stage {
		for j := range stage[i].Steps {
			ctx := &stepContext{
				name: p.Name + "." + stage[i].Name + "." + reflect.TypeOf(stage[i].Steps[j]).String(),
			}

			stage[i].Steps[j].SetCtx(ctx)
		}
	}

	p.Stages = append(p.Stages, stage...)
}

func (p *Pipeline) Start() *Result {
	ctx := context.Background()
	request := &Request{}
	result := &Result{}
	for _, stage := range p.Stages {
		var rp *ResultPool
		if stage.Concurrent {
			rp = &ResultPool{}
		}
		result = stage.Start(ctx, request, rp)
		if rp != nil {
			result.Data = rp.Merge(rp, stage.Agg).Data
		}

		if result.Error != nil {
			return result
		}
		request.Data = result.Data
		request.KeyVal = result.KeyVal
	}
	return result
}

//TODO key-vertx, key need to inform which builder to choose
func NewPipeLine(name string, graph dag.DAG, kr keyResolver) *Pipeline {
	pipeline := &Pipeline{Name: name}
	id := 1
	vertexQueue := queue.New()
	start := graph.StartVertex()
	graph.DeleteVertex(start)
	vertexQueue.Enqueue(start)

	for vertexQueue.Len() > 0 {
		concurrency := vertexQueue.Len() > 1
		stage := &Stage{Name: fmt.Sprintf("stage-%v", id), Concurrent: concurrency}

		for i := 0; i < vertexQueue.Len(); i++ {
			key := vertexQueue.Dequeue().(string)
			stage.AddStep(BuildMap[kr(key)].BuildStep())
			graph.DeleteVertex(key)
		}

		graph.RangeVertex(func(key string, value *dag.Vertex) bool {
			if value.InDegree() == 0 {
				vertexQueue.Enqueue(key)
			}
			return false
		})
		id++
		pipeline.AddStage(stage)
	}
	return pipeline
}
