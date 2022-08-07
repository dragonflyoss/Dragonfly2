package main

import (
	"awesomeProject/dag"
	"awesomeProject/pipeline1"
	"context"
	"fmt"
)

type MathStep struct {
	*pipeline1.StepInfra
}

func call(ctx context.Context, in chan *pipeline1.Request) (*pipeline1.Request, error) {
	sum := 0
	for i := range in {
		pa := i.Data.([]int)
		for num := range pa {
			sum += num
		}
	}
	return &pipeline1.Request{Data: sum}, nil
}

func NewMathStep() pipeline1.Step {
	return MathStep{
		pipeline1.New("math", call),
	}
}

type MathRandStep struct {
	*pipeline1.StepInfra
}

func random(val interface{}) (*pipeline1.Request, error) {
	if val == nil {
		return
	}
}

func NewMathRandStep() pipeline1.Step {
	return MathRandStep{
		pipeline1.New("rand", pipeline1.NewStepWrap(random)),
	}
}

type MathMultiStep struct {
	*pipeline1.StepInfra
}

func call2(ctx context.Context, in chan *pipeline1.Request) (*pipeline1.Request, error) {
	sum := 1
	for i := range in {
		sum *= i.Data.(int)
	}
	fmt.Println("call me")
	return &pipeline1.Request{Data: sum}, nil
}

func NewMathMultiStep() pipeline1.Step {
	return MathMultiStep{
		pipeline1.New("multi", call2),
	}
}

// TODO agg options, error handler options, default agg option is return list
func main() {
	p := pipeline1.NewPipeline()
	req := &pipeline1.Request{
		Data: []int{1, 2, 3, 4, 5, 6, 7},
	}
	graph := dag.NewDAG[pipeline1.StepConstruct]()
	graph.AddVertex("math", NewMathStep)
	graph.AddVertex("rand1", NewMathRandStep)
	graph.AddVertex("rand2", NewMathRandStep)
	graph.AddEdge("math", "rand1")
	graph.AddEdge("math", "rand2")
	graph.AddVertex("multi", NewMathMultiStep)
	graph.AddEdge("rand1", "multi")
	graph.AddEdge("rand2", "multi")
	_, err := p.Exec(req, graph)
	if err != nil {
		return
	}
	//fmt.Println(exec.Data)
}
