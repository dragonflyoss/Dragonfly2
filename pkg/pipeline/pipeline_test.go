package pipeline

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"

	"d7y.io/dragonfly/v2/pkg/dag"
)

type PlusOne struct {
	num int
	*StepInfra
}

func (po *PlusOne) plusOneCall(ctx context.Context, in chan *Request, out chan *Request) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case val := <-in:
			if val == nil {
				out <- &Request{
					Data: po.num,
				}
				return nil
			}
			po.num += val.Data.(int) + 1
		}
	}
}

func mockPlusEquation() Step {
	mpe := &PlusOne{}
	mpe.StepInfra = New("PlusOne", mpe.plusOneCall)
	return mpe
}

func mockNoParallelGraph() dag.DAG[StepConstruct] {
	graph := dag.NewDAG[StepConstruct]()
	err := graph.AddVertex("PlusOne", mockPlusEquation)
	if err != nil {
		return nil
	}
	err = graph.AddVertex("PlusTwo", mockPlusEquation)
	if err != nil {
		return nil
	}
	err = graph.AddEdge("PlusOne", "PlusTwo")
	if err != nil {
		return nil
	}
	return graph
}

func mockParallelGraph() dag.DAG[StepConstruct] {
	graph := dag.NewDAG[StepConstruct]()
	err := graph.AddVertex("PlusOne", mockPlusEquation)
	if err != nil {
		return nil
	}
	err = graph.AddVertex("PlusTwo", mockPlusEquation)
	if err != nil {
		return nil
	}
	err = graph.AddVertex("PlusThree", mockPlusEquation)
	if err != nil {
		return nil
	}
	err = graph.AddVertex("PlusFour", mockPlusEquation)
	if err != nil {
		return nil
	}
	err = graph.AddVertex("PlusFive", mockPlusEquation)
	if err != nil {
		return nil
	}
	err = graph.AddVertex("PlusSix", mockPlusEquation)
	if err != nil {
		return nil
	}
	err = graph.AddVertex("PlusSeven", mockPlusEquation)
	if err != nil {
		return nil
	}
	err = graph.AddEdge("PlusOne", "PlusTwo")
	if err != nil {
		return nil
	}
	err = graph.AddEdge("PlusOne", "PlusThree")
	if err != nil {
		return nil
	}
	err = graph.AddEdge("PlusOne", "PlusFour")
	if err != nil {
		return nil
	}
	err = graph.AddEdge("PlusTwo", "PlusFive")
	if err != nil {
		return nil
	}
	err = graph.AddEdge("PlusThree", "PlusFive")
	if err != nil {
		return nil
	}
	err = graph.AddEdge("PlusFour", "PlusSix")
	if err != nil {
		return nil
	}
	err = graph.AddEdge("PlusFive", "PlusSeven")
	if err != nil {
		return nil
	}
	err = graph.AddEdge("PlusSix", "PlusSeven")
	if err != nil {
		return nil
	}
	return graph
}

func mockMoreThanOneSouceGraph() dag.DAG[StepConstruct] {
	graph := dag.NewDAG[StepConstruct]()
	err := graph.AddVertex("PlusOne", mockPlusEquation)
	if err != nil {
		return nil
	}
	err = graph.AddVertex("PlusTwo", mockPlusEquation)
	if err != nil {
		return nil
	}
	err = graph.AddVertex("PlusThree", mockPlusEquation)
	if err != nil {
		return nil
	}
	err = graph.AddEdge("PlusOne", "PlusThree")
	if err != nil {
		return nil
	}
	err = graph.AddEdge("PlusTwo", "PlusThree")
	if err != nil {
		return nil
	}
	return graph
}

func mockMoreThanOneOutGraph() dag.DAG[StepConstruct] {
	graph := dag.NewDAG[StepConstruct]()
	err := graph.AddVertex("PlusOne", mockPlusEquation)
	if err != nil {
		return nil
	}
	err = graph.AddVertex("PlusTwo", mockPlusEquation)
	if err != nil {
		return nil
	}
	err = graph.AddVertex("PlusThree", mockPlusEquation)
	if err != nil {
		return nil
	}
	err = graph.AddEdge("PlusOne", "PlusTwo")
	if err != nil {
		return nil
	}
	err = graph.AddEdge("PlusOne", "PlusThree")
	if err != nil {
		return nil
	}
	return graph
}

type TestError struct {
	num int
	*StepInfra
}

func (te *TestError) serve(in *Request) error {
	if te.num == 0 {
		return errors.New("test error")
	}
	te.num += in.Data.(int)
	return nil
}

func (te *TestError) testErrorCall(ctx context.Context, in chan *Request, out chan *Request) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case val := <-in:
			if val == nil {
				out <- &Request{
					Data: te.num,
				}
				return nil
			}
			err := te.serve(val)
			if err != nil {
				return err
			}
		}
	}
}
func mockErrorEquation() Step {
	te := &TestError{}
	te.StepInfra = New("TestError", te.testErrorCall)
	return te
}

func mockTestErrorGraph() dag.DAG[StepConstruct] {
	graph := dag.NewDAG[StepConstruct]()
	err := graph.AddVertex("TestErrorOne", mockErrorEquation)
	if err != nil {
		return nil
	}
	err = graph.AddVertex("TestErrorTwo", mockErrorEquation)
	if err != nil {
		return nil
	}
	err = graph.AddEdge("TestErrorOne", "TestErrorTwo")
	if err != nil {
		return nil
	}
	return graph
}

func TestNewPipeline(t *testing.T) {
	p := NewPipeline()
	assert := assert.New(t)
	assert.Equal(reflect.TypeOf(p).Elem().Name(), "Pipeline")
}

func TestPipelineHandleDag(t *testing.T) {
	tests := []struct {
		name   string
		g      dag.DAG[StepConstruct]
		req    *Request
		expect func(t *testing.T, out *Request, err error)
	}{
		{
			name: "test dag with parallel output",
			g:    mockNoParallelGraph(),
			req:  &Request{Data: 0},
			expect: func(t *testing.T, out *Request, err error) {
				assert := assert.New(t)
				assert.Nil(err)
				if err != nil {
					t.Fatalf(err.Error())
					return
				}
				assert.Equal(2, out.Data.(int))
			},
		},
		{
			name: "test dag with parallel",
			g:    mockParallelGraph(),
			req:  &Request{Data: 0},
			expect: func(t *testing.T, out *Request, err error) {
				assert := assert.New(t)
				assert.Nil(err)
				if err != nil {
					t.Fatalf(err.Error())
					return
				}
				assert.Equal(11, out.Data.(int))
			},
		},
		{
			name: "test dag with more than one source",
			g:    mockMoreThanOneSouceGraph(),
			req:  &Request{Data: 0},
			expect: func(t *testing.T, out *Request, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "dag dont match requirements")
			},
		},
		{
			name: "test dag with more than one out",
			g:    mockMoreThanOneOutGraph(),
			req:  &Request{Data: 0},
			expect: func(t *testing.T, out *Request, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "dag dont match requirements")
			},
		},
		{
			name: "test dag with error handler",
			g:    mockTestErrorGraph(),
			req:  &Request{Data: 0},
			expect: func(t *testing.T, out *Request, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "test error")
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p := NewPipeline()
			result, err := p.Exec(tc.req, tc.g)
			tc.expect(t, result, err)
		})
	}
}
