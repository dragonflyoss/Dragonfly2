package pipeline

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"d7y.io/dragonfly/v2/pkg/dag"

	"github.com/stretchr/testify/assert"
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
	graph.AddVertex("PlusOne", mockPlusEquation)
	graph.AddVertex("PlusTwo", mockPlusEquation)
	graph.AddEdge("PlusOne", "PlusTwo")
	return graph
}

func mockParallelGraph() dag.DAG[StepConstruct] {
	graph := dag.NewDAG[StepConstruct]()
	graph.AddVertex("PlusOne", mockPlusEquation)
	graph.AddVertex("PlusTwo", mockPlusEquation)
	graph.AddVertex("PlusThree", mockPlusEquation)
	graph.AddVertex("PlusFour", mockPlusEquation)
	graph.AddVertex("PlusFive", mockPlusEquation)
	graph.AddVertex("PlusSix", mockPlusEquation)
	graph.AddVertex("PlusSeven", mockPlusEquation)
	graph.AddEdge("PlusOne", "PlusTwo")
	graph.AddEdge("PlusOne", "PlusThree")
	graph.AddEdge("PlusOne", "PlusFour")
	graph.AddEdge("PlusTwo", "PlusFive")
	graph.AddEdge("PlusThree", "PlusFive")
	graph.AddEdge("PlusFour", "PlusSix")
	graph.AddEdge("PlusFive", "PlusSeven")
	graph.AddEdge("PlusSix", "PlusSeven")
	return graph
}

func mockMoreThanOneSouceGraph() dag.DAG[StepConstruct] {
	graph := dag.NewDAG[StepConstruct]()
	graph.AddVertex("PlusOne", mockPlusEquation)
	graph.AddVertex("PlusTwo", mockPlusEquation)
	graph.AddVertex("PlusThree", mockPlusEquation)
	graph.AddEdge("PlusOne", "PlusThree")
	graph.AddEdge("PlusTwo", "PlusThree")
	return graph
}

func mockMoreThanOneOutGraph() dag.DAG[StepConstruct] {
	graph := dag.NewDAG[StepConstruct]()
	graph.AddVertex("PlusOne", mockPlusEquation)
	graph.AddVertex("PlusTwo", mockPlusEquation)
	graph.AddVertex("PlusThree", mockPlusEquation)
	graph.AddEdge("PlusOne", "PlusTwo")
	graph.AddEdge("PlusOne", "PlusThree")
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
	graph.AddVertex("TestErrorOne", mockErrorEquation)
	graph.AddVertex("TestErrorTwo", mockErrorEquation)
	graph.AddEdge("TestErrorOne", "TestErrorTwo")
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
			g:    mockMoreThanOneSouceGraph(),
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
