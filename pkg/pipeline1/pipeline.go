package pipeline1

import (
	"context"
	"fmt"

	"d7y.io/dragonfly/v2/pkg/dag"

	"golang.org/x/exp/constraints"
)

type Pipeline struct {
	channelStore map[string]chan *Request
	errs         chan error
}

type StepConstruct func() Step

const (
	OutDegree = "out_degree"
	StepName  = "step_name"
)

func (p *Pipeline) handleDag(ctx context.Context, cancel context.CancelFunc, graph dag.DAG[StepConstruct], fstChannel chan *Request) chan *Request {
	var set []string
	exist := make(map[string]bool)
	for k, _ := range graph.GetSourceVertices() {
		set = append(set, k)
		exist[k] = true
	}

	for len(set) > 0 {
		sz := len(set)
		for i := 0; i < sz; i++ {
			var input []chan *Request
			vertex, err := graph.GetVertex(set[i])
			if err != nil {
				p.errs <- fmt.Errorf("get vertex error, error is %v", err)
				return nil
			}

			if vertex.Parents.Len() == 0 {
				input = append(input, fstChannel)
			} else {
				for _, parent := range vertex.Parents.Values() {
					input = append(input, p.channelStore[parent.ID])
				}
			}

			s := vertex.Value()

			ctx = context.WithValue(ctx, StepName, vertex.ID)

			// handle the last step
			out := max(1, int(vertex.Children.Len()))
			ctx = context.WithValue(ctx, OutDegree, out)
			p.channelStore[vertex.ID] = make(chan *Request, out)

			go s.Exec(ctx, cancel, p, input...)

			for _, chd := range vertex.Children.Values() {
				if !exist[chd.ID] {
					set = append(set, chd.ID)
					exist[chd.ID] = true
				}
			}
		}
		set = set[sz:]
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			for k, _ := range graph.GetSinkVertices() {
				return p.channelStore[k]
			}
		}
	}
}

func (p *Pipeline) handleErrors(err <-chan error) error {
	return nil
}

func (p *Pipeline) Exec(req *Request, graph dag.DAG[StepConstruct]) (*Request, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer func() {
		cancel()
		close(p.errs)
	}()

	// check graph is given by one source and one out
	if p.preCheck(graph) {
		reqch := make(chan *Request, 1)

		if req == nil {
			req = &Request{}
		}
		reqch <- req
		close(reqch)

		result := p.handleDag(ctx, cancel, graph, reqch)
		if result != nil {
			for res := range result {
				if res != nil {
					return res, nil
				}
			}
		}

		return nil, p.handleErrors(p.errs)
	}

	return nil, fmt.Errorf("dag dont match requirements")
}

func (p *Pipeline) preCheck(graph dag.DAG[StepConstruct]) bool {
	if len(graph.GetSourceVertices()) == 1 && len(graph.GetSinkVertices()) == 1 {
		return true
	}
	return false
}

func NewPipeline() *Pipeline {
	return &Pipeline{
		channelStore: make(map[string]chan *Request),
		errs:         make(chan error),
	}
}

func max[T constraints.Ordered](a, b T) T {
	if a > b {
		return a
	}
	return b
}
