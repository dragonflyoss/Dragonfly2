package pipeline

import "context"

type Step interface {
	Exec(ctx context.Context, cancel context.CancelFunc, p *Pipeline, input ...chan *Request)
}

type Request struct {
	Data   interface{}
	KeyVal map[string]interface{}
}
