package pipeline1

import (
	"context"
	"fmt"
)

type Function struct {
	Val     []interface{}
	Op      *Option
	Handler func(interface{}, *Function)
	// function state
	State interface{}
}

// TODO store val list for agg
func (f *Function) NewStepWrap() HandlerFunc {
	return func(ctx context.Context, in chan *Request) (*Request, error) {
		for {
			select {
			case <-ctx.Done():
				return nil, fmt.Errorf("has been canceled")
			case val := <-in:
				if val == nil {
					return &Request{Data: f.State}, nil
				}
				f.Handler(val, f)
			}
		}
	}
}

// TODO
func (f *Function) NewFunction(handler func(interface{}, *Function), opts ...OptionFunc) *Function {
	fn := &Function{}
	for _, o := range opts {
		o(fn.Op)
	}
	fn.Handler = handler
	return fn
}
