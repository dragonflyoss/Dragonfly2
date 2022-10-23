package pipeline

import (
	"context"
	"sync"

	logger "d7y.io/dragonfly/v2/internal/dflog"
)

type StepInfra struct {
	name    string
	Handler HandlerFunc
}

// HandlerFunc first chan represent input
type HandlerFunc func(context.Context, chan *Request, chan *Request) error

func (si *StepInfra) Exec(ctx context.Context, cancel context.CancelFunc, p *Pipeline, input ...chan *Request) {
	var wg sync.WaitGroup
	// suc: rev output, in: merge request
	in := make(chan *Request, 10)
	out := make(chan *Request, 10)
	// transfer rq
	for _, stream := range input {
		go func(st chan *Request) {
			req := <-st
			in <- req
			wg.Done()
		}(stream)
	}
	wg.Add(len(input))
	go func() {
		wg.Wait()
		close(in)
	}()

	// application handler
	go func() {
		err := si.Handler(ctx, in, out)

		defer func(err error) {
			if err != nil {
				logger.Infof("error is %v", err)
				p.errs <- err
				cancel()
				return
			}
		}(err)
	}()

	outNum := ctx.Value(OutDegree).(int)
	stepName := ctx.Value(StepName).(string)
	multiStream, _ := p.channelStore.Get(stepName)

	var src *Request
	for {
		select {
		case src = <-out:
			if src == nil {
				return
			}
			// send what downstream need
			for i := 0; i < outNum; i++ {
				multiStream <- src
			}
			close(multiStream)
		case <-ctx.Done():
			return
		}
	}
}

func New(name string, h HandlerFunc) *StepInfra {
	return &StepInfra{
		name:    name,
		Handler: h,
	}
}
