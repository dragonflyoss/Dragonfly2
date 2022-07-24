package pipeline

import (
	"context"
	"sync"
)

// Result is returned by a step to dispatch data to the next step or stage
type Result struct {
	Error error
	// dispatch any type
	Data interface{}
	// dispatch key value pairs
	KeyVal map[string]interface{}
}

type ResultGroup struct {
	sync.RWMutex
	cancel func()
	result *Result
	wg     sync.WaitGroup
	// if key-val changed we will keep, if in concurrency
	// the same key has changed in different step, we only
	// keep one change
	copy map[string]interface{}
}

func (rg *ResultGroup) MergeResult(r *Result) {
	rg.Lock()
	defer rg.Unlock()

	if r == nil {
		return
	}
	if rg.result == nil {
		rg.result = &Result{
			KeyVal: make(map[string]interface{}),
		}
	}

	for k, v := range r.KeyVal {
		_, ok := rg.result.KeyVal[k]
		if ok && rg.copy[k] == v {
			continue
		}
		rg.result.KeyVal[k] = v
	}

}

func (rg *ResultGroup) Start(f func() *Result) {
	rg.wg.Add(1)
	go func() {
		result := f()
		rg.MergeResult(result)
		if result.Error != nil {
			rg.cancel()
		}
		rg.wg.Done()
	}()
}

func NewGroup(ctx context.Context) (*ResultGroup, context.Context) {
	ctx, cancelFunc := context.WithCancel(ctx)
	return &ResultGroup{cancel: cancelFunc}, ctx
}

func (rg *ResultGroup) Wait() {
	rg.wg.Wait()
	if rg.cancel != nil {
		rg.cancel()
	}
}
