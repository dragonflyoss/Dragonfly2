package pipeline

import (
	"sync"

	"github.com/kevwan/mapreduce"
)

type Aggregate interface {
	Agg(*Result, *Result) *Result
}

type ResultPool struct {
	sync.RWMutex
	DataList []interface{}
}

func (rp *ResultPool) Merge(result *ResultPool, agg Aggregate) *Result {
	reduce, _ := mapreduce.MapReduce(func(source chan<- interface{}) {
		for _, data := range result.DataList {
			source <- data
		}
	}, func(item interface{}, writer mapreduce.Writer, cancel func(error)) {
		writer.Write(item)
	}, func(pipe <-chan interface{}, writer mapreduce.Writer, cancel func(error)) {
		var res *Result
		for i := range pipe {
			res = agg.Agg(res, i.(*Result))
		}
		writer.Write(res)
	})
	return reduce.(*Result)
}

func (rp *ResultPool) Add(r *Result) {
	rp.Lock()
	defer rp.Unlock()

	rp.DataList = append(rp.DataList, r)
}
