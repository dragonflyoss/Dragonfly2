package scheduler

import (
	"github.com/dragonflyoss/Dragonfly2/scheduler/types"
	"sort"
	"sync"
	"time"
)

type GetEvaluatorFunc func(task *types.Task) (string, bool)

var factory = &evaluatorFactory{
	lock:              new(sync.RWMutex),
	evaluators:        make(map[string]IPeerTaskEvaluator),
	getEvaluatorFuncs: map[int]GetEvaluatorFunc{},
	cache:             map[*types.Task]IPeerTaskEvaluator{},
	cacheClearFunc:    new(sync.Once),
}

type evaluatorFactory struct {
	lock                         *sync.RWMutex
	evaluators                   map[string]IPeerTaskEvaluator
	getEvaluatorFuncs            map[int]GetEvaluatorFunc
	getEvaluatorFuncPriorityList []GetEvaluatorFunc
	cache                        map[*types.Task]IPeerTaskEvaluator
	cacheClearFunc               *sync.Once
}

func (ef *evaluatorFactory) getEvaluator(task *types.Task) IPeerTaskEvaluator {
	ef.lock.RLock()

	evaluator, ok := ef.cache[task]
	if ok {
		ef.lock.RUnlock()
		return evaluator
	}

	for _, fun := range ef.getEvaluatorFuncPriorityList {
		name, ok := fun(task)
		if !ok {
			continue
		}
		evaluator, ok := ef.evaluators[name]
		if !ok {
			continue
		}

		ef.lock.RUnlock()
		ef.lock.Lock()
		ef.cache[task] = evaluator
		ef.lock.Unlock()
		return evaluator
	}
	return nil
}

func (ef *evaluatorFactory) clearCache() {
	ef.lock.Lock()
	ef.cache = make(map[*types.Task]IPeerTaskEvaluator)
	ef.lock.Unlock()
}

func (ef *evaluatorFactory) addEvaluator(name string, evaluator IPeerTaskEvaluator) {
	ef.lock.Lock()
	ef.evaluators[name] = evaluator
	ef.lock.Unlock()
}

func (ef *evaluatorFactory) addGetEvaluatorFunc(priority int, fun GetEvaluatorFunc) {
	ef.lock.Lock()
	defer ef.lock.Unlock()
	_, ok := ef.getEvaluatorFuncs[priority]
	if ok {
		return
	}
	ef.getEvaluatorFuncs[priority] = fun
	var priorities []int
	for p := range ef.getEvaluatorFuncs {
		priorities = append(priorities, p)
	}
	sort.Ints(priorities)
	ef.getEvaluatorFuncPriorityList = ef.getEvaluatorFuncPriorityList[:0]
	for i := len(priorities) - 1; i >= 0; i-- {
		ef.getEvaluatorFuncPriorityList = append(ef.getEvaluatorFuncPriorityList, ef.getEvaluatorFuncs[priorities[i]])
	}

}

func (ef *evaluatorFactory) deleteGetEvaluatorFunc(priority int, fun GetEvaluatorFunc) {
	ef.lock.Lock()

	delete(ef.getEvaluatorFuncs, priority)

	var priorities []int
	for p := range ef.getEvaluatorFuncs {
		priorities = append(priorities, p)
	}
	sort.Ints(priorities)
	ef.getEvaluatorFuncPriorityList = ef.getEvaluatorFuncPriorityList[:0]
	for i := len(priorities) - 1; i >= 0; i-- {
		ef.getEvaluatorFuncPriorityList = append(ef.getEvaluatorFuncPriorityList, ef.getEvaluatorFuncs[priorities[i]])
	}

	ef.lock.Unlock()
}

func RegisterEvaluator(name string, evaluator IPeerTaskEvaluator) {
	factory.cacheClearFunc.Do(func() {
		go func() {
			tick := time.NewTicker(time.Hour)
			for {
				select {
				case <-tick.C:
					factory.clearCache()
				}
			}
		}()
	})
	factory.addEvaluator(name, evaluator)
	factory.clearCache()
}

func RegisterGetEvaluatorFunc(priority int, fun GetEvaluatorFunc) {
	factory.addGetEvaluatorFunc(priority, fun)
	factory.clearCache()
}

func DeleteGetEvaluatorFunc(priority int, fun GetEvaluatorFunc) {
	factory.deleteGetEvaluatorFunc(priority, fun)
}
