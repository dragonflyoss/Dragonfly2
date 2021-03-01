/*
 *     Copyright 2020 The Dragonfly Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package scheduler

import (
	"d7y.io/dragonfly/v2/scheduler/config"
	"d7y.io/dragonfly/v2/scheduler/types"
	"sort"
	"strings"
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

	if config.GetConfig().Scheduler.ABTest {
		name := ""
		if strings.HasSuffix(task.TaskId, "TB") {
			if config.GetConfig().Scheduler.BScheduler != "" {
				name = config.GetConfig().Scheduler.BScheduler
			}
		} else {
			if config.GetConfig().Scheduler.AScheduler != "" {
				name = config.GetConfig().Scheduler.AScheduler
			}
		}
		if name != "" {
			evaluator, ok = ef.evaluators[name]
			if ok {
				ef.lock.RUnlock()
				ef.lock.Lock()
				ef.cache[task] = evaluator
				ef.lock.Unlock()
				return evaluator
			}
		}
	}

	for _, fun := range ef.getEvaluatorFuncPriorityList {
		name, ok := fun(task)
		if !ok {
			continue
		}
		evaluator, ok = ef.evaluators[name]
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
