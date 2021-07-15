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

package evaluator

import (
	"sort"
	"strings"
	"sync"
	"time"

	"d7y.io/dragonfly/v2/internal/idgen"
	"d7y.io/dragonfly/v2/pkg/safe"
	"d7y.io/dragonfly/v2/scheduler/config"
	"d7y.io/dragonfly/v2/scheduler/types"
)

type Evaluator interface {

	// Evaluate todo Normalization
	Evaluate(dst *types.PeerNode, src *types.PeerNode) float64

	// NeedAdjustParent determine whether the peerNode needs a new parent node
	NeedAdjustParent(peer *types.PeerNode) bool

	// IsBadNode determine if peerNode is a failed node
	IsBadNode(peer *types.PeerNode) bool
}

type Factory struct {
	lock                         sync.RWMutex
	evaluators                   map[string]Evaluator
	getEvaluatorFuncs            map[int]getEvaluatorFunc
	getEvaluatorFuncPriorityList []getEvaluatorFunc
	cache                        map[string]Evaluator
	cacheClearFunc               sync.Once
	abtest                       bool
	ascheduler                   string
	bscheduler                   string
}

var _ Evaluator = (*Factory)(nil)

func (ef *Factory) Evaluate(dst *types.PeerNode, src *types.PeerNode) float64 {
	return ef.get(dst.Task.TaskID).Evaluate(dst, src)
}

func (ef *Factory) NeedAdjustParent(peer *types.PeerNode) bool {
	return ef.get(peer.Task.TaskID).NeedAdjustParent(peer)
}

func (ef *Factory) IsBadNode(peer *types.PeerNode) bool {
	return ef.get(peer.Task.TaskID).IsBadNode(peer)
}

func NewEvaluatorFactory(cfg *config.SchedulerConfig) *Factory {
	factory := &Factory{
		evaluators:        make(map[string]Evaluator),
		getEvaluatorFuncs: map[int]getEvaluatorFunc{},
		cache:             map[string]Evaluator{},
		abtest:            cfg.ABTest,
		ascheduler:        cfg.AScheduler,
		bscheduler:        cfg.BScheduler,
	}
	return factory
}

var (
	m = make(map[string]Evaluator)
)

func Register(name string, evaluator Evaluator) {
	m[strings.ToLower(name)] = evaluator
}

func Get(name string) Evaluator {
	if eval, ok := m[strings.ToLower(name)]; ok {
		return eval
	}
	return nil
}

type getEvaluatorFunc func(taskID string) (string, bool)

func (ef *Factory) get(taskID string) Evaluator {
	ef.lock.RLock()

	evaluator, ok := ef.cache[taskID]
	if ok {
		ef.lock.RUnlock()
		return evaluator
	}

	if ef.abtest {
		name := ""
		if strings.HasSuffix(taskID, idgen.TwinsBSuffix) {
			if ef.bscheduler != "" {
				name = ef.bscheduler
			}
		} else {
			if ef.ascheduler != "" {
				name = ef.ascheduler
			}
		}
		if name != "" {
			evaluator, ok = ef.evaluators[name]
			if ok {
				ef.lock.RUnlock()
				ef.lock.Lock()
				ef.cache[taskID] = evaluator
				ef.lock.Unlock()
				return evaluator
			}
		}
	}

	for _, fun := range ef.getEvaluatorFuncPriorityList {
		name, ok := fun(taskID)
		if !ok {
			continue
		}
		evaluator, ok = ef.evaluators[name]
		if !ok {
			continue
		}

		ef.lock.RUnlock()
		ef.lock.Lock()
		ef.cache[taskID] = evaluator
		ef.lock.Unlock()
		return evaluator
	}
	return nil
}

func (ef *Factory) clearCache() {
	ef.lock.Lock()
	ef.cache = make(map[string]Evaluator)
	ef.lock.Unlock()
}

func (ef *Factory) add(name string, evaluator Evaluator) {
	ef.lock.Lock()
	ef.evaluators[name] = evaluator
	ef.lock.Unlock()
}

func (ef *Factory) addGetEvaluatorFunc(priority int, fun getEvaluatorFunc) {
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

func (ef *Factory) deleteGetEvaluatorFunc(priority int, fun getEvaluatorFunc) {
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

func (ef *Factory) Register(name string, evaluator Evaluator) {
	ef.cacheClearFunc.Do(func() {
		go safe.Call(func() {
			tick := time.NewTicker(time.Hour)
			for {
				select {
				case <-tick.C:
					ef.clearCache()
				}
			}
		})
	})
	ef.add(name, evaluator)
	ef.clearCache()
}

func (ef *Factory) RegisterGetEvaluatorFunc(priority int, fun getEvaluatorFunc) {
	ef.addGetEvaluatorFunc(priority, fun)
	ef.clearCache()
}
