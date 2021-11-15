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

	"d7y.io/dragonfly/v2/internal/idgen"
	"d7y.io/dragonfly/v2/scheduler/config"
	"d7y.io/dragonfly/v2/scheduler/supervisor"
)

type EvaluatorFactory interface {
	// Evaluate todo Normalization
	Evaluate(parent *supervisor.Peer, child *supervisor.Peer) float64

	// NeedAdjustParent determine whether the peer needs a new parent node
	NeedAdjustParent(peer *supervisor.Peer) bool

	// IsBadNode determine if peer is a failed node
	IsBadNode(peer *supervisor.Peer) bool
}

type evaluatorFactory struct {
	lock                         sync.RWMutex
	evaluators                   map[string]EvaluatorFactory
	getEvaluatorFuncs            map[int]getEvaluatorFunc
	getEvaluatorFuncPriorityList []getEvaluatorFunc
	cache                        map[string]EvaluatorFactory
	cacheClearFunc               sync.Once
	abtest                       bool
	aEvaluator                   string
	bEvaluator                   string
}

func NewEvaluatorFactory(cfg *config.SchedulerConfig) EvaluatorFactory {
	return &evaluatorFactory{
		evaluators:        make(map[string]EvaluatorFactory),
		getEvaluatorFuncs: map[int]getEvaluatorFunc{},
		cache:             map[string]EvaluatorFactory{},
		abtest:            cfg.ABTest,
		aEvaluator:        cfg.AEvaluator,
		bEvaluator:        cfg.BEvaluator,
	}
}

func (ef *evaluatorFactory) Evaluate(dst *supervisor.Peer, src *supervisor.Peer) float64 {
	return ef.get(dst.Task.ID).Evaluate(dst, src)
}

func (ef *evaluatorFactory) NeedAdjustParent(peer *supervisor.Peer) bool {
	return ef.get(peer.Task.ID).NeedAdjustParent(peer)
}

func (ef *evaluatorFactory) IsBadNode(peer *supervisor.Peer) bool {
	return ef.get(peer.Task.ID).IsBadNode(peer)
}

type getEvaluatorFunc func(taskID string) (string, bool)

func (ef *evaluatorFactory) get(taskID string) EvaluatorFactory {
	ef.lock.RLock()
	evaluator, ok := ef.cache[taskID]
	ef.lock.RUnlock()
	if ok {
		return evaluator
	}

	if ef.abtest {
		name := ""
		if strings.HasSuffix(taskID, idgen.TwinsBSuffix) {
			if ef.bEvaluator != "" {
				name = ef.bEvaluator
			}
		} else {
			if ef.aEvaluator != "" {
				name = ef.aEvaluator
			}
		}
		if name != "" {
			ef.lock.RLock()
			evaluator, ok = ef.evaluators[name]
			ef.lock.RUnlock()
			if ok {
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
		ef.lock.RLock()
		evaluator, ok = ef.evaluators[name]
		ef.lock.RUnlock()
		if !ok {
			continue
		}

		ef.lock.Lock()
		ef.cache[taskID] = evaluator
		ef.lock.Unlock()
		return evaluator
	}

	return nil
}

func (ef *evaluatorFactory) clearCache() {
	ef.lock.Lock()
	ef.cache = make(map[string]EvaluatorFactory)
	ef.lock.Unlock()
}

func (ef *evaluatorFactory) add(name string, evaluator EvaluatorFactory) {
	ef.lock.Lock()
	ef.evaluators[name] = evaluator
	ef.lock.Unlock()
}

func (ef *evaluatorFactory) addGetEvaluatorFunc(priority int, fun getEvaluatorFunc) {
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

func (ef *evaluatorFactory) deleteGetEvaluatorFunc(priority int, fun getEvaluatorFunc) {
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

func (ef *evaluatorFactory) Register(name string, evaluator EvaluatorFactory) {
	ef.add(name, evaluator)
	ef.clearCache()
}

func (ef *evaluatorFactory) RegisterGetEvaluatorFunc(priority int, fun getEvaluatorFunc) {
	ef.addGetEvaluatorFunc(priority, fun)
	ef.clearCache()
}
