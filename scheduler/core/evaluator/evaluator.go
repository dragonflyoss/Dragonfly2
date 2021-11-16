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
	"d7y.io/dragonfly/v2/scheduler/supervisor"
)

const (
	// DefaultAlgorithm is a rule-based scheduling algorithm
	DefaultAlgorithm = "default"

	// MLAlgorithm is a machine learning scheduling algorithm
	MLAlgorithm = "ml"

	// PluginAlgorithm is a scheduling algorithm based on plugin extension
	PluginAlgorithm = "plugin"
)

type Evaluator interface {
	// Evaluate todo Normalization
	Evaluate(parent *supervisor.Peer, child *supervisor.Peer) float64

	// NeedAdjustParent determine whether the peer needs a new parent node
	NeedAdjustParent(peer *supervisor.Peer) bool

	// IsBadNode determine if peer is a failed node
	IsBadNode(peer *supervisor.Peer) bool
}

type evaluator struct {
	strategy Evaluator
}

func New(algorithm string) Evaluator {
	switch algorithm {
	case PluginAlgorithm:
		if plugin, err := LoadPlugin(); err == nil {
			return &evaluator{strategy: plugin}
		}
	// TODO Implement MLAlgorithm
	case MLAlgorithm, DefaultAlgorithm:
		return &evaluator{strategy: NewEvaluatorBase()}
	}

	return &evaluator{strategy: NewEvaluatorBase()}
}

func (e *evaluator) Evaluate(dst *supervisor.Peer, src *supervisor.Peer) float64 {
	return e.strategy.Evaluate(dst, src)
}

func (e *evaluator) NeedAdjustParent(peer *supervisor.Peer) bool {
	return e.strategy.NeedAdjustParent(peer)
}

func (e *evaluator) IsBadNode(peer *supervisor.Peer) bool {
	return e.strategy.IsBadNode(peer)
}
