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
	"d7y.io/dragonfly/v2/scheduler/resource"
)

const (
	// DefaultAlgorithm is a rule-based scheduling algorithm.
	DefaultAlgorithm = "default"

	// MLAlgorithm is a machine learning scheduling algorithm.
	MLAlgorithm = "ml"

	// PluginAlgorithm is a scheduling algorithm based on plugin extension.
	PluginAlgorithm = "plugin"
)

type Evaluator interface {
	// Evaluate todo Normalization.
	Evaluate(parent *resource.Peer, child *resource.Peer, taskPieceCount int32) float64

	// IsBadNode determine if peer is a failed node.
	IsBadNode(peer *resource.Peer) bool
}

func New(algorithm string, pluginDir string) Evaluator {
	switch algorithm {
	case PluginAlgorithm:
		if plugin, err := LoadPlugin(pluginDir); err == nil {
			return plugin
		}
	// TODO Implement MLAlgorithm.
	case MLAlgorithm, DefaultAlgorithm:
		return NewEvaluatorBase()
	}

	return NewEvaluatorBase()
}
