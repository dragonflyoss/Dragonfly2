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
	"math/big"

	"github.com/montanaflynn/stats"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/scheduler/resource"
)

const (
	// DefaultAlgorithm is a rule-based scheduling algorithm.
	DefaultAlgorithm = "default"

	// NetworkTopologyAlgorithm is a scheduling algorithm based on rules and network topology.
	NetworkTopologyAlgorithm = "nt"

	// MLAlgorithm is a machine learning scheduling algorithm.
	MLAlgorithm = "ml"

	// PluginAlgorithm is a scheduling algorithm based on plugin extension.
	PluginAlgorithm = "plugin"
)

const (
	// Maximum score.
	maxScore float64 = 1

	// Minimum score.
	minScore = 0
)

const (
	// Maximum number of elements.
	maxElementLen = 5

	// If the number of samples is greater than or equal to 30,
	// it is close to the normal distribution.
	normalDistributionLen = 30

	// When costs len is greater than or equal to 2,
	// the last cost can be compared and calculated.
	minAvailableCostLen = 2
)

// Evaluator is an interface that evaluates the parents.
type Evaluator interface {
	// EvaluateParents sort parents by evaluating multiple feature scores.
	EvaluateParents(parents []*resource.Peer, child *resource.Peer, taskPieceCount int32) []*resource.Peer

	// IsBadNode determine if peer is a failed node.
	IsBadNode(peer *resource.Peer) bool
}

// evaluator is an implementation of Evaluator.
type evaluator struct{}

// New returns a new Evaluator.
func New(algorithm string, pluginDir string, networkTopologyOptions ...NetworkTopologyOption) Evaluator {
	switch algorithm {
	case PluginAlgorithm:
		if plugin, err := LoadPlugin(pluginDir); err == nil {
			return plugin
		}
	case NetworkTopologyAlgorithm:
		return newEvaluatorNetworkTopology(networkTopologyOptions...)
	// TODO Implement MLAlgorithm.
	case MLAlgorithm, DefaultAlgorithm:
		return newEvaluatorBase()
	}

	return newEvaluatorBase()
}

// IsBadNode determine if peer is a failed node.
func (e *evaluator) IsBadNode(peer *resource.Peer) bool {
	if peer.FSM.Is(resource.PeerStateFailed) || peer.FSM.Is(resource.PeerStateLeave) || peer.FSM.Is(resource.PeerStatePending) ||
		peer.FSM.Is(resource.PeerStateReceivedTiny) || peer.FSM.Is(resource.PeerStateReceivedSmall) ||
		peer.FSM.Is(resource.PeerStateReceivedNormal) || peer.FSM.Is(resource.PeerStateReceivedEmpty) {
		peer.Log.Debugf("peer is bad node because peer status is %s", peer.FSM.Current())
		return true
	}

	// Determine whether to bad node based on piece download costs.
	costs := stats.LoadRawData(peer.PieceCosts())
	len := len(costs)
	// Peer has not finished downloading enough piece.
	if len < minAvailableCostLen {
		logger.Debugf("peer %s has not finished downloading enough piece, it can't be bad node", peer.ID)
		return false
	}

	lastCost := costs[len-1]
	mean, _ := stats.Mean(costs[:len-1]) // nolint: errcheck

	// Download costs does not meet the normal distribution,
	// if the last cost is twenty times more than mean, it is bad node.
	if len < normalDistributionLen {
		isBadNode := big.NewFloat(lastCost).Cmp(big.NewFloat(mean*20)) > 0
		logger.Debugf("peer %s mean is %.2f and it is bad node: %t", peer.ID, mean, isBadNode)
		return isBadNode
	}

	// Download costs satisfies the normal distribution,
	// last cost falling outside of three-sigma effect need to be adjusted parent,
	// refer to https://en.wikipedia.org/wiki/68%E2%80%9395%E2%80%9399.7_rule.
	stdev, _ := stats.StandardDeviation(costs[:len-1]) // nolint: errcheck
	isBadNode := big.NewFloat(lastCost).Cmp(big.NewFloat(mean+3*stdev)) > 0
	logger.Debugf("peer %s meet the normal distribution, costs mean is %.2f and standard deviation is %.2f, peer is bad node: %t",
		peer.ID, mean, stdev, isBadNode)
	return isBadNode
}
