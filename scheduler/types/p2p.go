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

package types

import "time"

type P2PEdge struct {
	parent *P2PNode // parent, provider
	child  *P2PNode // child, consumer
}

func (pe *P2PEdge) AddCost(cost int64) {
	if pe == nil {
		return
	}
	pe.CostHistory = append(pe.CostHistory, cost)
	if len(pe.CostHistory) > 20 {
		pe.CostHistory = pe.CostHistory[1:]
	}
}

type P2PNode struct {
	PeerID      string
	HostID      string
	Parent      *P2PNode
	Children    []*P2PNode
	Concurrency int8    // number of thread download from the node
	CostHistory []int64 // history of downloading one piece cost from the provider
	Leaf        bool
}

func (node *P2PNode) HasParent() bool {
	return node.Parent != nil
}

func (node *P2PNode) HistoryCost() time.Duration {

}
