/*
 *     Copyright 2024 The Dragonfly Authors
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

package pex

import "encoding/json"

type peerExchangeDelegate struct {
	meta *MemberMeta
}

type MemberMeta struct {
	// keep private field, so isLocal meta in other members' list is false
	isLocal   bool
	HostID    string
	IP        string
	RPCPort   int32
	ProxyPort int32
}

func newPeerExchangeDelegate(nodeMata *MemberMeta) *peerExchangeDelegate {
	return &peerExchangeDelegate{meta: nodeMata}
}

func (p *peerExchangeDelegate) NodeMeta(limit int) []byte {
	data, _ := json.Marshal(p.meta)
	return data
}

func (p *peerExchangeDelegate) NotifyMsg(bytes []byte) {
	return
}

func (p *peerExchangeDelegate) GetBroadcasts(overhead, limit int) [][]byte {
	return nil
}

func (p *peerExchangeDelegate) LocalState(join bool) []byte {
	return nil
}

func (p *peerExchangeDelegate) MergeRemoteState(buf []byte, join bool) {
	return
}
