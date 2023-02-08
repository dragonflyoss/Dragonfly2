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

package peer

import "d7y.io/dragonfly/v2/pkg/idgen"

const (
	defaultPeerIDBufferSize = 1024
)

type IDGenerator interface {
	PeerID() string
}

type peerIDGenerator struct {
	ip string
	ch chan string
}

func NewPeerIDGenerator(ip string) IDGenerator {
	p := &peerIDGenerator{
		ip: ip,
		ch: make(chan string, defaultPeerIDBufferSize),
	}
	go p.run()
	return p
}

func (p *peerIDGenerator) PeerID() string {
	return <-p.ch
}

func (p *peerIDGenerator) run() {
	for {
		p.ch <- idgen.PeerIDV1(p.ip)
	}
}
