/*
 *     Copyright 2023 The Dragonfly Authors
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

import (
	"testing"

	"github.com/stretchr/testify/assert"

	dfdaemonv1 "d7y.io/api/v2/pkg/apis/dfdaemon/v1"
	managerv1 "d7y.io/api/v2/pkg/apis/manager/v1"
)

func TestPeerExchange(t *testing.T) {
	assert := assert.New(t)

	peerUpdateChan := make(chan *dfdaemonv1.PeerMetadata)
	ex, err := NewPeerExchange(&MemberMeta{
		IP:        "",
		RpcPort:   0,
		ProxyPort: 0,
	}, NewSeedPeerMemberLister(func() ([]*managerv1.SeedPeer, error) {
		return []*managerv1.SeedPeer{}, nil
	}), peerUpdateChan)

	assert.Nil(err, "")
	ex.Serve()
}
