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
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"d7y.io/dragonfly/v2/pkg/idgen"
	"d7y.io/dragonfly/v2/pkg/rpc/scheduler"
	"d7y.io/dragonfly/v2/scheduler/entity"
)

const (
	cdnHostType    = "cdn"
	clientHostType = "client"
)

const (
	mockIP      = "127.0.0.1"
	mockTaskURL = "https://example.com"
)

type factor struct {
	hostType           string
	securityDomain     string
	idc                string
	location           string
	netTopology        string
	totalUploadLoad    uint32
	currentUploadLoad  uint32
	finishedPieceCount int32
	hostUUID           string
	taskPieceCount     int32
}

func TestEvaluatorIsBadNode(t *testing.T) {
	tests := []struct {
		name   string
		peer   *factor
		expect func(t *testing.T, e Evaluator, peer *entity.Peer)
	}{
		{
			name: "peer is bad",
			peer: &factor{
				hostType: clientHostType,
			},
			expect: func(t *testing.T, e Evaluator, peer *entity.Peer) {
				assert := assert.New(t)
				peer.FSM.SetState(entity.PeerStateFailed)
				assert.Equal(e.IsBadNode(peer), true)
			},
		},
		{
			name: "peer is CDN",
			peer: &factor{
				hostType: cdnHostType,
			},
			expect: func(t *testing.T, e Evaluator, peer *entity.Peer) {
				assert := assert.New(t)
				assert.Equal(e.IsBadNode(peer), false)
			},
		},
		{
			name: "empty costs",
			peer: &factor{
				hostType: clientHostType,
			},
			expect: func(t *testing.T, e Evaluator, peer *entity.Peer) {
				assert := assert.New(t)
				assert.Equal(e.IsBadNode(peer), false)
			},
		},
		{
			name: "costs length is available and peer is not bad node",
			peer: &factor{
				hostType: clientHostType,
			},
			expect: func(t *testing.T, e Evaluator, peer *entity.Peer) {
				assert := assert.New(t)
				for _, v := range []int{1, 2, 3, 4, 5, 6, 7, 8, 9} {
					peer.PieceCosts.Add(v)
				}
				assert.Equal(e.IsBadNode(peer), false)
			},
		},
		{
			name: "costs length is available and peer is bad node",
			peer: &factor{
				hostType: clientHostType,
			},
			expect: func(t *testing.T, e Evaluator, peer *entity.Peer) {
				assert := assert.New(t)
				for _, v := range []int{1, 2, 3, 4, 5, 6, 7, 8, 181} {
					peer.PieceCosts.Add(v)
				}
				assert.Equal(e.IsBadNode(peer), true)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			task := entity.NewTask(idgen.TaskID(mockTaskURL, nil), mockTaskURL, 0, nil)
			mockPeerHost := &scheduler.PeerHost{
				Uuid:           uuid.NewString(),
				Ip:             "",
				HostName:       "",
				RpcPort:        0,
				DownPort:       0,
				SecurityDomain: "",
				Location:       "",
				Idc:            "",
			}

			var peer *entity.Peer
			if tc.peer.hostType == cdnHostType {
				childHost := entity.NewHost(mockPeerHost, entity.WithIsCDN(true))
				peer = entity.NewPeer(idgen.CDNPeerID(mockIP), task, childHost)
			} else {
				childHost := entity.NewHost(mockPeerHost)
				peer = entity.NewPeer(idgen.PeerID(mockIP), task, childHost)
			}

			e := NewEvaluatorBase()
			tc.expect(t, e, peer)
		})
	}
}
