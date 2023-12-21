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
	"context"
	"fmt"
	"log"
	"net"
	"testing"
	"time"

	"github.com/hashicorp/memberlist"
	"github.com/phayes/freeport"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"

	commonv1 "d7y.io/api/v2/pkg/apis/common/v1"
	dfdaemonv1 "d7y.io/api/v2/pkg/apis/dfdaemon/v1"
)

func TestPeerExchange(t *testing.T) {
	testCases := []struct {
		name        string
		memberCount int
		genPeers    func() []*dfdaemonv1.PeerMetadata
	}{
		{
			name:        "normal peers",
			memberCount: 3,
			genPeers: func() []*dfdaemonv1.PeerMetadata {
				peers := []*dfdaemonv1.PeerMetadata{
					{
						TaskId: "task-1",
						PeerId: "peer-1",
						State:  dfdaemonv1.PeerState_Running,
					},
					{
						TaskId: "task-2",
						PeerId: "peer-2",
						State:  dfdaemonv1.PeerState_Success,
					},
					{
						TaskId: "task-3",
						PeerId: "peer-3",
						State:  dfdaemonv1.PeerState_Running,
					},
				}
				return peers
			},
		},
		{
			name:        "normal peers with deleted",
			memberCount: 3,
			genPeers: func() []*dfdaemonv1.PeerMetadata {
				peers := []*dfdaemonv1.PeerMetadata{
					{
						TaskId: "task-1",
						PeerId: "peer-1",
						State:  dfdaemonv1.PeerState_Running,
					},
					{
						TaskId: "task-2",
						PeerId: "peer-2",
						State:  dfdaemonv1.PeerState_Success,
					},
					{
						TaskId: "task-3",
						PeerId: "peer-3",
						State:  dfdaemonv1.PeerState_Deleted,
					},
				}
				return peers
			},
		},
		{
			name:        "normal peers with failed",
			memberCount: 3,
			genPeers: func() []*dfdaemonv1.PeerMetadata {
				peers := []*dfdaemonv1.PeerMetadata{
					{
						TaskId: "task-1",
						PeerId: "peer-1",
						State:  dfdaemonv1.PeerState_Running,
					},
					{
						TaskId: "task-2",
						PeerId: "peer-2",
						State:  dfdaemonv1.PeerState_Success,
					},
					{
						TaskId: "task-3",
						PeerId: "peer-3",
						State:  dfdaemonv1.PeerState_Deleted,
					},
					{
						TaskId: "task-4",
						PeerId: "peer-4",
						State:  dfdaemonv1.PeerState_Failed,
					},
				}
				return peers
			},
		},
		{
			name:        "normal peers with failed",
			memberCount: 30,
			genPeers: func() []*dfdaemonv1.PeerMetadata {
				peers := []*dfdaemonv1.PeerMetadata{
					{
						TaskId: "task-1",
						PeerId: "peer-1",
						State:  dfdaemonv1.PeerState_Running,
					},
					{
						TaskId: "task-2",
						PeerId: "peer-2",
						State:  dfdaemonv1.PeerState_Success,
					},
					{
						TaskId: "task-3",
						PeerId: "peer-3",
						State:  dfdaemonv1.PeerState_Deleted,
					},
					{
						TaskId: "task-4",
						PeerId: "peer-4",
						State:  dfdaemonv1.PeerState_Failed,
					},
				}
				return peers
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert := assert.New(t)

			memberCount := tc.memberCount
			peers := tc.genPeers()

			pexServers := setupMembers(assert, memberCount)

			// for large scale members, need wait more time for inter-connections ready
			time.Sleep(time.Duration(memberCount/3) * time.Second)

			// 1. ensure members' connections
			for _, pex := range pexServers {
				memberKeys := pex.memberManager.memberPool.MemberKeys()
				assert.Equal(memberCount-1, len(memberKeys))
			}

			// 2. broadcast peers in all pex servers
			for _, p := range peers {
				for _, pex := range pexServers {
					peer := &dfdaemonv1.PeerMetadata{
						TaskId: p.TaskId,
						PeerId: genPeerID(p, pex),
						State:  p.State,
					}
					pex.PeerSearchBroadcaster().BroadcastPeer(peer)
				}
			}

			time.Sleep(3 * time.Second)

			// 3. verify peers
			for _, peer := range peers {
				for _, pex := range pexServers {
					peersByTask, ok := pex.PeerSearchBroadcaster().FindPeersByTask(peer.TaskId)
					switch peer.State {
					case dfdaemonv1.PeerState_Running, dfdaemonv1.PeerState_Success:
						assert.True(ok)
						assert.Equal(memberCount-1, len(peersByTask))
						for _, realPeer := range peersByTask {
							var found bool
							found = isPeerExistInOtherPEXServers(pexServers, pex.localMember.HostID, peer, realPeer)
							assert.True(found)
						}
					case dfdaemonv1.PeerState_Failed, dfdaemonv1.PeerState_Deleted:
						assert.False(ok)
						assert.Equal(0, len(peersByTask))
						for _, realPeer := range peersByTask {
							var found bool
							found = isPeerExistInOtherPEXServers(pexServers, pex.localMember.HostID, peer, realPeer)
							assert.False(found)
						}
					default:

					}
				}
			}
		})
	}
}

func isPeerExistInOtherPEXServers(peerExchangeServers []*peerExchange, hostID string, peer *dfdaemonv1.PeerMetadata, real *DestPeer) bool {
	for _, pex := range peerExchangeServers {
		// skip for local
		if pex.localMember.HostID == hostID {
			continue
		}
		// verify peer with host id
		if genPeerID(peer, pex) == real.PeerID {
			return true
		}
	}
	return false
}

func genPeerID(peer *dfdaemonv1.PeerMetadata, pex *peerExchange) string {
	return peer.PeerId + "-" + pex.localMember.HostID
}

type mockServer struct {
	PeerExchangeFunc func(dfdaemonv1.Daemon_PeerExchangeServer) error
}

func (m *mockServer) Download(request *dfdaemonv1.DownRequest, server dfdaemonv1.Daemon_DownloadServer) error {
	panic("should not be invoked")
}

func (m *mockServer) GetPieceTasks(ctx context.Context, request *commonv1.PieceTaskRequest) (*commonv1.PiecePacket, error) {
	panic("should not be invoked")
}

func (m *mockServer) CheckHealth(ctx context.Context, empty *emptypb.Empty) (*emptypb.Empty, error) {
	panic("should not be invoked")
}

func (m *mockServer) SyncPieceTasks(server dfdaemonv1.Daemon_SyncPieceTasksServer) error {
	panic("should not be invoked")
}

func (m *mockServer) StatTask(ctx context.Context, request *dfdaemonv1.StatTaskRequest) (*emptypb.Empty, error) {
	panic("should not be invoked")
}

func (m *mockServer) ImportTask(ctx context.Context, request *dfdaemonv1.ImportTaskRequest) (*emptypb.Empty, error) {
	panic("should not be invoked")
}

func (m *mockServer) ExportTask(ctx context.Context, request *dfdaemonv1.ExportTaskRequest) (*emptypb.Empty, error) {
	panic("should not be invoked")
}

func (m *mockServer) DeleteTask(ctx context.Context, request *dfdaemonv1.DeleteTaskRequest) (*emptypb.Empty, error) {
	panic("should not be invoked")
}

func (m *mockServer) PeerExchange(server dfdaemonv1.Daemon_PeerExchangeServer) error {
	return m.PeerExchangeFunc(server)
}

type testMember struct {
	idx                 int
	rpcPort, gossipPort int
}

func setupMember(assert *assert.Assertions, member *testMember, members []*memberlist.Node) *peerExchange {
	listen, err := net.Listen("tcp", fmt.Sprintf(":%d", member.rpcPort))
	assert.Nil(err)

	memberMeta := &MemberMeta{
		HostID:    fmt.Sprintf("host-%d", member.idx),
		IP:        "127.0.0.1",
		RpcPort:   int32(member.rpcPort),
		ProxyPort: 0,
	}

	lister := NewStaticPeerMemberLister(members)

	pex, err := NewPeerExchange(memberMeta, lister,
		WithName(fmt.Sprintf("node-%d", member.idx)),
		WithBindPort(member.gossipPort),
		WithAdvertisePort(member.gossipPort),
		WithAdvertiseAddr("127.0.0.1"),
		WithInitialRetryInterval(10*time.Microsecond),
		WithReSyncInterval(10*time.Microsecond),
	)
	assert.Nil(err)

	ms := &mockServer{
		PeerExchangeFunc: pex.PeerExchangeRPC().PeerExchange,
	}

	s := grpc.NewServer()
	dfdaemonv1.RegisterDaemonServer(s, ms)
	go func() {
		if err := s.Serve(listen); err != nil {
			log.Fatalf("Server exited with error: %v", err)
		}
	}()
	go pex.Serve()
	return pex.(*peerExchange)
}

func setupMembers(assert *assert.Assertions, memberCount int) []*peerExchange {
	var (
		testMembers         []*testMember
		members             []*memberlist.Node
		peerExchangeServers []*peerExchange
	)

	ports, err := freeport.GetFreePorts(2 * memberCount)
	assert.Nil(err)

	for i := 0; i < memberCount; i++ {
		rpcPort, gossipPort := ports[2*i], ports[2*i+1]
		testMembers = append(testMembers, &testMember{
			idx:        i,
			rpcPort:    rpcPort,
			gossipPort: gossipPort,
		})
		members = append(members, &memberlist.Node{
			Addr: net.ParseIP("127.0.0.1"),
			Port: uint16(gossipPort),
		})
	}

	for i := 0; i < memberCount; i++ {
		peerExchangeServers = append(peerExchangeServers, setupMember(assert, testMembers[i], members))
	}
	return peerExchangeServers
}
