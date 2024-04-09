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

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"testing"
	"time"

	"github.com/hashicorp/memberlist"
	"github.com/phayes/freeport"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/grpclog"
	"google.golang.org/protobuf/types/known/emptypb"

	commonv1 "d7y.io/api/v2/pkg/apis/common/v1"
	dfdaemonv1 "d7y.io/api/v2/pkg/apis/dfdaemon/v1"

	"d7y.io/dragonfly/v2/pkg/retry"
)

func TestPeerExchange(t *testing.T) {
	grpclog.SetLoggerV2(grpclog.NewLoggerV2(io.Discard, io.Discard, io.Discard))
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
			memberCount: 10,
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

			// 1. ensure members' connections
			// for large scale members, need wait more time for inter-connections ready
			_, _, err := retry.Run(context.Background(), float64(memberCount/3), 10, 10, func() (data any, cancel bool, err error) {
				var shouldRetry bool
				for _, pex := range pexServers {
					memberKeys := pex.memberManager.memberPool.MemberKeys()
					if memberCount-1 != len(memberKeys) {
						shouldRetry = true
					}
				}
				if shouldRetry {
					return nil, false, fmt.Errorf("members count not match")
				}
				return nil, false, nil
			})

			// make sure all members are ready
			if err != nil {
				for _, pex := range pexServers {
					memberKeys := pex.memberManager.memberPool.MemberKeys()
					assert.Equal(memberCount-1, len(memberKeys),
						fmt.Sprintf("%s should have %d members", pex.localMember.HostID, memberCount-1))
				}
				return
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
					searchPeerResult := pex.PeerSearchBroadcaster().SearchPeer(peer.TaskId)
					// check peer state
					switch peer.State {
					case dfdaemonv1.PeerState_Running, dfdaemonv1.PeerState_Success:
						assert.Truef(searchPeerResult.Type != SearchPeerResultTypeNotFound,
							"%s should have task %s", pex.localMember.HostID, peer.TaskId)
						// other members + local member
						assert.Equalf(memberCount, len(searchPeerResult.Peers),
							"%s should have %d peers for task %s", pex.localMember.HostID, memberCount, peer.TaskId)

						// check all peers is in other members
						for _, realPeer := range searchPeerResult.Peers {
							if realPeer.isLocal {
								continue
							}
							var found bool
							found = isPeerExistInOtherPEXServers(pexServers, pex.localMember.HostID, peer, realPeer)
							assert.Truef(found, "peer %s/%s in %s should be found in other members", peer.TaskId, realPeer.PeerID, pex.localMember.HostID)
						}
					case dfdaemonv1.PeerState_Failed, dfdaemonv1.PeerState_Deleted:
						assert.Truef(searchPeerResult.Type == SearchPeerResultTypeNotFound, "%s should not have task %s", pex.localMember.HostID, peer.TaskId)
						assert.Equalf(0, len(searchPeerResult.Peers),
							"%s should not have any peers for task %s", pex.localMember.HostID, peer.TaskId)
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

func (m *mockServer) LeaveHost(ctx context.Context, empty *emptypb.Empty) (*emptypb.Empty, error) {
	panic("should not be invoked")
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
		RPCPort:   int32(member.rpcPort),
		ProxyPort: 0,
	}

	lister := NewStaticPeerMemberLister(members)

	pex, err := NewPeerExchange(
		func(task, peer string) error {
			return nil
		},
		lister,
		time.Minute,
		[]grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		},
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
			log.Fatalf("grpc Serve exited with error: %v", err)
		}
	}()
	go func() {
		if err := pex.Serve(memberMeta); err != nil {
			log.Fatalf("pex Serve exited with error: %v", err)
		}
	}()
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
