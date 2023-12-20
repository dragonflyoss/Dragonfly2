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
	assert := assert.New(t)

	peers := []*dfdaemonv1.PeerMetadata{
		{
			TaskId: "task-1",
			PeerId: "peer-1",
			State:  dfdaemonv1.PeerState_Running,
		},
		{
			TaskId: "task-2",
			PeerId: "peer-2",
			State:  dfdaemonv1.PeerState_Running,
		},
		{
			TaskId: "task-3",
			PeerId: "peer-3",
			State:  dfdaemonv1.PeerState_Running,
		},
	}

	memberCount := 2
	peerExchangeServers := setupMembers(assert, memberCount)

	time.Sleep(time.Second)

	// 1. ensure members' connections
	for _, pex := range peerExchangeServers {
		memberKeys := pex.(*peerExchange).memberManager.memberPool.MemberKeys()
		assert.Equal(memberCount-1, len(memberKeys))
	}

	// 2. broadcast peers
	for _, peer := range peers {
		peerExchangeServers[0].PeerSearchBroadcaster().BroadcastPeer(peer)
	}

	time.Sleep(500 * time.Second)

	// 3. verify peers
	for _, peer := range peers {
		for _, pex := range peerExchangeServers {
			dp, ok := pex.PeerSearchBroadcaster().FindPeersByTask(peer.TaskId)
			assert.True(ok)
			assert.Equal(peer.PeerId, dp[0].PeerID)
		}
	}
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

func setupMember(assert *assert.Assertions, member *testMember, members []*memberlist.Node) PeerExchangeServer {
	listen, err := net.Listen("tcp", fmt.Sprintf(":%d", member.rpcPort))
	assert.Nil(err)

	memberMeta := &MemberMeta{
		HostID:    fmt.Sprintf("host-id-%d", member.idx),
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
	return pex
}

func setupMembers(assert *assert.Assertions, memberCount int) []PeerExchangeServer {
	var (
		testMembers         []*testMember
		members             []*memberlist.Node
		peerExchangeServers []PeerExchangeServer
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
