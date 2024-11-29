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

package service

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	commonv1 "d7y.io/api/v2/pkg/apis/common/v1"
	commonv2 "d7y.io/api/v2/pkg/apis/common/v2"
	errordetailsv1 "d7y.io/api/v2/pkg/apis/errordetails/v1"
	managerv2 "d7y.io/api/v2/pkg/apis/manager/v2"
	schedulerv1 "d7y.io/api/v2/pkg/apis/scheduler/v1"
	schedulerv1mocks "d7y.io/api/v2/pkg/apis/scheduler/v1/mocks"

	"d7y.io/dragonfly/v2/internal/dferrors"
	"d7y.io/dragonfly/v2/manager/types"
	"d7y.io/dragonfly/v2/pkg/container/set"
	"d7y.io/dragonfly/v2/pkg/digest"
	"d7y.io/dragonfly/v2/pkg/idgen"
	nethttp "d7y.io/dragonfly/v2/pkg/net/http"
	"d7y.io/dragonfly/v2/pkg/rpc/common"
	pkgtypes "d7y.io/dragonfly/v2/pkg/types"
	"d7y.io/dragonfly/v2/scheduler/config"
	configmocks "d7y.io/dragonfly/v2/scheduler/config/mocks"
	resource "d7y.io/dragonfly/v2/scheduler/resource/standard"
	"d7y.io/dragonfly/v2/scheduler/scheduling"
	"d7y.io/dragonfly/v2/scheduler/scheduling/mocks"
)

var (
	mockSchedulerConfig = config.SchedulerConfig{
		RetryLimit:             10,
		RetryBackToSourceLimit: 3,
		RetryInterval:          10 * time.Millisecond,
		BackToSourceCount:      int(mockTaskBackToSourceLimit),
	}

	mockSeedPeerConfig = config.SeedPeerConfig{
		Enable:              true,
		TaskDownloadTimeout: 1 * time.Hour,
	}

	mockPeerHost = &schedulerv1.PeerHost{
		Id:       mockHostID,
		Ip:       "127.0.0.1",
		RpcPort:  8003,
		DownPort: 8001,
		Hostname: "baz",
		Location: mockHostLocation,
		Idc:      mockHostIDC,
	}

	mockTaskBackToSourceLimit   int32 = 200
	mockTaskURL                       = "http://example.com/foo"
	mockTaskID                        = idgen.TaskIDV2(mockTaskURL, mockTaskTag, mockTaskApplication, mockTaskFilteredQueryParams)
	mockTaskDigest                    = digest.New(digest.AlgorithmSHA256, "c71d239df91726fc519c6eb72d318ec65820627232b2f796219e87dcf35d0ab4")
	mockTaskTag                       = "d7y"
	mockTaskApplication               = "foo"
	mockTaskFilteredQueryParams       = []string{"bar"}
	mockTaskHeader                    = map[string]string{"Content-Length": "100", "Range": "bytes=0-99"}
	mockTaskPieceLength         int32 = 2048
	mockHostID                        = idgen.HostIDV2("127.0.0.1", "foo", false)
	mockSeedHostID                    = idgen.HostIDV2("127.0.0.1", "bar", true)
	mockHostLocation                  = "bas"
	mockHostIDC                       = "baz"
	mockPeerID                        = idgen.PeerIDV2()
	mockSeedPeerID                    = idgen.PeerIDV2()
	mockPeerRange                     = nethttp.Range{
		Start:  0,
		Length: 10,
	}
	mockURLMetaRange = "0-9"
	mockPieceMD5     = digest.New(digest.AlgorithmMD5, "86d3f3a95c324c9479bd8986968f4327")
	mockPiece        = resource.Piece{
		Number:      1,
		ParentID:    "foo",
		Offset:      2,
		Length:      10,
		Digest:      mockPieceMD5,
		TrafficType: commonv2.TrafficType_REMOTE_PEER,
		Cost:        1 * time.Minute,
		CreatedAt:   time.Now(),
	}
)

func TestService_NewV1(t *testing.T) {
	tests := []struct {
		name   string
		expect func(t *testing.T, s any)
	}{
		{
			name: "new service",
			expect: func(t *testing.T, s any) {
				assert := assert.New(t)
				assert.Equal(reflect.TypeOf(s).Elem().Name(), "V1")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			scheduling := mocks.NewMockScheduling(ctl)
			resource := resource.NewMockResource(ctl)
			dynconfig := configmocks.NewMockDynconfigInterface(ctl)

			tc.expect(t, NewV1(&config.Config{Scheduler: mockSchedulerConfig}, resource, scheduling, dynconfig))
		})
	}
}

func TestServiceV1_RegisterPeerTask(t *testing.T) {
	tests := []struct {
		name string
		req  *schedulerv1.PeerTaskRequest
		mock func(
			req *schedulerv1.PeerTaskRequest, mockPeer *resource.Peer, mockSeedPeer *resource.Peer,
			scheduling scheduling.Scheduling, res resource.Resource, hostManager resource.HostManager, taskManager resource.TaskManager, peerManager resource.PeerManager,
			ms *mocks.MockSchedulingMockRecorder, mr *resource.MockResourceMockRecorder, mh *resource.MockHostManagerMockRecorder, mt *resource.MockTaskManagerMockRecorder,
			mp *resource.MockPeerManagerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder,
		)
		expect func(t *testing.T, peer *resource.Peer, result *schedulerv1.RegisterResult, err error)
	}{
		{
			name: "task state is TaskStateRunning and it has available peer",
			req: &schedulerv1.PeerTaskRequest{
				UrlMeta: &commonv1.UrlMeta{
					Priority: commonv1.Priority_LEVEL0,
				},
				PeerHost: &schedulerv1.PeerHost{
					Id: mockRawHost.ID,
				},
			},
			mock: func(
				req *schedulerv1.PeerTaskRequest, mockPeer *resource.Peer, mockSeedPeer *resource.Peer,
				scheduling scheduling.Scheduling, res resource.Resource, hostManager resource.HostManager, taskManager resource.TaskManager, peerManager resource.PeerManager,
				ms *mocks.MockSchedulingMockRecorder, mr *resource.MockResourceMockRecorder, mh *resource.MockHostManagerMockRecorder, mt *resource.MockTaskManagerMockRecorder,
				mp *resource.MockPeerManagerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder,
			) {
				mockPeer.Task.FSM.SetState(resource.TaskStateRunning)
				mockSeedPeer.FSM.SetState(resource.PeerStateRunning)
				mockPeer.Task.StorePeer(mockSeedPeer)
				gomock.InOrder(
					mr.TaskManager().Return(taskManager).Times(1),
					mt.Load(gomock.Any()).Return(mockPeer.Task, true).Times(1),
					mr.HostManager().Return(hostManager).Times(1),
					mh.Load(gomock.Eq(mockPeer.Host.ID)).Return(mockPeer.Host, true).Times(1),
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Any()).Return(mockPeer, true).Times(1),
				)
			},
			expect: func(t *testing.T, peer *resource.Peer, result *schedulerv1.RegisterResult, err error) {
				assert := assert.New(t)
				assert.NoError(err)
				assert.Equal(result.TaskId, peer.Task.ID)
				assert.Equal(result.SizeScope, commonv1.SizeScope_NORMAL)
				assert.Equal(peer.NeedBackToSource.Load(), false)
			},
		},
		{
			name: "task state is TaskStatePending and priority is Priority_LEVEL1",
			req: &schedulerv1.PeerTaskRequest{
				UrlMeta: &commonv1.UrlMeta{
					Priority: commonv1.Priority_LEVEL0,
				},
				PeerHost: &schedulerv1.PeerHost{
					Id: mockRawHost.ID,
				},
			},
			mock: func(
				req *schedulerv1.PeerTaskRequest, mockPeer *resource.Peer, mockSeedPeer *resource.Peer,
				scheduler scheduling.Scheduling, res resource.Resource, hostManager resource.HostManager, taskManager resource.TaskManager, peerManager resource.PeerManager,
				ms *mocks.MockSchedulingMockRecorder, mr *resource.MockResourceMockRecorder, mh *resource.MockHostManagerMockRecorder, mt *resource.MockTaskManagerMockRecorder,
				mp *resource.MockPeerManagerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder,
			) {
				mockPeer.Task.FSM.SetState(resource.TaskStatePending)
				mockPeer.Task.Application = "baz"
				gomock.InOrder(
					mr.TaskManager().Return(taskManager).Times(1),
					mt.Load(gomock.Any()).Return(mockPeer.Task, true).Times(1),
					mr.HostManager().Return(hostManager).Times(1),
					mh.Load(gomock.Eq(mockPeer.Host.ID)).Return(mockPeer.Host, true).Times(1),
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Any()).Return(mockPeer, true).Times(1),
					md.GetApplications().Return([]*managerv2.Application{
						{
							Name: "baz",
							Priority: &managerv2.ApplicationPriority{
								Value: commonv2.Priority_LEVEL1,
							},
						},
					}, nil).Times(1),
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Delete(gomock.Any()).Return().Times(1),
				)
			},
			expect: func(t *testing.T, peer *resource.Peer, result *schedulerv1.RegisterResult, err error) {
				assert := assert.New(t)
				dferr, ok := err.(*dferrors.DfError)
				assert.True(ok)
				assert.Equal(dferr.Code, commonv1.Code_SchedForbidden)
				assert.Equal(peer.FSM.Current(), resource.PeerStateLeave)
			},
		},
		{
			name: "task state is TaskStateRunning and peer state is PeerStateFailed",
			req: &schedulerv1.PeerTaskRequest{
				UrlMeta: &commonv1.UrlMeta{
					Priority: commonv1.Priority_LEVEL0,
				},
				PeerHost: &schedulerv1.PeerHost{
					Id: mockRawHost.ID,
				},
			},
			mock: func(
				req *schedulerv1.PeerTaskRequest, mockPeer *resource.Peer, mockSeedPeer *resource.Peer,
				scheduling scheduling.Scheduling, res resource.Resource, hostManager resource.HostManager, taskManager resource.TaskManager, peerManager resource.PeerManager,
				ms *mocks.MockSchedulingMockRecorder, mr *resource.MockResourceMockRecorder, mh *resource.MockHostManagerMockRecorder, mt *resource.MockTaskManagerMockRecorder,
				mp *resource.MockPeerManagerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder,
			) {
				mockPeer.Task.FSM.SetState(resource.TaskStateRunning)
				mockSeedPeer.FSM.SetState(resource.PeerStateRunning)
				mockPeer.Task.StorePeer(mockSeedPeer)
				mockPeer.FSM.SetState(resource.PeerStateFailed)
				gomock.InOrder(
					mr.TaskManager().Return(taskManager).Times(1),
					mt.Load(gomock.Any()).Return(mockPeer.Task, true).Times(1),
					mr.HostManager().Return(hostManager).Times(1),
					mh.Load(gomock.Eq(mockPeer.Host.ID)).Return(mockPeer.Host, true).Times(1),
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Any()).Return(mockPeer, true).Times(1),
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Delete(gomock.Any()).Return().Times(1),
				)
			},
			expect: func(t *testing.T, peer *resource.Peer, result *schedulerv1.RegisterResult, err error) {
				assert := assert.New(t)
				dferr, ok := err.(*dferrors.DfError)
				assert.True(ok)
				assert.Equal(dferr.Code, commonv1.Code_SchedError)
				assert.Equal(peer.NeedBackToSource.Load(), false)
				assert.Equal(peer.FSM.Current(), resource.PeerStateLeave)
			},
		},
		{
			name: "task scope size is SizeScope_EMPTY",
			req: &schedulerv1.PeerTaskRequest{
				UrlMeta: &commonv1.UrlMeta{
					Priority: commonv1.Priority_LEVEL0,
				},
				PeerHost: &schedulerv1.PeerHost{
					Id: mockRawHost.ID,
				},
			},
			mock: func(
				req *schedulerv1.PeerTaskRequest, mockPeer *resource.Peer, mockSeedPeer *resource.Peer,
				scheduling scheduling.Scheduling, res resource.Resource, hostManager resource.HostManager, taskManager resource.TaskManager, peerManager resource.PeerManager,
				ms *mocks.MockSchedulingMockRecorder, mr *resource.MockResourceMockRecorder, mh *resource.MockHostManagerMockRecorder, mt *resource.MockTaskManagerMockRecorder,
				mp *resource.MockPeerManagerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder,
			) {
				mockPeer.Task.FSM.SetState(resource.TaskStateSucceeded)
				mockSeedPeer.FSM.SetState(resource.PeerStateRunning)
				mockPeer.Task.StorePeer(mockSeedPeer)
				mockPeer.Task.ContentLength.Store(0)
				gomock.InOrder(
					mr.TaskManager().Return(taskManager).Times(1),
					mt.Load(gomock.Any()).Return(mockPeer.Task, true).Times(1),
					mr.HostManager().Return(hostManager).Times(1),
					mh.Load(gomock.Eq(mockPeer.Host.ID)).Return(mockPeer.Host, true).Times(1),
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Any()).Return(mockPeer, true).Times(1),
				)
			},
			expect: func(t *testing.T, peer *resource.Peer, result *schedulerv1.RegisterResult, err error) {
				assert := assert.New(t)
				assert.NoError(err)
				assert.Equal(result.TaskId, peer.Task.ID)
				assert.Equal(result.SizeScope, commonv1.SizeScope_EMPTY)
				assert.Equal(result.DirectPiece, &schedulerv1.RegisterResult_PieceContent{
					PieceContent: []byte{},
				})
				assert.Equal(peer.NeedBackToSource.Load(), false)
			},
		},
		{
			name: "task scope size is SizeScope_TINY",
			req: &schedulerv1.PeerTaskRequest{
				UrlMeta: &commonv1.UrlMeta{
					Priority: commonv1.Priority_LEVEL0,
				},
				PeerHost: &schedulerv1.PeerHost{
					Id: mockRawHost.ID,
				},
			},
			mock: func(
				req *schedulerv1.PeerTaskRequest, mockPeer *resource.Peer, mockSeedPeer *resource.Peer,
				scheduling scheduling.Scheduling, res resource.Resource, hostManager resource.HostManager, taskManager resource.TaskManager, peerManager resource.PeerManager,
				ms *mocks.MockSchedulingMockRecorder, mr *resource.MockResourceMockRecorder, mh *resource.MockHostManagerMockRecorder, mt *resource.MockTaskManagerMockRecorder,
				mp *resource.MockPeerManagerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder,
			) {
				mockPeer.Task.FSM.SetState(resource.TaskStateSucceeded)
				mockSeedPeer.FSM.SetState(resource.PeerStateRunning)
				mockPeer.Task.StorePeer(mockSeedPeer)
				mockPeer.Task.ContentLength.Store(1)
				mockPeer.Task.TotalPieceCount.Store(1)
				mockPeer.Task.DirectPiece = []byte{1}
				gomock.InOrder(
					mr.TaskManager().Return(taskManager).Times(1),
					mt.Load(gomock.Any()).Return(mockPeer.Task, true).Times(1),
					mr.HostManager().Return(hostManager).Times(1),
					mh.Load(gomock.Eq(mockPeer.Host.ID)).Return(mockPeer.Host, true).Times(1),
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Any()).Return(mockPeer, true).Times(1),
				)
			},
			expect: func(t *testing.T, peer *resource.Peer, result *schedulerv1.RegisterResult, err error) {
				assert := assert.New(t)
				assert.NoError(err)
				assert.Equal(result.TaskId, peer.Task.ID)
				assert.Equal(result.SizeScope, commonv1.SizeScope_TINY)
				assert.Equal(result.DirectPiece, &schedulerv1.RegisterResult_PieceContent{
					PieceContent: peer.Task.DirectPiece,
				})
				assert.Equal(peer.NeedBackToSource.Load(), false)
			},
		},
		{
			name: "task scope size is SizeScope_TINY and direct piece content is error",
			req: &schedulerv1.PeerTaskRequest{
				UrlMeta: &commonv1.UrlMeta{
					Priority: commonv1.Priority_LEVEL0,
				},
				PeerHost: &schedulerv1.PeerHost{
					Id: mockRawHost.ID,
				},
			},
			mock: func(
				req *schedulerv1.PeerTaskRequest, mockPeer *resource.Peer, mockSeedPeer *resource.Peer,
				scheduling scheduling.Scheduling, res resource.Resource, hostManager resource.HostManager, taskManager resource.TaskManager, peerManager resource.PeerManager,
				ms *mocks.MockSchedulingMockRecorder, mr *resource.MockResourceMockRecorder, mh *resource.MockHostManagerMockRecorder, mt *resource.MockTaskManagerMockRecorder,
				mp *resource.MockPeerManagerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder,
			) {
				mockPeer.Task.FSM.SetState(resource.TaskStateSucceeded)
				mockSeedPeer.FSM.SetState(resource.PeerStateRunning)
				mockPeer.Task.StorePeer(mockSeedPeer)
				mockPeer.Task.ContentLength.Store(1)
				mockPeer.Task.TotalPieceCount.Store(1)
				gomock.InOrder(
					mr.TaskManager().Return(taskManager).Times(1),
					mt.Load(gomock.Any()).Return(mockPeer.Task, true).Times(1),
					mr.HostManager().Return(hostManager).Times(1),
					mh.Load(gomock.Eq(mockPeer.Host.ID)).Return(mockPeer.Host, true).Times(1),
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Any()).Return(mockPeer, true).Times(1),
				)
			},
			expect: func(t *testing.T, peer *resource.Peer, result *schedulerv1.RegisterResult, err error) {
				assert := assert.New(t)
				assert.NoError(err)
				assert.Equal(result.TaskId, peer.Task.ID)
				assert.Equal(result.SizeScope, commonv1.SizeScope_NORMAL)
				assert.True(peer.FSM.Is(resource.PeerStateReceivedNormal))
				assert.Equal(peer.NeedBackToSource.Load(), false)
			},
		},
		{
			name: "task scope size is SizeScope_TINY and direct piece content is error, peer state is PeerStateFailed",
			req: &schedulerv1.PeerTaskRequest{
				UrlMeta: &commonv1.UrlMeta{
					Priority: commonv1.Priority_LEVEL0,
				},
				PeerHost: &schedulerv1.PeerHost{
					Id: mockRawHost.ID,
				},
			},
			mock: func(
				req *schedulerv1.PeerTaskRequest, mockPeer *resource.Peer, mockSeedPeer *resource.Peer,
				scheduling scheduling.Scheduling, res resource.Resource, hostManager resource.HostManager, taskManager resource.TaskManager, peerManager resource.PeerManager,
				ms *mocks.MockSchedulingMockRecorder, mr *resource.MockResourceMockRecorder, mh *resource.MockHostManagerMockRecorder, mt *resource.MockTaskManagerMockRecorder,
				mp *resource.MockPeerManagerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder,
			) {
				mockPeer.Task.FSM.SetState(resource.TaskStateSucceeded)
				mockSeedPeer.FSM.SetState(resource.PeerStateRunning)
				mockPeer.Task.StorePeer(mockSeedPeer)
				mockPeer.Task.ContentLength.Store(1)
				mockPeer.Task.TotalPieceCount.Store(1)
				mockPeer.FSM.SetState(resource.PeerStateFailed)
				gomock.InOrder(
					mr.TaskManager().Return(taskManager).Times(1),
					mt.Load(gomock.Any()).Return(mockPeer.Task, true).Times(1),
					mr.HostManager().Return(hostManager).Times(1),
					mh.Load(gomock.Eq(mockPeer.Host.ID)).Return(mockPeer.Host, true).Times(1),
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Any()).Return(mockPeer, true).Times(1),
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Delete(gomock.Any()).Return().Times(1),
				)
			},
			expect: func(t *testing.T, peer *resource.Peer, result *schedulerv1.RegisterResult, err error) {
				assert := assert.New(t)
				dferr, ok := err.(*dferrors.DfError)
				assert.True(ok)
				assert.Equal(dferr.Code, commonv1.Code_SchedError)
				assert.Equal(peer.NeedBackToSource.Load(), false)
				assert.Equal(peer.FSM.Current(), resource.PeerStateLeave)
			},
		},
		{
			name: "task scope size is SizeScope_SMALL and load piece error, parent state is PeerStateRunning",
			req: &schedulerv1.PeerTaskRequest{
				UrlMeta: &commonv1.UrlMeta{
					Priority: commonv1.Priority_LEVEL0,
				},
				PeerHost: &schedulerv1.PeerHost{
					Id: mockRawHost.ID,
				},
			},
			mock: func(
				req *schedulerv1.PeerTaskRequest, mockPeer *resource.Peer, mockSeedPeer *resource.Peer,
				scheduling scheduling.Scheduling, res resource.Resource, hostManager resource.HostManager, taskManager resource.TaskManager, peerManager resource.PeerManager,
				ms *mocks.MockSchedulingMockRecorder, mr *resource.MockResourceMockRecorder, mh *resource.MockHostManagerMockRecorder, mt *resource.MockTaskManagerMockRecorder,
				mp *resource.MockPeerManagerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder,
			) {
				mockPeer.Task.FSM.SetState(resource.TaskStateSucceeded)
				mockPeer.Task.StorePeer(mockPeer)
				mockPeer.Task.StorePeer(mockSeedPeer)
				mockPeer.Task.ContentLength.Store(129)
				mockPeer.Task.StorePiece(&resource.Piece{
					Number: 0,
				})
				mockPeer.Task.TotalPieceCount.Store(1)
				mockPeer.FSM.SetState(resource.PeerStatePending)
				mockSeedPeer.FSM.SetState(resource.PeerStateRunning)

				gomock.InOrder(
					mr.TaskManager().Return(taskManager).Times(1),
					mt.Load(gomock.Any()).Return(mockPeer.Task, true).Times(1),
					mr.HostManager().Return(hostManager).Times(1),
					mh.Load(gomock.Eq(mockPeer.Host.ID)).Return(mockPeer.Host, true).Times(1),
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Any()).Return(mockPeer, true).Times(1),
					ms.FindParentAndCandidateParents(gomock.Any(), gomock.Any(), gomock.Any()).Return([]*resource.Peer{mockSeedPeer}, true).Times(1),
				)
			},
			expect: func(t *testing.T, peer *resource.Peer, result *schedulerv1.RegisterResult, err error) {
				assert := assert.New(t)
				assert.NoError(err)
				assert.Equal(result.TaskId, peer.Task.ID)
				assert.Equal(result.SizeScope, commonv1.SizeScope_NORMAL)
				assert.True(peer.FSM.Is(resource.PeerStateReceivedNormal))
				assert.Equal(peer.NeedBackToSource.Load(), false)
			},
		},
		{
			name: "task scope size is SizeScope_SMALL and load piece error, peer state is PeerStateFailed",
			req: &schedulerv1.PeerTaskRequest{
				UrlMeta: &commonv1.UrlMeta{
					Priority: commonv1.Priority_LEVEL0,
				},
				PeerHost: &schedulerv1.PeerHost{
					Id: mockRawHost.ID,
				},
			},
			mock: func(
				req *schedulerv1.PeerTaskRequest, mockPeer *resource.Peer, mockSeedPeer *resource.Peer,
				scheduling scheduling.Scheduling, res resource.Resource, hostManager resource.HostManager, taskManager resource.TaskManager, peerManager resource.PeerManager,
				ms *mocks.MockSchedulingMockRecorder, mr *resource.MockResourceMockRecorder, mh *resource.MockHostManagerMockRecorder, mt *resource.MockTaskManagerMockRecorder,
				mp *resource.MockPeerManagerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder,
			) {
				mockPeer.Task.FSM.SetState(resource.TaskStateSucceeded)
				mockPeer.Task.StorePeer(mockPeer)
				mockPeer.Task.StorePeer(mockSeedPeer)
				mockPeer.Task.ContentLength.Store(129)
				mockPeer.Task.StorePiece(&resource.Piece{
					Number: 0,
				})
				mockPeer.Task.TotalPieceCount.Store(1)
				mockPeer.FSM.SetState(resource.PeerStateFailed)
				mockSeedPeer.FSM.SetState(resource.PeerStateSucceeded)

				gomock.InOrder(
					mr.TaskManager().Return(taskManager).Times(1),
					mt.Load(gomock.Any()).Return(mockPeer.Task, true).Times(1),
					mr.HostManager().Return(hostManager).Times(1),
					mh.Load(gomock.Eq(mockPeer.Host.ID)).Return(mockPeer.Host, true).Times(1),
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Any()).Return(mockPeer, true).Times(1),
					ms.FindParentAndCandidateParents(gomock.Any(), gomock.Any(), gomock.Any()).Return([]*resource.Peer{mockSeedPeer}, true).Times(1),
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Delete(gomock.Any()).Return().Times(1),
				)
			},
			expect: func(t *testing.T, peer *resource.Peer, result *schedulerv1.RegisterResult, err error) {
				assert := assert.New(t)
				dferr, ok := err.(*dferrors.DfError)
				assert.True(ok)
				assert.Equal(dferr.Code, commonv1.Code_SchedError)
				assert.Equal(peer.NeedBackToSource.Load(), false)
				assert.Equal(peer.FSM.Current(), resource.PeerStateLeave)
			},
		},
		{
			name: "task scope size is SizeScope_SMALL and peer state is PeerStateFailed",
			req: &schedulerv1.PeerTaskRequest{
				UrlMeta: &commonv1.UrlMeta{
					Priority: commonv1.Priority_LEVEL0,
				},
				PeerHost: &schedulerv1.PeerHost{
					Id: mockRawHost.ID,
				},
			},
			mock: func(
				req *schedulerv1.PeerTaskRequest, mockPeer *resource.Peer, mockSeedPeer *resource.Peer,
				scheduling scheduling.Scheduling, res resource.Resource, hostManager resource.HostManager, taskManager resource.TaskManager, peerManager resource.PeerManager,
				ms *mocks.MockSchedulingMockRecorder, mr *resource.MockResourceMockRecorder, mh *resource.MockHostManagerMockRecorder, mt *resource.MockTaskManagerMockRecorder,
				mp *resource.MockPeerManagerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder,
			) {
				mockPeer.Task.FSM.SetState(resource.TaskStateSucceeded)
				mockPeer.Task.StorePeer(mockPeer)
				mockPeer.Task.StorePeer(mockSeedPeer)
				mockPeer.Task.ContentLength.Store(129)
				mockPeer.Task.StorePiece(&resource.Piece{
					Number: 0,
				})
				mockPeer.Task.TotalPieceCount.Store(1)
				mockPeer.FSM.SetState(resource.PeerStateFailed)
				mockSeedPeer.FSM.SetState(resource.PeerStateSucceeded)

				gomock.InOrder(
					mr.TaskManager().Return(taskManager).Times(1),
					mt.Load(gomock.Any()).Return(mockPeer.Task, true).Times(1),
					mr.HostManager().Return(hostManager).Times(1),
					mh.Load(gomock.Eq(mockPeer.Host.ID)).Return(mockPeer.Host, true).Times(1),
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Any()).Return(mockPeer, true).Times(1),
					ms.FindParentAndCandidateParents(gomock.Any(), gomock.Any(), gomock.Any()).Return([]*resource.Peer{mockSeedPeer}, true).Times(1),
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Delete(gomock.Any()).Return().Times(1),
				)
			},
			expect: func(t *testing.T, peer *resource.Peer, result *schedulerv1.RegisterResult, err error) {
				assert := assert.New(t)
				dferr, ok := err.(*dferrors.DfError)
				assert.True(ok)
				assert.Equal(dferr.Code, commonv1.Code_SchedError)
				assert.Equal(peer.NeedBackToSource.Load(), false)
				assert.Equal(peer.FSM.Current(), resource.PeerStateLeave)
			},
		},
		{
			name: "task scope size is SizeScope_SMALL and vetex not found",
			req: &schedulerv1.PeerTaskRequest{
				UrlMeta: &commonv1.UrlMeta{
					Priority: commonv1.Priority_LEVEL0,
				},
				PeerHost: &schedulerv1.PeerHost{
					Id: mockRawHost.ID,
				},
			},
			mock: func(
				req *schedulerv1.PeerTaskRequest, mockPeer *resource.Peer, mockSeedPeer *resource.Peer,
				scheduling scheduling.Scheduling, res resource.Resource, hostManager resource.HostManager, taskManager resource.TaskManager, peerManager resource.PeerManager,
				ms *mocks.MockSchedulingMockRecorder, mr *resource.MockResourceMockRecorder, mh *resource.MockHostManagerMockRecorder, mt *resource.MockTaskManagerMockRecorder,
				mp *resource.MockPeerManagerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder,
			) {
				mockPeer.Task.FSM.SetState(resource.TaskStateSucceeded)
				mockPeer.Task.StorePeer(mockSeedPeer)
				mockPeer.Task.ContentLength.Store(129)
				mockPeer.Task.StorePiece(&resource.Piece{
					Number: 0,
				})
				mockPeer.Task.TotalPieceCount.Store(1)
				mockSeedPeer.FSM.SetState(resource.PeerStateSucceeded)

				gomock.InOrder(
					mr.TaskManager().Return(taskManager).Times(1),
					mt.Load(gomock.Any()).Return(mockPeer.Task, true).Times(1),
					mr.HostManager().Return(hostManager).Times(1),
					mh.Load(gomock.Eq(mockPeer.Host.ID)).Return(mockPeer.Host, true).Times(1),
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Any()).Return(mockPeer, true).Times(1),
					ms.FindParentAndCandidateParents(gomock.Any(), gomock.Any(), gomock.Any()).Return([]*resource.Peer{mockSeedPeer}, true).Times(1),
				)
			},
			expect: func(t *testing.T, peer *resource.Peer, result *schedulerv1.RegisterResult, err error) {
				assert := assert.New(t)
				assert.NoError(err)
				assert.Equal(result.TaskId, peer.Task.ID)
				assert.Equal(result.SizeScope, commonv1.SizeScope_NORMAL)
				assert.True(peer.FSM.Is(resource.PeerStateReceivedNormal))
				assert.Equal(peer.NeedBackToSource.Load(), false)
			},
		},
		{
			name: "task scope size is SizeScope_SMALL",
			req: &schedulerv1.PeerTaskRequest{
				UrlMeta: &commonv1.UrlMeta{
					Priority: commonv1.Priority_LEVEL0,
				},
				PeerHost: &schedulerv1.PeerHost{
					Id: mockRawHost.ID,
				},
			},
			mock: func(
				req *schedulerv1.PeerTaskRequest, mockPeer *resource.Peer, mockSeedPeer *resource.Peer,
				scheduling scheduling.Scheduling, res resource.Resource, hostManager resource.HostManager, taskManager resource.TaskManager, peerManager resource.PeerManager,
				ms *mocks.MockSchedulingMockRecorder, mr *resource.MockResourceMockRecorder, mh *resource.MockHostManagerMockRecorder, mt *resource.MockTaskManagerMockRecorder,
				mp *resource.MockPeerManagerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder,
			) {
				mockPeer.Task.FSM.SetState(resource.TaskStateSucceeded)
				mockPeer.Task.StorePeer(mockPeer)
				mockPeer.Task.StorePeer(mockSeedPeer)
				mockPeer.Task.ContentLength.Store(129)
				mockPeer.Task.StorePiece(&resource.Piece{
					Number: 0,
				})
				mockPeer.Task.TotalPieceCount.Store(1)
				mockSeedPeer.FSM.SetState(resource.PeerStateSucceeded)

				gomock.InOrder(
					mr.TaskManager().Return(taskManager).Times(1),
					mt.Load(gomock.Any()).Return(mockPeer.Task, true).Times(1),
					mr.HostManager().Return(hostManager).Times(1),
					mh.Load(gomock.Eq(mockPeer.Host.ID)).Return(mockPeer.Host, true).Times(1),
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Any()).Return(mockPeer, true).Times(1),
					ms.FindParentAndCandidateParents(gomock.Any(), gomock.Any(), gomock.Any()).Return([]*resource.Peer{mockSeedPeer}, true).Times(1),
				)
			},
			expect: func(t *testing.T, peer *resource.Peer, result *schedulerv1.RegisterResult, err error) {
				assert := assert.New(t)
				assert.NoError(err)
				assert.Equal(result.TaskId, peer.Task.ID)
				assert.Equal(result.SizeScope, commonv1.SizeScope_SMALL)
				assert.True(peer.FSM.Is(resource.PeerStateReceivedSmall))
				assert.Equal(peer.NeedBackToSource.Load(), false)
			},
		},
		{
			name: "task scope size is SizeScope_NORMAL and peer state is PeerStateFailed",
			req: &schedulerv1.PeerTaskRequest{
				UrlMeta: &commonv1.UrlMeta{
					Priority: commonv1.Priority_LEVEL0,
				},
				PeerHost: &schedulerv1.PeerHost{
					Id: mockRawHost.ID,
				},
			},
			mock: func(
				req *schedulerv1.PeerTaskRequest, mockPeer *resource.Peer, mockSeedPeer *resource.Peer,
				scheduling scheduling.Scheduling, res resource.Resource, hostManager resource.HostManager, taskManager resource.TaskManager, peerManager resource.PeerManager,
				ms *mocks.MockSchedulingMockRecorder, mr *resource.MockResourceMockRecorder, mh *resource.MockHostManagerMockRecorder, mt *resource.MockTaskManagerMockRecorder,
				mp *resource.MockPeerManagerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder,
			) {
				mockPeer.Task.FSM.SetState(resource.TaskStateSucceeded)
				mockSeedPeer.FSM.SetState(resource.PeerStateRunning)
				mockPeer.Task.StorePeer(mockSeedPeer)
				mockPeer.Task.ContentLength.Store(129)
				mockPeer.Task.TotalPieceCount.Store(2)
				mockPeer.FSM.SetState(resource.PeerStateFailed)
				gomock.InOrder(
					mr.TaskManager().Return(taskManager).Times(1),
					mt.Load(gomock.Any()).Return(mockPeer.Task, true).Times(1),
					mr.HostManager().Return(hostManager).Times(1),
					mh.Load(gomock.Eq(mockPeer.Host.ID)).Return(mockPeer.Host, true).Times(1),
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Any()).Return(mockPeer, true).Times(1),
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Delete(gomock.Any()).Return().Times(1),
				)
			},
			expect: func(t *testing.T, peer *resource.Peer, result *schedulerv1.RegisterResult, err error) {
				assert := assert.New(t)
				dferr, ok := err.(*dferrors.DfError)
				assert.True(ok)
				assert.Equal(dferr.Code, commonv1.Code_SchedError)
				assert.Equal(peer.NeedBackToSource.Load(), false)
				assert.Equal(peer.FSM.Current(), resource.PeerStateLeave)
			},
		},
		{
			name: "task scope size is SizeScope_NORMAL",
			req: &schedulerv1.PeerTaskRequest{
				UrlMeta: &commonv1.UrlMeta{
					Priority: commonv1.Priority_LEVEL0,
				},
				PeerHost: &schedulerv1.PeerHost{
					Id: mockRawHost.ID,
				},
			},
			mock: func(
				req *schedulerv1.PeerTaskRequest, mockPeer *resource.Peer, mockSeedPeer *resource.Peer,
				scheduling scheduling.Scheduling, res resource.Resource, hostManager resource.HostManager, taskManager resource.TaskManager, peerManager resource.PeerManager,
				ms *mocks.MockSchedulingMockRecorder, mr *resource.MockResourceMockRecorder, mh *resource.MockHostManagerMockRecorder, mt *resource.MockTaskManagerMockRecorder,
				mp *resource.MockPeerManagerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder,
			) {
				mockPeer.Task.FSM.SetState(resource.TaskStateSucceeded)
				mockSeedPeer.FSM.SetState(resource.PeerStateRunning)
				mockPeer.Task.StorePeer(mockSeedPeer)
				mockPeer.Task.ContentLength.Store(129)
				mockPeer.Task.TotalPieceCount.Store(2)
				gomock.InOrder(
					mr.TaskManager().Return(taskManager).Times(1),
					mt.Load(gomock.Any()).Return(mockPeer.Task, true).Times(1),
					mr.HostManager().Return(hostManager).Times(1),
					mh.Load(gomock.Eq(mockPeer.Host.ID)).Return(mockPeer.Host, true).Times(1),
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Any()).Return(mockPeer, true).Times(1),
				)
			},
			expect: func(t *testing.T, peer *resource.Peer, result *schedulerv1.RegisterResult, err error) {
				assert := assert.New(t)
				assert.NoError(err)
				assert.Equal(result.TaskId, peer.Task.ID)
				assert.Equal(result.SizeScope, commonv1.SizeScope_NORMAL)
				assert.True(peer.FSM.Is(resource.PeerStateReceivedNormal))
				assert.Equal(peer.NeedBackToSource.Load(), false)
			},
		},
		{
			name: "task scope size is SizeScope_UNKNOW",
			req: &schedulerv1.PeerTaskRequest{
				UrlMeta: &commonv1.UrlMeta{
					Priority: commonv1.Priority_LEVEL0,
				},
				PeerHost: &schedulerv1.PeerHost{
					Id: mockRawHost.ID,
				},
			},
			mock: func(
				req *schedulerv1.PeerTaskRequest, mockPeer *resource.Peer, mockSeedPeer *resource.Peer,
				scheduling scheduling.Scheduling, res resource.Resource, hostManager resource.HostManager, taskManager resource.TaskManager, peerManager resource.PeerManager,
				ms *mocks.MockSchedulingMockRecorder, mr *resource.MockResourceMockRecorder, mh *resource.MockHostManagerMockRecorder, mt *resource.MockTaskManagerMockRecorder,
				mp *resource.MockPeerManagerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder,
			) {
				mockPeer.Task.FSM.SetState(resource.TaskStateSucceeded)
				mockSeedPeer.FSM.SetState(resource.PeerStateRunning)
				mockPeer.Task.StorePeer(mockSeedPeer)
				mockPeer.Task.ContentLength.Store(-1)
				mockPeer.Task.TotalPieceCount.Store(2)
				gomock.InOrder(
					mr.TaskManager().Return(taskManager).Times(1),
					mt.Load(gomock.Any()).Return(mockPeer.Task, true).Times(1),
					mr.HostManager().Return(hostManager).Times(1),
					mh.Load(gomock.Eq(mockPeer.Host.ID)).Return(mockPeer.Host, true).Times(1),
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Any()).Return(mockPeer, true).Times(1),
				)
			},
			expect: func(t *testing.T, peer *resource.Peer, result *schedulerv1.RegisterResult, err error) {
				assert := assert.New(t)
				assert.NoError(err)
				assert.Equal(result.TaskId, peer.Task.ID)
				assert.Equal(result.SizeScope, commonv1.SizeScope_NORMAL)
				assert.True(peer.FSM.Is(resource.PeerStateReceivedNormal))
				assert.Equal(peer.NeedBackToSource.Load(), false)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			scheduling := mocks.NewMockScheduling(ctl)
			res := resource.NewMockResource(ctl)
			dynconfig := configmocks.NewMockDynconfigInterface(ctl)
			hostManager := resource.NewMockHostManager(ctl)
			taskManager := resource.NewMockTaskManager(ctl)
			peerManager := resource.NewMockPeerManager(ctl)
			svc := NewV1(&config.Config{Scheduler: mockSchedulerConfig}, res, scheduling, dynconfig)

			mockHost := resource.NewHost(
				mockRawHost.ID, mockRawHost.IP, mockRawHost.Hostname,
				mockRawHost.Port, mockRawHost.DownloadPort, mockRawHost.Type)
			mockTask := resource.NewTask(mockTaskID, mockTaskURL, mockTaskTag, mockTaskApplication, commonv2.TaskType_STANDARD, mockTaskFilteredQueryParams, mockTaskHeader, mockTaskBackToSourceLimit, resource.WithDigest(mockTaskDigest), resource.WithPieceLength(mockTaskPieceLength))
			mockPeer := resource.NewPeer(mockPeerID, mockTask, mockHost)
			mockSeedHost := resource.NewHost(
				mockRawSeedHost.ID, mockRawSeedHost.IP, mockRawSeedHost.Hostname,
				mockRawSeedHost.Port, mockRawSeedHost.DownloadPort, mockRawSeedHost.Type)
			mockSeedPeer := resource.NewPeer(mockSeedPeerID, mockTask, mockSeedHost)
			tc.mock(
				tc.req, mockPeer, mockSeedPeer,
				scheduling, res, hostManager, taskManager, peerManager,
				scheduling.EXPECT(), res.EXPECT(), hostManager.EXPECT(),
				taskManager.EXPECT(), peerManager.EXPECT(), dynconfig.EXPECT(),
			)

			result, err := svc.RegisterPeerTask(context.Background(), tc.req)
			tc.expect(t, mockPeer, result, err)
		})
	}
}

func TestServiceV1_ReportPieceResult(t *testing.T) {
	tests := []struct {
		name string
		mock func(
			mockPeer *resource.Peer,
			res resource.Resource, peerManager resource.PeerManager,
			mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder, ms *schedulerv1mocks.MockScheduler_ReportPieceResultServerMockRecorder,
		)
		expect func(t *testing.T, peer *resource.Peer, err error)
	}{
		{
			name: "context was done",
			mock: func(
				mockPeer *resource.Peer,
				res resource.Resource, peerManager resource.PeerManager,
				mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder, ms *schedulerv1mocks.MockScheduler_ReportPieceResultServerMockRecorder,

			) {
				ctx, cancel := context.WithCancel(context.Background())
				gomock.InOrder(
					ms.Context().Return(ctx).Times(1),
				)
				cancel()
			},
			expect: func(t *testing.T, peer *resource.Peer, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "context canceled")
			},
		},
		{
			name: "receive error",
			mock: func(
				mockPeer *resource.Peer,
				res resource.Resource, peerManager resource.PeerManager,
				mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder, ms *schedulerv1mocks.MockScheduler_ReportPieceResultServerMockRecorder,

			) {
				gomock.InOrder(
					ms.Context().Return(context.Background()).Times(1),
					ms.Recv().Return(nil, errors.New("foo")).Times(1),
				)
			},
			expect: func(t *testing.T, peer *resource.Peer, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "foo")
			},
		},
		{
			name: "receive EOF",
			mock: func(
				mockPeer *resource.Peer,
				res resource.Resource, peerManager resource.PeerManager,
				mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder, ms *schedulerv1mocks.MockScheduler_ReportPieceResultServerMockRecorder,

			) {
				gomock.InOrder(
					ms.Context().Return(context.Background()).Times(1),
					ms.Recv().Return(nil, io.EOF).Times(1),
				)
			},
			expect: func(t *testing.T, peer *resource.Peer, err error) {
				assert := assert.New(t)
				assert.NoError(err)
			},
		},
		{
			name: "load peer error",
			mock: func(
				mockPeer *resource.Peer,
				res resource.Resource, peerManager resource.PeerManager,
				mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder, ms *schedulerv1mocks.MockScheduler_ReportPieceResultServerMockRecorder,

			) {
				gomock.InOrder(
					ms.Context().Return(context.Background()).Times(1),
					ms.Recv().Return(&schedulerv1.PieceResult{
						SrcPid: mockPeerID,
					}, nil).Times(1),
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(mockPeerID)).Return(nil, false).Times(1),
				)
			},
			expect: func(t *testing.T, peer *resource.Peer, err error) {
				assert := assert.New(t)
				dferr, ok := err.(*dferrors.DfError)
				assert.True(ok)
				assert.Equal(dferr.Code, commonv1.Code_SchedReregister)
			},
		},
		{
			name: "revice begin of piece",
			mock: func(
				mockPeer *resource.Peer,
				res resource.Resource, peerManager resource.PeerManager,
				mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder, ms *schedulerv1mocks.MockScheduler_ReportPieceResultServerMockRecorder,

			) {
				mockPeer.FSM.SetState(resource.PeerStateBackToSource)
				gomock.InOrder(
					ms.Context().Return(context.Background()).Times(1),
					ms.Recv().Return(&schedulerv1.PieceResult{
						SrcPid: mockPeerID,
						PieceInfo: &commonv1.PieceInfo{
							PieceNum: common.BeginOfPiece,
						},
					}, nil).Times(1),
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(mockPeerID)).Return(mockPeer, true).Times(1),
					ms.Recv().Return(nil, io.EOF).Times(1),
				)
			},
			expect: func(t *testing.T, peer *resource.Peer, err error) {
				assert := assert.New(t)
				assert.NoError(err)
				_, loaded := peer.LoadReportPieceResultStream()
				assert.False(loaded)
			},
		},
		{
			name: "revice end of piece",
			mock: func(
				mockPeer *resource.Peer,
				res resource.Resource, peerManager resource.PeerManager,
				mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder, ms *schedulerv1mocks.MockScheduler_ReportPieceResultServerMockRecorder,

			) {
				gomock.InOrder(
					ms.Context().Return(context.Background()).Times(1),
					ms.Recv().Return(&schedulerv1.PieceResult{
						SrcPid: mockPeerID,
						PieceInfo: &commonv1.PieceInfo{
							PieceNum: common.EndOfPiece,
						},
					}, nil).Times(1),
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(mockPeerID)).Return(mockPeer, true).Times(1),
					ms.Recv().Return(nil, io.EOF).Times(1),
				)
			},
			expect: func(t *testing.T, peer *resource.Peer, err error) {
				assert := assert.New(t)
				assert.NoError(err)
				_, loaded := peer.LoadReportPieceResultStream()
				assert.False(loaded)
			},
		},
		{
			name: "revice successful piece",
			mock: func(
				mockPeer *resource.Peer,
				res resource.Resource, peerManager resource.PeerManager,
				mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder, ms *schedulerv1mocks.MockScheduler_ReportPieceResultServerMockRecorder,

			) {
				gomock.InOrder(
					ms.Context().Return(context.Background()).Times(1),
					ms.Recv().Return(&schedulerv1.PieceResult{
						SrcPid:  mockPeerID,
						Success: true,
						PieceInfo: &commonv1.PieceInfo{
							PieceNum: 1,
						},
					}, nil).Times(1),
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(mockPeerID)).Return(mockPeer, true).Times(1),
					ms.Recv().Return(nil, io.EOF).Times(1),
				)
			},
			expect: func(t *testing.T, peer *resource.Peer, err error) {
				assert := assert.New(t)
				assert.NoError(err)
				_, loaded := peer.LoadReportPieceResultStream()
				assert.False(loaded)
			},
		},
		{
			name: "revice Code_ClientWaitPieceReady code",
			mock: func(
				mockPeer *resource.Peer,
				res resource.Resource, peerManager resource.PeerManager,
				mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder, ms *schedulerv1mocks.MockScheduler_ReportPieceResultServerMockRecorder,

			) {
				gomock.InOrder(
					ms.Context().Return(context.Background()).Times(1),
					ms.Recv().Return(&schedulerv1.PieceResult{
						SrcPid: mockPeerID,
						Code:   commonv1.Code_ClientWaitPieceReady,
					}, nil).Times(1),
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(mockPeerID)).Return(mockPeer, true).Times(1),
					ms.Recv().Return(nil, io.EOF).Times(1),
				)
			},
			expect: func(t *testing.T, peer *resource.Peer, err error) {
				assert := assert.New(t)
				assert.NoError(err)
				_, loaded := peer.LoadReportPieceResultStream()
				assert.False(loaded)
			},
		},
		{
			name: "revice Code_PeerTaskNotFound code",
			mock: func(
				mockPeer *resource.Peer,
				res resource.Resource, peerManager resource.PeerManager,
				mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder, ms *schedulerv1mocks.MockScheduler_ReportPieceResultServerMockRecorder,

			) {
				mockPeer.FSM.SetState(resource.PeerStateBackToSource)
				gomock.InOrder(
					ms.Context().Return(context.Background()).Times(1),
					ms.Recv().Return(&schedulerv1.PieceResult{
						SrcPid: mockPeerID,
						Code:   commonv1.Code_PeerTaskNotFound,
					}, nil).Times(1),
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(mockPeerID)).Return(mockPeer, true).Times(1),
					ms.Recv().Return(nil, io.EOF).Times(1),
				)
			},
			expect: func(t *testing.T, peer *resource.Peer, err error) {
				assert := assert.New(t)
				assert.NoError(err)
				_, loaded := peer.LoadReportPieceResultStream()
				assert.False(loaded)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			scheduling := mocks.NewMockScheduling(ctl)
			res := resource.NewMockResource(ctl)
			dynconfig := configmocks.NewMockDynconfigInterface(ctl)
			peerManager := resource.NewMockPeerManager(ctl)
			stream := schedulerv1mocks.NewMockScheduler_ReportPieceResultServer(ctl)
			svc := NewV1(&config.Config{Scheduler: mockSchedulerConfig}, res, scheduling, dynconfig)

			mockHost := resource.NewHost(
				mockRawHost.ID, mockRawHost.IP, mockRawHost.Hostname,
				mockRawHost.Port, mockRawHost.DownloadPort, mockRawHost.Type)
			mockTask := resource.NewTask(mockTaskID, mockTaskURL, mockTaskTag, mockTaskApplication, commonv2.TaskType_STANDARD, mockTaskFilteredQueryParams, mockTaskHeader, mockTaskBackToSourceLimit, resource.WithDigest(mockTaskDigest), resource.WithPieceLength(mockTaskPieceLength))
			mockPeer := resource.NewPeer(mockPeerID, mockTask, mockHost)
			tc.mock(mockPeer, res, peerManager, res.EXPECT(), peerManager.EXPECT(), stream.EXPECT())
			tc.expect(t, mockPeer, svc.ReportPieceResult(stream))
		})
	}
}

func TestServiceV1_ReportPeerResult(t *testing.T) {
	tests := []struct {
		name string
		req  *schedulerv1.PeerResult
		run  func(t *testing.T, peer *resource.Peer, req *schedulerv1.PeerResult, svc *V1, mockPeer *resource.Peer, res resource.Resource, peerManager resource.PeerManager,
			mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder)
	}{
		{
			name: "peer not found",
			req: &schedulerv1.PeerResult{
				PeerId: mockPeerID,
			},
			run: func(t *testing.T, peer *resource.Peer, req *schedulerv1.PeerResult, svc *V1, mockPeer *resource.Peer, res resource.Resource, peerManager resource.PeerManager,
				mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(mockPeerID)).Return(nil, false).Times(1),
				)

				assert := assert.New(t)
				err := svc.ReportPeerResult(context.Background(), req)
				dferr, ok := err.(*dferrors.DfError)
				assert.True(ok)
				assert.Equal(dferr.Code, commonv1.Code_SchedPeerNotFound)
			},
		},
		{
			name: "receive peer failed",
			req: &schedulerv1.PeerResult{
				Success: false,
				PeerId:  mockPeerID,
			},
			run: func(t *testing.T, peer *resource.Peer, req *schedulerv1.PeerResult, svc *V1, mockPeer *resource.Peer, res resource.Resource, peerManager resource.PeerManager,
				mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				mockPeer.FSM.SetState(resource.PeerStateFailed)
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(mockPeerID)).Return(mockPeer, true).Times(1),
					md.GetApplications().Return([]*managerv2.Application{}, nil).Times(1),
				)

				assert := assert.New(t)
				err := svc.ReportPeerResult(context.Background(), req)
				assert.NoError(err)
			},
		},
		{
			name: "receive peer failed and peer state is PeerStateBackToSource",
			req: &schedulerv1.PeerResult{
				Success: false,
				PeerId:  mockPeerID,
			},
			run: func(t *testing.T, peer *resource.Peer, req *schedulerv1.PeerResult, svc *V1, mockPeer *resource.Peer, res resource.Resource, peerManager resource.PeerManager,
				mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				mockPeer.FSM.SetState(resource.PeerStateBackToSource)
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(mockPeerID)).Return(mockPeer, true).Times(1),
					md.GetApplications().Return([]*managerv2.Application{}, nil).Times(1),
				)

				assert := assert.New(t)
				err := svc.ReportPeerResult(context.Background(), req)
				assert.NoError(err)
			},
		},
		{
			name: "receive peer success",
			req: &schedulerv1.PeerResult{
				Success: true,
				PeerId:  mockPeerID,
			},
			run: func(t *testing.T, peer *resource.Peer, req *schedulerv1.PeerResult, svc *V1, mockPeer *resource.Peer, res resource.Resource, peerManager resource.PeerManager,
				mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				mockPeer.FSM.SetState(resource.PeerStateFailed)
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(mockPeerID)).Return(mockPeer, true).Times(1),
					md.GetApplications().Return([]*managerv2.Application{}, nil).Times(1),
				)

				assert := assert.New(t)
				err := svc.ReportPeerResult(context.Background(), req)
				assert.NoError(err)
			},
		},
		{
			name: "receive peer success, and peer state is PeerStateBackToSource",
			req: &schedulerv1.PeerResult{
				Success: true,
				PeerId:  mockPeerID,
			},
			run: func(t *testing.T, peer *resource.Peer, req *schedulerv1.PeerResult, svc *V1, mockPeer *resource.Peer, res resource.Resource, peerManager resource.PeerManager,
				mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				mockPeer.FSM.SetState(resource.PeerStateBackToSource)
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(mockPeerID)).Return(mockPeer, true).Times(1),
					md.GetApplications().Return([]*managerv2.Application{}, nil).Times(1),
				)

				assert := assert.New(t)
				err := svc.ReportPeerResult(context.Background(), req)
				assert.NoError(err)
			},
		},
		{
			name: "receive peer success and create download failed",
			req: &schedulerv1.PeerResult{
				Success: true,
				PeerId:  mockPeerID,
			},
			run: func(t *testing.T, peer *resource.Peer, req *schedulerv1.PeerResult, svc *V1, mockPeer *resource.Peer, res resource.Resource, peerManager resource.PeerManager,
				mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				mockPeer.FSM.SetState(resource.PeerStateBackToSource)
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(mockPeerID)).Return(mockPeer, true).Times(1),
					md.GetApplications().Return([]*managerv2.Application{}, nil).Times(1),
				)

				assert := assert.New(t)
				err := svc.ReportPeerResult(context.Background(), req)
				assert.NoError(err)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			scheduling := mocks.NewMockScheduling(ctl)
			res := resource.NewMockResource(ctl)
			dynconfig := configmocks.NewMockDynconfigInterface(ctl)
			peerManager := resource.NewMockPeerManager(ctl)
			svc := NewV1(&config.Config{Scheduler: mockSchedulerConfig}, res, scheduling, dynconfig)

			mockHost := resource.NewHost(
				mockRawHost.ID, mockRawHost.IP, mockRawHost.Hostname,
				mockRawHost.Port, mockRawHost.DownloadPort, mockRawHost.Type)
			mockTask := resource.NewTask(mockTaskID, mockTaskURL, mockTaskTag, mockTaskApplication, commonv2.TaskType_STANDARD, mockTaskFilteredQueryParams, mockTaskHeader, mockTaskBackToSourceLimit, resource.WithDigest(mockTaskDigest), resource.WithPieceLength(mockTaskPieceLength))
			mockPeer := resource.NewPeer(mockPeerID, mockTask, mockHost)
			tc.run(t, mockPeer, tc.req, svc, mockPeer, res, peerManager, res.EXPECT(), peerManager.EXPECT(), dynconfig.EXPECT())
		})
	}
}

func TestServiceV1_StatTask(t *testing.T) {
	tests := []struct {
		name   string
		mock   func(mockTask *resource.Task, taskManager resource.TaskManager, mr *resource.MockResourceMockRecorder, mt *resource.MockTaskManagerMockRecorder)
		expect func(t *testing.T, task *schedulerv1.Task, err error)
	}{
		{
			name: "task not found",
			mock: func(mockTask *resource.Task, taskManager resource.TaskManager, mr *resource.MockResourceMockRecorder, mt *resource.MockTaskManagerMockRecorder) {
				gomock.InOrder(
					mr.TaskManager().Return(taskManager).Times(1),
					mt.Load(gomock.Any()).Return(nil, false).Times(1),
				)
			},
			expect: func(t *testing.T, task *schedulerv1.Task, err error) {
				assert := assert.New(t)
				assert.Error(err)
			},
		},
		{
			name: "stat task",
			mock: func(mockTask *resource.Task, taskManager resource.TaskManager, mr *resource.MockResourceMockRecorder, mt *resource.MockTaskManagerMockRecorder) {
				gomock.InOrder(
					mr.TaskManager().Return(taskManager).Times(1),
					mt.Load(gomock.Any()).Return(mockTask, true).Times(1),
				)
			},
			expect: func(t *testing.T, task *schedulerv1.Task, err error) {
				assert := assert.New(t)
				assert.NoError(err)
				assert.EqualValues(task, &schedulerv1.Task{
					Id:               mockTaskID,
					Type:             pkgtypes.TaskTypeV2ToV1(commonv2.TaskType_STANDARD),
					ContentLength:    -1,
					TotalPieceCount:  0,
					State:            resource.TaskStatePending,
					PeerCount:        0,
					HasAvailablePeer: false,
				})
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			scheduling := mocks.NewMockScheduling(ctl)
			res := resource.NewMockResource(ctl)
			dynconfig := configmocks.NewMockDynconfigInterface(ctl)
			taskManager := resource.NewMockTaskManager(ctl)
			svc := NewV1(&config.Config{Scheduler: mockSchedulerConfig, Metrics: config.MetricsConfig{EnableHost: true}}, res, scheduling, dynconfig)
			mockTask := resource.NewTask(mockTaskID, mockTaskURL, mockTaskTag, mockTaskApplication, commonv2.TaskType_STANDARD, mockTaskFilteredQueryParams, mockTaskHeader, mockTaskBackToSourceLimit, resource.WithDigest(mockTaskDigest), resource.WithPieceLength(mockTaskPieceLength))

			tc.mock(mockTask, taskManager, res.EXPECT(), taskManager.EXPECT())
			task, err := svc.StatTask(context.Background(), &schedulerv1.StatTaskRequest{TaskId: mockTaskID})
			tc.expect(t, task, err)
		})
	}
}

func TestServiceV1_AnnounceTask(t *testing.T) {
	tests := []struct {
		name string
		req  *schedulerv1.AnnounceTaskRequest
		mock func(mockHost *resource.Host, mockTask *resource.Task, mockPeer *resource.Peer,
			hostManager resource.HostManager, taskManager resource.TaskManager, peerManager resource.PeerManager,
			mr *resource.MockResourceMockRecorder, mh *resource.MockHostManagerMockRecorder, mt *resource.MockTaskManagerMockRecorder, mp *resource.MockPeerManagerMockRecorder)
		expect func(t *testing.T, mockTask *resource.Task, mockPeer *resource.Peer, err error)
	}{
		{
			name: "task state is TaskStateSucceeded and peer state is PeerStateSucceeded",
			req: &schedulerv1.AnnounceTaskRequest{
				TaskId: mockTaskID,
				Url:    mockTaskURL,
				UrlMeta: &commonv1.UrlMeta{
					Priority: commonv1.Priority_LEVEL0,
				},
				PeerHost: &schedulerv1.PeerHost{
					Id: mockRawHost.ID,
				},
				PiecePacket: &commonv1.PiecePacket{
					PieceInfos: []*commonv1.PieceInfo{{PieceNum: 1}},
					TotalPiece: 1,
				},
			},
			mock: func(mockHost *resource.Host, mockTask *resource.Task, mockPeer *resource.Peer,
				hostManager resource.HostManager, taskManager resource.TaskManager, peerManager resource.PeerManager,
				mr *resource.MockResourceMockRecorder, mh *resource.MockHostManagerMockRecorder, mt *resource.MockTaskManagerMockRecorder, mp *resource.MockPeerManagerMockRecorder) {
				mockTask.FSM.SetState(resource.TaskStateSucceeded)
				mockPeer.FSM.SetState(resource.PeerStateSucceeded)

				gomock.InOrder(
					mr.TaskManager().Return(taskManager).Times(1),
					mt.LoadOrStore(gomock.Any()).Return(mockTask, true).Times(1),
					mr.HostManager().Return(hostManager).Times(1),
					mh.Load(gomock.Any()).Return(mockHost, true).Times(1),
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Any()).Return(mockPeer, true).Times(1),
				)
			},
			expect: func(t *testing.T, mockTask *resource.Task, mockPeer *resource.Peer, err error) {
				assert := assert.New(t)
				assert.NoError(err)
				assert.Equal(mockTask.FSM.Current(), resource.TaskStateSucceeded)
				assert.Equal(mockPeer.FSM.Current(), resource.PeerStateSucceeded)
			},
		},
		{
			name: "task state is TaskStatePending and peer state is PeerStateSucceeded",
			req: &schedulerv1.AnnounceTaskRequest{
				TaskId: mockTaskID,
				Url:    mockTaskURL,
				UrlMeta: &commonv1.UrlMeta{
					Priority: commonv1.Priority_LEVEL0,
				},
				PeerHost: mockPeerHost,
				PiecePacket: &commonv1.PiecePacket{
					PieceInfos: []*commonv1.PieceInfo{{
						PieceNum:     1,
						RangeStart:   0,
						RangeSize:    10,
						PieceMd5:     mockPieceMD5.Encoded,
						DownloadCost: 1,
					}},
					TotalPiece:    1,
					ContentLength: 1000,
				},
			},
			mock: func(mockHost *resource.Host, mockTask *resource.Task, mockPeer *resource.Peer,
				hostManager resource.HostManager, taskManager resource.TaskManager, peerManager resource.PeerManager,
				mr *resource.MockResourceMockRecorder, mh *resource.MockHostManagerMockRecorder, mt *resource.MockTaskManagerMockRecorder, mp *resource.MockPeerManagerMockRecorder) {
				mockTask.FSM.SetState(resource.TaskStatePending)
				mockPeer.FSM.SetState(resource.PeerStateSucceeded)

				gomock.InOrder(
					mr.TaskManager().Return(taskManager).Times(1),
					mt.LoadOrStore(gomock.Any()).Return(mockTask, true).Times(1),
					mr.HostManager().Return(hostManager).Times(1),
					mh.Load(gomock.Any()).Return(mockHost, true).Times(1),
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Any()).Return(mockPeer, true).Times(1),
				)
			},
			expect: func(t *testing.T, mockTask *resource.Task, mockPeer *resource.Peer, err error) {
				assert := assert.New(t)
				assert.NoError(err)
				assert.Equal(mockTask.FSM.Current(), resource.TaskStateSucceeded)
				assert.Equal(mockTask.TotalPieceCount.Load(), int32(1))
				assert.Equal(mockTask.ContentLength.Load(), int64(1000))
				piece, loaded := mockTask.LoadPiece(1)
				assert.True(loaded)
				assert.Equal(piece.Number, int32(1))
				assert.Equal(piece.Offset, uint64(0))
				assert.Equal(piece.Length, uint64(10))
				assert.EqualValues(piece.Digest, mockPieceMD5)
				assert.Equal(piece.TrafficType, commonv2.TrafficType_LOCAL_PEER)
				assert.Equal(piece.Cost, time.Duration(0))
				assert.NotEqual(piece.CreatedAt.Nanosecond(), 0)
				assert.Equal(mockPeer.FinishedPieces.Count(), uint(1))
				assert.Equal(mockPeer.PieceCosts()[0], time.Duration(0))
				assert.Equal(mockPeer.FSM.Current(), resource.PeerStateSucceeded)
			},
		},
		{
			name: "task state is TaskStateFailed and peer state is PeerStateSucceeded",
			req: &schedulerv1.AnnounceTaskRequest{
				TaskId: mockTaskID,
				Url:    mockTaskURL,
				UrlMeta: &commonv1.UrlMeta{
					Priority: commonv1.Priority_LEVEL0,
				},
				PeerHost: mockPeerHost,
				PiecePacket: &commonv1.PiecePacket{
					PieceInfos: []*commonv1.PieceInfo{{
						PieceNum:     1,
						RangeStart:   0,
						RangeSize:    10,
						PieceMd5:     mockPieceMD5.Encoded,
						DownloadCost: 1,
					}},
					TotalPiece:    1,
					ContentLength: 1000,
				},
			},
			mock: func(mockHost *resource.Host, mockTask *resource.Task, mockPeer *resource.Peer,
				hostManager resource.HostManager, taskManager resource.TaskManager, peerManager resource.PeerManager,
				mr *resource.MockResourceMockRecorder, mh *resource.MockHostManagerMockRecorder, mt *resource.MockTaskManagerMockRecorder, mp *resource.MockPeerManagerMockRecorder) {
				mockTask.FSM.SetState(resource.TaskStateFailed)
				mockPeer.FSM.SetState(resource.PeerStateSucceeded)

				gomock.InOrder(
					mr.TaskManager().Return(taskManager).Times(1),
					mt.LoadOrStore(gomock.Any()).Return(mockTask, true).Times(1),
					mr.HostManager().Return(hostManager).Times(1),
					mh.Load(gomock.Any()).Return(mockHost, true).Times(1),
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Any()).Return(mockPeer, true).Times(1),
				)
			},
			expect: func(t *testing.T, mockTask *resource.Task, mockPeer *resource.Peer, err error) {
				assert := assert.New(t)
				assert.NoError(err)
				assert.Equal(mockTask.FSM.Current(), resource.TaskStateSucceeded)
				assert.Equal(mockTask.TotalPieceCount.Load(), int32(1))
				assert.Equal(mockTask.ContentLength.Load(), int64(1000))
				piece, loaded := mockTask.LoadPiece(1)
				assert.True(loaded)

				assert.Equal(piece.Number, int32(1))
				assert.Equal(piece.Offset, uint64(0))
				assert.Equal(piece.Length, uint64(10))
				assert.EqualValues(piece.Digest, mockPieceMD5)
				assert.Equal(piece.TrafficType, commonv2.TrafficType_LOCAL_PEER)
				assert.Equal(piece.Cost, time.Duration(0))
				assert.NotEqual(piece.CreatedAt.Nanosecond(), 0)
				assert.Equal(mockPeer.FinishedPieces.Count(), uint(1))
				assert.Equal(mockPeer.PieceCosts()[0], time.Duration(0))
				assert.Equal(mockPeer.FSM.Current(), resource.PeerStateSucceeded)
			},
		},
		{
			name: "task state is TaskStatePending and peer state is PeerStatePending",
			req: &schedulerv1.AnnounceTaskRequest{
				TaskId: mockTaskID,
				Url:    mockTaskURL,
				UrlMeta: &commonv1.UrlMeta{
					Priority: commonv1.Priority_LEVEL0,
				},
				PeerHost: mockPeerHost,
				PiecePacket: &commonv1.PiecePacket{
					PieceInfos: []*commonv1.PieceInfo{{
						PieceNum:     1,
						RangeStart:   0,
						RangeSize:    10,
						PieceMd5:     mockPieceMD5.Encoded,
						DownloadCost: 1,
					}},
					TotalPiece:    1,
					ContentLength: 1000,
				},
			},
			mock: func(mockHost *resource.Host, mockTask *resource.Task, mockPeer *resource.Peer,
				hostManager resource.HostManager, taskManager resource.TaskManager, peerManager resource.PeerManager,
				mr *resource.MockResourceMockRecorder, mh *resource.MockHostManagerMockRecorder, mt *resource.MockTaskManagerMockRecorder, mp *resource.MockPeerManagerMockRecorder) {
				mockTask.FSM.SetState(resource.TaskStatePending)
				mockPeer.FSM.SetState(resource.PeerStatePending)

				gomock.InOrder(
					mr.TaskManager().Return(taskManager).Times(1),
					mt.LoadOrStore(gomock.Any()).Return(mockTask, true).Times(1),
					mr.HostManager().Return(hostManager).Times(1),
					mh.Load(gomock.Any()).Return(mockHost, true).Times(1),
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Any()).Return(mockPeer, true).Times(1),
				)
			},
			expect: func(t *testing.T, mockTask *resource.Task, mockPeer *resource.Peer, err error) {
				assert := assert.New(t)
				assert.NoError(err)
				assert.Equal(mockTask.FSM.Current(), resource.TaskStateSucceeded)
				assert.Equal(mockTask.TotalPieceCount.Load(), int32(1))
				assert.Equal(mockTask.ContentLength.Load(), int64(1000))
				piece, loaded := mockTask.LoadPiece(1)
				assert.True(loaded)

				assert.Equal(piece.Number, int32(1))
				assert.Equal(piece.Offset, uint64(0))
				assert.Equal(piece.Length, uint64(10))
				assert.EqualValues(piece.Digest, mockPieceMD5)
				assert.Equal(piece.TrafficType, commonv2.TrafficType_LOCAL_PEER)
				assert.Equal(piece.Cost, time.Duration(0))
				assert.NotEqual(piece.CreatedAt.Nanosecond(), 0)
				assert.Equal(mockPeer.FinishedPieces.Count(), uint(1))
				assert.Equal(mockPeer.PieceCosts()[0], time.Duration(0))
				assert.Equal(mockPeer.FSM.Current(), resource.PeerStateSucceeded)
			},
		},
		{
			name: "task state is TaskStatePending and peer state is PeerStateReceivedNormal",
			req: &schedulerv1.AnnounceTaskRequest{
				TaskId: mockTaskID,
				Url:    mockTaskURL,
				UrlMeta: &commonv1.UrlMeta{
					Priority: commonv1.Priority_LEVEL0,
				},
				PeerHost: mockPeerHost,
				PiecePacket: &commonv1.PiecePacket{
					PieceInfos: []*commonv1.PieceInfo{{
						PieceNum:     1,
						RangeStart:   0,
						RangeSize:    10,
						DownloadCost: 1,
					}},
					TotalPiece:    1,
					ContentLength: 1000,
				},
			},
			mock: func(mockHost *resource.Host, mockTask *resource.Task, mockPeer *resource.Peer,
				hostManager resource.HostManager, taskManager resource.TaskManager, peerManager resource.PeerManager,
				mr *resource.MockResourceMockRecorder, mh *resource.MockHostManagerMockRecorder, mt *resource.MockTaskManagerMockRecorder, mp *resource.MockPeerManagerMockRecorder) {
				mockTask.FSM.SetState(resource.TaskStatePending)
				mockPeer.FSM.SetState(resource.PeerStateReceivedNormal)

				gomock.InOrder(
					mr.TaskManager().Return(taskManager).Times(1),
					mt.LoadOrStore(gomock.Any()).Return(mockTask, true).Times(1),
					mr.HostManager().Return(hostManager).Times(1),
					mh.Load(gomock.Any()).Return(mockHost, true).Times(1),
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Any()).Return(mockPeer, true).Times(1),
				)
			},
			expect: func(t *testing.T, mockTask *resource.Task, mockPeer *resource.Peer, err error) {
				assert := assert.New(t)
				assert.NoError(err)
				assert.Equal(mockTask.FSM.Current(), resource.TaskStateSucceeded)
				assert.Equal(mockTask.TotalPieceCount.Load(), int32(1))
				assert.Equal(mockTask.ContentLength.Load(), int64(1000))
				piece, loaded := mockTask.LoadPiece(1)
				assert.True(loaded)

				assert.Equal(piece.Number, int32(1))
				assert.Equal(piece.Offset, uint64(0))
				assert.Equal(piece.Length, uint64(10))
				assert.Nil(piece.Digest)
				assert.Equal(piece.TrafficType, commonv2.TrafficType_LOCAL_PEER)
				assert.Equal(piece.Cost, time.Duration(0))
				assert.NotEqual(piece.CreatedAt.Nanosecond(), 0)
				assert.Equal(mockPeer.FinishedPieces.Count(), uint(1))
				assert.Equal(mockPeer.PieceCosts()[0], time.Duration(0))
				assert.Equal(mockPeer.FSM.Current(), resource.PeerStateSucceeded)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			scheduling := mocks.NewMockScheduling(ctl)
			res := resource.NewMockResource(ctl)
			dynconfig := configmocks.NewMockDynconfigInterface(ctl)
			hostManager := resource.NewMockHostManager(ctl)
			taskManager := resource.NewMockTaskManager(ctl)
			peerManager := resource.NewMockPeerManager(ctl)
			svc := NewV1(&config.Config{Scheduler: mockSchedulerConfig, Metrics: config.MetricsConfig{EnableHost: true}}, res, scheduling, dynconfig)
			mockHost := resource.NewHost(
				mockRawHost.ID, mockRawHost.IP, mockRawHost.Hostname,
				mockRawHost.Port, mockRawHost.DownloadPort, mockRawHost.Type)
			mockTask := resource.NewTask(mockTaskID, mockTaskURL, mockTaskTag, mockTaskApplication, commonv2.TaskType_STANDARD, mockTaskFilteredQueryParams, mockTaskHeader, mockTaskBackToSourceLimit, resource.WithDigest(mockTaskDigest), resource.WithPieceLength(mockTaskPieceLength))
			mockPeer := resource.NewPeer(mockPeerID, mockTask, mockHost)

			tc.mock(mockHost, mockTask, mockPeer, hostManager, taskManager, peerManager, res.EXPECT(), hostManager.EXPECT(), taskManager.EXPECT(), peerManager.EXPECT())
			tc.expect(t, mockTask, mockPeer, svc.AnnounceTask(context.Background(), tc.req))
		})
	}
}

func TestServiceV1_LeaveTask(t *testing.T) {
	tests := []struct {
		name   string
		mock   func(peer *resource.Peer, peerManager resource.PeerManager, ms *mocks.MockSchedulingMockRecorder, mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder)
		expect func(t *testing.T, peer *resource.Peer, err error)
	}{
		{
			name: "peer state is PeerStateLeave",
			mock: func(peer *resource.Peer, peerManager resource.PeerManager, ms *mocks.MockSchedulingMockRecorder, mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder) {
				peer.FSM.SetState(resource.PeerStateLeave)
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Any()).Return(peer, true).Times(1),
				)
			},
			expect: func(t *testing.T, peer *resource.Peer, err error) {
				assert := assert.New(t)
				dferr, ok := err.(*dferrors.DfError)
				assert.True(ok)
				assert.Equal(dferr.Code, commonv1.Code_SchedTaskStatusError)
				assert.True(peer.FSM.Is(resource.PeerStateLeave))
			},
		},
		{
			name: "peer not found",
			mock: func(peer *resource.Peer, peerManager resource.PeerManager, ms *mocks.MockSchedulingMockRecorder, mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder) {
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Any()).Return(nil, false).Times(1),
				)
			},
			expect: func(t *testing.T, peer *resource.Peer, err error) {
				assert := assert.New(t)
				dferr, ok := err.(*dferrors.DfError)
				assert.True(ok)
				assert.Equal(dferr.Code, commonv1.Code_SchedPeerNotFound)
			},
		},
		{
			name: "peer state is PeerStatePending",
			mock: func(peer *resource.Peer, peerManager resource.PeerManager, ms *mocks.MockSchedulingMockRecorder, mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder) {
				peer.FSM.SetState(resource.PeerStatePending)
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Any()).Return(peer, true).Times(1),
				)
			},
			expect: func(t *testing.T, peer *resource.Peer, err error) {
				assert := assert.New(t)
				assert.NoError(err)
				assert.True(peer.FSM.Is(resource.PeerStateLeave))
			},
		},
		{
			name: "peer state is PeerStateReceivedEmpty",
			mock: func(peer *resource.Peer, peerManager resource.PeerManager, ms *mocks.MockSchedulingMockRecorder, mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder) {
				peer.FSM.SetState(resource.PeerStateReceivedEmpty)
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Any()).Return(peer, true).Times(1),
				)
			},
			expect: func(t *testing.T, peer *resource.Peer, err error) {
				assert := assert.New(t)
				assert.NoError(err)
				assert.True(peer.FSM.Is(resource.PeerStateLeave))
			},
		},
		{
			name: "peer state is PeerStateReceivedTiny",
			mock: func(peer *resource.Peer, peerManager resource.PeerManager, ms *mocks.MockSchedulingMockRecorder, mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder) {
				peer.FSM.SetState(resource.PeerStateReceivedTiny)
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Any()).Return(peer, true).Times(1),
				)
			},
			expect: func(t *testing.T, peer *resource.Peer, err error) {
				assert := assert.New(t)
				assert.NoError(err)
				assert.True(peer.FSM.Is(resource.PeerStateLeave))
			},
		},
		{
			name: "peer state is PeerStateReceivedSmall",
			mock: func(peer *resource.Peer, peerManager resource.PeerManager, ms *mocks.MockSchedulingMockRecorder, mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder) {
				peer.FSM.SetState(resource.PeerStateReceivedSmall)
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Any()).Return(peer, true).Times(1),
				)
			},
			expect: func(t *testing.T, peer *resource.Peer, err error) {
				assert := assert.New(t)
				assert.NoError(err)
				assert.True(peer.FSM.Is(resource.PeerStateLeave))
			},
		},
		{
			name: "peer state is PeerStateReceivedNormal",
			mock: func(peer *resource.Peer, peerManager resource.PeerManager, ms *mocks.MockSchedulingMockRecorder, mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder) {
				peer.FSM.SetState(resource.PeerStateReceivedNormal)
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Any()).Return(peer, true).Times(1),
				)
			},
			expect: func(t *testing.T, peer *resource.Peer, err error) {
				assert := assert.New(t)
				assert.NoError(err)
				assert.Equal(len(peer.Children()), 0)
				assert.True(peer.FSM.Is(resource.PeerStateLeave))
			},
		},
		{
			name: "peer state is PeerStateRunning",
			mock: func(peer *resource.Peer, peerManager resource.PeerManager, ms *mocks.MockSchedulingMockRecorder, mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder) {
				peer.FSM.SetState(resource.PeerStateRunning)
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Any()).Return(peer, true).Times(1),
				)
			},
			expect: func(t *testing.T, peer *resource.Peer, err error) {
				assert := assert.New(t)
				assert.NoError(err)
				assert.True(peer.FSM.Is(resource.PeerStateLeave))
			},
		},
		{
			name: "peer state is PeerStateBackToSource",
			mock: func(peer *resource.Peer, peerManager resource.PeerManager, ms *mocks.MockSchedulingMockRecorder, mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder) {
				peer.FSM.SetState(resource.PeerStateBackToSource)
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Any()).Return(peer, true).Times(1),
				)
			},
			expect: func(t *testing.T, peer *resource.Peer, err error) {
				assert := assert.New(t)
				assert.NoError(err)
				assert.True(peer.FSM.Is(resource.PeerStateLeave))
			},
		},
		{
			name: "peer state is PeerStateFailed",
			mock: func(peer *resource.Peer, peerManager resource.PeerManager, ms *mocks.MockSchedulingMockRecorder, mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder) {
				peer.FSM.SetState(resource.PeerStateFailed)
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Any()).Return(peer, true).Times(1),
				)
			},
			expect: func(t *testing.T, peer *resource.Peer, err error) {
				assert := assert.New(t)
				assert.NoError(err)
				assert.True(peer.FSM.Is(resource.PeerStateLeave))
			},
		},
		{
			name: "peer state is PeerStateSucceeded",
			mock: func(peer *resource.Peer, peerManager resource.PeerManager, ms *mocks.MockSchedulingMockRecorder, mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder) {
				peer.FSM.SetState(resource.PeerStateSucceeded)
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Any()).Return(peer, true).Times(1),
				)
			},
			expect: func(t *testing.T, peer *resource.Peer, err error) {
				assert := assert.New(t)
				assert.NoError(err)
				assert.True(peer.FSM.Is(resource.PeerStateLeave))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			scheduling := mocks.NewMockScheduling(ctl)
			res := resource.NewMockResource(ctl)
			dynconfig := configmocks.NewMockDynconfigInterface(ctl)
			peerManager := resource.NewMockPeerManager(ctl)
			mockHost := resource.NewHost(
				mockRawHost.ID, mockRawHost.IP, mockRawHost.Hostname,
				mockRawHost.Port, mockRawHost.DownloadPort, mockRawHost.Type)
			mockTask := resource.NewTask(mockTaskID, mockTaskURL, mockTaskTag, mockTaskApplication, commonv2.TaskType_STANDARD, mockTaskFilteredQueryParams, mockTaskHeader, mockTaskBackToSourceLimit, resource.WithDigest(mockTaskDigest), resource.WithPieceLength(mockTaskPieceLength))
			peer := resource.NewPeer(mockSeedPeerID, mockTask, mockHost)
			svc := NewV1(&config.Config{Scheduler: mockSchedulerConfig, Metrics: config.MetricsConfig{EnableHost: true}}, res, scheduling, dynconfig)

			tc.mock(peer, peerManager, scheduling.EXPECT(), res.EXPECT(), peerManager.EXPECT())
			tc.expect(t, peer, svc.LeaveTask(context.Background(), &schedulerv1.PeerTarget{}))
		})
	}
}

func TestServiceV1_AnnounceHost(t *testing.T) {
	tests := []struct {
		name string
		req  *schedulerv1.AnnounceHostRequest
		run  func(t *testing.T, svc *V1, req *schedulerv1.AnnounceHostRequest, host *resource.Host, hostManager resource.HostManager, mr *resource.MockResourceMockRecorder, mh *resource.MockHostManagerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder)
	}{
		{
			name: "host not found",
			req: &schedulerv1.AnnounceHostRequest{
				Id:              mockHostID,
				Type:            pkgtypes.HostTypeNormal.Name(),
				Hostname:        "hostname",
				Ip:              "127.0.0.1",
				Port:            8003,
				DownloadPort:    8001,
				Os:              "darwin",
				Platform:        "darwin",
				PlatformFamily:  "Standalone Workstation",
				PlatformVersion: "11.1",
				KernelVersion:   "20.2.0",
				Cpu: &schedulerv1.CPU{
					LogicalCount:   mockCPU.LogicalCount,
					PhysicalCount:  mockCPU.PhysicalCount,
					Percent:        mockCPU.Percent,
					ProcessPercent: mockCPU.ProcessPercent,
					Times: &schedulerv1.CPUTimes{
						User:      mockCPU.Times.User,
						System:    mockCPU.Times.System,
						Idle:      mockCPU.Times.Idle,
						Nice:      mockCPU.Times.Nice,
						Iowait:    mockCPU.Times.Iowait,
						Irq:       mockCPU.Times.Irq,
						Softirq:   mockCPU.Times.Softirq,
						Steal:     mockCPU.Times.Steal,
						Guest:     mockCPU.Times.Guest,
						GuestNice: mockCPU.Times.GuestNice,
					},
				},
				Memory: &schedulerv1.Memory{
					Total:              mockMemory.Total,
					Available:          mockMemory.Available,
					Used:               mockMemory.Used,
					UsedPercent:        mockMemory.UsedPercent,
					ProcessUsedPercent: mockMemory.ProcessUsedPercent,
					Free:               mockMemory.Free,
				},
				Network: &schedulerv1.Network{
					TcpConnectionCount:       mockNetwork.TCPConnectionCount,
					UploadTcpConnectionCount: mockNetwork.UploadTCPConnectionCount,
					Location:                 mockNetwork.Location,
					Idc:                      mockNetwork.IDC,
				},
				Disk: &schedulerv1.Disk{
					Total:             mockDisk.Total,
					Free:              mockDisk.Free,
					Used:              mockDisk.Used,
					UsedPercent:       mockDisk.UsedPercent,
					InodesTotal:       mockDisk.InodesTotal,
					InodesUsed:        mockDisk.InodesUsed,
					InodesFree:        mockDisk.InodesFree,
					InodesUsedPercent: mockDisk.InodesUsedPercent,
				},
				Build: &schedulerv1.Build{
					GitVersion: mockBuild.GitVersion,
					GitCommit:  mockBuild.GitCommit,
					GoVersion:  mockBuild.GoVersion,
					Platform:   mockBuild.Platform,
				},
			},
			run: func(t *testing.T, svc *V1, req *schedulerv1.AnnounceHostRequest, host *resource.Host, hostManager resource.HostManager, mr *resource.MockResourceMockRecorder, mh *resource.MockHostManagerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				gomock.InOrder(
					md.GetSchedulerClusterClientConfig().Return(types.SchedulerClusterClientConfig{LoadLimit: 10}, nil).Times(1),
					mr.HostManager().Return(hostManager).Times(1),
					mh.Load(gomock.Any()).Return(nil, false).Times(1),
					mr.HostManager().Return(hostManager).Times(1),
					mh.Store(gomock.Any()).Do(func(host *resource.Host) {
						assert := assert.New(t)
						assert.Equal(host.ID, req.Id)
						assert.Equal(host.Type, pkgtypes.ParseHostType(req.Type))
						assert.Equal(host.Hostname, req.Hostname)
						assert.Equal(host.IP, req.Ip)
						assert.Equal(host.Port, req.Port)
						assert.Equal(host.DownloadPort, req.DownloadPort)
						assert.Equal(host.OS, req.Os)
						assert.Equal(host.Platform, req.Platform)
						assert.Equal(host.PlatformVersion, req.PlatformVersion)
						assert.Equal(host.KernelVersion, req.KernelVersion)
						assert.EqualValues(host.CPU, mockCPU)
						assert.EqualValues(host.Memory, mockMemory)
						assert.EqualValues(host.Network, mockNetwork)
						assert.EqualValues(host.Disk, mockDisk)
						assert.EqualValues(host.Build, mockBuild)
						assert.Equal(host.ConcurrentUploadLimit.Load(), int32(10))
						assert.Equal(host.ConcurrentUploadCount.Load(), int32(0))
						assert.Equal(host.UploadCount.Load(), int64(0))
						assert.Equal(host.UploadFailedCount.Load(), int64(0))
						assert.NotNil(host.Peers)
						assert.Equal(host.PeerCount.Load(), int32(0))
						assert.NotEqual(host.CreatedAt.Load().Nanosecond(), 0)
						assert.NotEqual(host.UpdatedAt.Load().Nanosecond(), 0)
						assert.NotNil(host.Log)
					}).Return().Times(1),
				)

				assert := assert.New(t)
				assert.NoError(svc.AnnounceHost(context.Background(), req))
			},
		},
		{
			name: "host not found and dynconfig returns error",
			req: &schedulerv1.AnnounceHostRequest{
				Id:              mockHostID,
				Type:            pkgtypes.HostTypeNormal.Name(),
				Hostname:        "hostname",
				Ip:              "127.0.0.1",
				Port:            8003,
				DownloadPort:    8001,
				Os:              "darwin",
				Platform:        "darwin",
				PlatformFamily:  "Standalone Workstation",
				PlatformVersion: "11.1",
				KernelVersion:   "20.2.0",
				Cpu: &schedulerv1.CPU{
					LogicalCount:   mockCPU.LogicalCount,
					PhysicalCount:  mockCPU.PhysicalCount,
					Percent:        mockCPU.Percent,
					ProcessPercent: mockCPU.ProcessPercent,
					Times: &schedulerv1.CPUTimes{
						User:      mockCPU.Times.User,
						System:    mockCPU.Times.System,
						Idle:      mockCPU.Times.Idle,
						Nice:      mockCPU.Times.Nice,
						Iowait:    mockCPU.Times.Iowait,
						Irq:       mockCPU.Times.Irq,
						Softirq:   mockCPU.Times.Softirq,
						Steal:     mockCPU.Times.Steal,
						Guest:     mockCPU.Times.Guest,
						GuestNice: mockCPU.Times.GuestNice,
					},
				},
				Memory: &schedulerv1.Memory{
					Total:              mockMemory.Total,
					Available:          mockMemory.Available,
					Used:               mockMemory.Used,
					UsedPercent:        mockMemory.UsedPercent,
					ProcessUsedPercent: mockMemory.ProcessUsedPercent,
					Free:               mockMemory.Free,
				},
				Network: &schedulerv1.Network{
					TcpConnectionCount:       mockNetwork.TCPConnectionCount,
					UploadTcpConnectionCount: mockNetwork.UploadTCPConnectionCount,
					Location:                 mockNetwork.Location,
					Idc:                      mockNetwork.IDC,
				},
				Disk: &schedulerv1.Disk{
					Total:             mockDisk.Total,
					Free:              mockDisk.Free,
					Used:              mockDisk.Used,
					UsedPercent:       mockDisk.UsedPercent,
					InodesTotal:       mockDisk.InodesTotal,
					InodesUsed:        mockDisk.InodesUsed,
					InodesFree:        mockDisk.InodesFree,
					InodesUsedPercent: mockDisk.InodesUsedPercent,
				},
				Build: &schedulerv1.Build{
					GitVersion: mockBuild.GitVersion,
					GitCommit:  mockBuild.GitCommit,
					GoVersion:  mockBuild.GoVersion,
					Platform:   mockBuild.Platform,
				},
			},
			run: func(t *testing.T, svc *V1, req *schedulerv1.AnnounceHostRequest, host *resource.Host, hostManager resource.HostManager, mr *resource.MockResourceMockRecorder, mh *resource.MockHostManagerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				gomock.InOrder(
					md.GetSchedulerClusterClientConfig().Return(types.SchedulerClusterClientConfig{}, errors.New("foo")).Times(1),
					mr.HostManager().Return(hostManager).Times(1),
					mh.Load(gomock.Any()).Return(nil, false).Times(1),
					mr.HostManager().Return(hostManager).Times(1),
					mh.Store(gomock.Any()).Do(func(host *resource.Host) {
						assert := assert.New(t)
						assert.Equal(host.ID, req.Id)
						assert.Equal(host.Type, pkgtypes.ParseHostType(req.Type))
						assert.Equal(host.Hostname, req.Hostname)
						assert.Equal(host.IP, req.Ip)
						assert.Equal(host.Port, req.Port)
						assert.Equal(host.DownloadPort, req.DownloadPort)
						assert.Equal(host.OS, req.Os)
						assert.Equal(host.Platform, req.Platform)
						assert.Equal(host.PlatformVersion, req.PlatformVersion)
						assert.Equal(host.KernelVersion, req.KernelVersion)
						assert.EqualValues(host.CPU, mockCPU)
						assert.EqualValues(host.Memory, mockMemory)
						assert.EqualValues(host.Network, mockNetwork)
						assert.EqualValues(host.Disk, mockDisk)
						assert.EqualValues(host.Build, mockBuild)
						assert.Equal(host.ConcurrentUploadLimit.Load(), int32(200))
						assert.Equal(host.ConcurrentUploadCount.Load(), int32(0))
						assert.Equal(host.UploadCount.Load(), int64(0))
						assert.Equal(host.UploadFailedCount.Load(), int64(0))
						assert.NotNil(host.Peers)
						assert.Equal(host.PeerCount.Load(), int32(0))
						assert.NotEqual(host.CreatedAt.Load().Nanosecond(), 0)
						assert.NotEqual(host.UpdatedAt.Load().Nanosecond(), 0)
						assert.NotNil(host.Log)
					}).Return().Times(1),
				)

				assert := assert.New(t)
				assert.NoError(svc.AnnounceHost(context.Background(), req))
			},
		},
		{
			name: "host already exists",
			req: &schedulerv1.AnnounceHostRequest{
				Id:              mockHostID,
				Type:            pkgtypes.HostTypeNormal.Name(),
				Hostname:        "foo",
				Ip:              "127.0.0.1",
				Port:            8003,
				DownloadPort:    8001,
				Os:              "darwin",
				Platform:        "darwin",
				PlatformFamily:  "Standalone Workstation",
				PlatformVersion: "11.1",
				KernelVersion:   "20.2.0",
				Cpu: &schedulerv1.CPU{
					LogicalCount:   mockCPU.LogicalCount,
					PhysicalCount:  mockCPU.PhysicalCount,
					Percent:        mockCPU.Percent,
					ProcessPercent: mockCPU.ProcessPercent,
					Times: &schedulerv1.CPUTimes{
						User:      mockCPU.Times.User,
						System:    mockCPU.Times.System,
						Idle:      mockCPU.Times.Idle,
						Nice:      mockCPU.Times.Nice,
						Iowait:    mockCPU.Times.Iowait,
						Irq:       mockCPU.Times.Irq,
						Softirq:   mockCPU.Times.Softirq,
						Steal:     mockCPU.Times.Steal,
						Guest:     mockCPU.Times.Guest,
						GuestNice: mockCPU.Times.GuestNice,
					},
				},
				Memory: &schedulerv1.Memory{
					Total:              mockMemory.Total,
					Available:          mockMemory.Available,
					Used:               mockMemory.Used,
					UsedPercent:        mockMemory.UsedPercent,
					ProcessUsedPercent: mockMemory.ProcessUsedPercent,
					Free:               mockMemory.Free,
				},
				Network: &schedulerv1.Network{
					TcpConnectionCount:       mockNetwork.TCPConnectionCount,
					UploadTcpConnectionCount: mockNetwork.UploadTCPConnectionCount,
					Location:                 mockNetwork.Location,
					Idc:                      mockNetwork.IDC,
				},
				Disk: &schedulerv1.Disk{
					Total:             mockDisk.Total,
					Free:              mockDisk.Free,
					Used:              mockDisk.Used,
					UsedPercent:       mockDisk.UsedPercent,
					InodesTotal:       mockDisk.InodesTotal,
					InodesUsed:        mockDisk.InodesUsed,
					InodesFree:        mockDisk.InodesFree,
					InodesUsedPercent: mockDisk.InodesUsedPercent,
				},
				Build: &schedulerv1.Build{
					GitVersion: mockBuild.GitVersion,
					GitCommit:  mockBuild.GitCommit,
					GoVersion:  mockBuild.GoVersion,
					Platform:   mockBuild.Platform,
				},
			},
			run: func(t *testing.T, svc *V1, req *schedulerv1.AnnounceHostRequest, host *resource.Host, hostManager resource.HostManager, mr *resource.MockResourceMockRecorder, mh *resource.MockHostManagerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				gomock.InOrder(
					md.GetSchedulerClusterClientConfig().Return(types.SchedulerClusterClientConfig{LoadLimit: 10}, nil).Times(1),
					mr.HostManager().Return(hostManager).Times(1),
					mh.Load(gomock.Any()).Return(host, true).Times(1),
				)

				assert := assert.New(t)
				assert.NoError(svc.AnnounceHost(context.Background(), req))
				assert.Equal(host.ID, req.Id)
				assert.Equal(host.Type, pkgtypes.ParseHostType(req.Type))
				assert.Equal(host.Hostname, req.Hostname)
				assert.Equal(host.IP, req.Ip)
				assert.Equal(host.Port, req.Port)
				assert.Equal(host.DownloadPort, req.DownloadPort)
				assert.Equal(host.OS, req.Os)
				assert.Equal(host.Platform, req.Platform)
				assert.Equal(host.PlatformVersion, req.PlatformVersion)
				assert.Equal(host.KernelVersion, req.KernelVersion)
				assert.EqualValues(host.CPU, mockCPU)
				assert.EqualValues(host.Memory, mockMemory)
				assert.EqualValues(host.Network, mockNetwork)
				assert.EqualValues(host.Disk, mockDisk)
				assert.EqualValues(host.Build, mockBuild)
				assert.Equal(host.ConcurrentUploadLimit.Load(), int32(10))
				assert.Equal(host.ConcurrentUploadCount.Load(), int32(0))
				assert.Equal(host.UploadCount.Load(), int64(0))
				assert.Equal(host.UploadFailedCount.Load(), int64(0))
				assert.NotNil(host.Peers)
				assert.Equal(host.PeerCount.Load(), int32(0))
				assert.NotEqual(host.CreatedAt.Load().Nanosecond(), 0)
				assert.NotEqual(host.UpdatedAt.Load().Nanosecond(), 0)
				assert.NotNil(host.Log)
			},
		},
		{
			name: "host already exists and dynconfig returns error",
			req: &schedulerv1.AnnounceHostRequest{
				Id:              mockHostID,
				Type:            pkgtypes.HostTypeNormal.Name(),
				Hostname:        "foo",
				Ip:              "127.0.0.1",
				Port:            8003,
				DownloadPort:    8001,
				Os:              "darwin",
				Platform:        "darwin",
				PlatformFamily:  "Standalone Workstation",
				PlatformVersion: "11.1",
				KernelVersion:   "20.2.0",
				Cpu: &schedulerv1.CPU{
					LogicalCount:   mockCPU.LogicalCount,
					PhysicalCount:  mockCPU.PhysicalCount,
					Percent:        mockCPU.Percent,
					ProcessPercent: mockCPU.ProcessPercent,
					Times: &schedulerv1.CPUTimes{
						User:      mockCPU.Times.User,
						System:    mockCPU.Times.System,
						Idle:      mockCPU.Times.Idle,
						Nice:      mockCPU.Times.Nice,
						Iowait:    mockCPU.Times.Iowait,
						Irq:       mockCPU.Times.Irq,
						Softirq:   mockCPU.Times.Softirq,
						Steal:     mockCPU.Times.Steal,
						Guest:     mockCPU.Times.Guest,
						GuestNice: mockCPU.Times.GuestNice,
					},
				},
				Memory: &schedulerv1.Memory{
					Total:              mockMemory.Total,
					Available:          mockMemory.Available,
					Used:               mockMemory.Used,
					UsedPercent:        mockMemory.UsedPercent,
					ProcessUsedPercent: mockMemory.ProcessUsedPercent,
					Free:               mockMemory.Free,
				},
				Network: &schedulerv1.Network{
					TcpConnectionCount:       mockNetwork.TCPConnectionCount,
					UploadTcpConnectionCount: mockNetwork.UploadTCPConnectionCount,
					Location:                 mockNetwork.Location,
					Idc:                      mockNetwork.IDC,
				},
				Disk: &schedulerv1.Disk{
					Total:             mockDisk.Total,
					Free:              mockDisk.Free,
					Used:              mockDisk.Used,
					UsedPercent:       mockDisk.UsedPercent,
					InodesTotal:       mockDisk.InodesTotal,
					InodesUsed:        mockDisk.InodesUsed,
					InodesFree:        mockDisk.InodesFree,
					InodesUsedPercent: mockDisk.InodesUsedPercent,
				},
				Build: &schedulerv1.Build{
					GitVersion: mockBuild.GitVersion,
					GitCommit:  mockBuild.GitCommit,
					GoVersion:  mockBuild.GoVersion,
					Platform:   mockBuild.Platform,
				},
			},
			run: func(t *testing.T, svc *V1, req *schedulerv1.AnnounceHostRequest, host *resource.Host, hostManager resource.HostManager, mr *resource.MockResourceMockRecorder, mh *resource.MockHostManagerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				gomock.InOrder(
					md.GetSchedulerClusterClientConfig().Return(types.SchedulerClusterClientConfig{}, errors.New("foo")).Times(1),
					mr.HostManager().Return(hostManager).Times(1),
					mh.Load(gomock.Any()).Return(host, true).Times(1),
				)

				assert := assert.New(t)
				assert.NoError(svc.AnnounceHost(context.Background(), req))
				assert.Equal(host.ID, req.Id)
				assert.Equal(host.Type, pkgtypes.ParseHostType(req.Type))
				assert.Equal(host.Hostname, req.Hostname)
				assert.Equal(host.IP, req.Ip)
				assert.Equal(host.Port, req.Port)
				assert.Equal(host.DownloadPort, req.DownloadPort)
				assert.Equal(host.OS, req.Os)
				assert.Equal(host.Platform, req.Platform)
				assert.Equal(host.PlatformVersion, req.PlatformVersion)
				assert.Equal(host.KernelVersion, req.KernelVersion)
				assert.EqualValues(host.CPU, mockCPU)
				assert.EqualValues(host.Memory, mockMemory)
				assert.EqualValues(host.Network, mockNetwork)
				assert.EqualValues(host.Disk, mockDisk)
				assert.EqualValues(host.Build, mockBuild)
				assert.Equal(host.ConcurrentUploadLimit.Load(), int32(200))
				assert.Equal(host.ConcurrentUploadCount.Load(), int32(0))
				assert.Equal(host.UploadCount.Load(), int64(0))
				assert.Equal(host.UploadFailedCount.Load(), int64(0))
				assert.NotNil(host.Peers)
				assert.Equal(host.PeerCount.Load(), int32(0))
				assert.NotEqual(host.CreatedAt.Load().Nanosecond(), 0)
				assert.NotEqual(host.UpdatedAt.Load().Nanosecond(), 0)
				assert.NotNil(host.Log)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			scheduling := mocks.NewMockScheduling(ctl)
			res := resource.NewMockResource(ctl)
			dynconfig := configmocks.NewMockDynconfigInterface(ctl)
			hostManager := resource.NewMockHostManager(ctl)
			host := resource.NewHost(
				mockRawHost.ID, mockRawHost.IP, mockRawHost.Hostname,
				mockRawHost.Port, mockRawHost.DownloadPort, mockRawHost.Type)
			svc := NewV1(&config.Config{Scheduler: mockSchedulerConfig, Metrics: config.MetricsConfig{EnableHost: true}}, res, scheduling, dynconfig)

			tc.run(t, svc, tc.req, host, hostManager, res.EXPECT(), hostManager.EXPECT(), dynconfig.EXPECT())
		})
	}
}

func TestServiceV1_LeaveHost(t *testing.T) {
	tests := []struct {
		name   string
		mock   func(host *resource.Host, mockPeer *resource.Peer, hostManager resource.HostManager, ms *mocks.MockSchedulingMockRecorder, mr *resource.MockResourceMockRecorder, mh *resource.MockHostManagerMockRecorder)
		expect func(t *testing.T, peer *resource.Peer, err error)
	}{
		{
			name: "host not found",
			mock: func(host *resource.Host, mockPeer *resource.Peer, hostManager resource.HostManager, ms *mocks.MockSchedulingMockRecorder, mr *resource.MockResourceMockRecorder, mh *resource.MockHostManagerMockRecorder) {
				gomock.InOrder(
					mr.HostManager().Return(hostManager).Times(1),
					mh.Load(gomock.Any()).Return(nil, false).Times(1),
				)
			},
			expect: func(t *testing.T, peer *resource.Peer, err error) {
				assert := assert.New(t)
				assert.Error(err)
			},
		},
		{
			name: "host has not peers",
			mock: func(host *resource.Host, mockPeer *resource.Peer, hostManager resource.HostManager, ms *mocks.MockSchedulingMockRecorder, mr *resource.MockResourceMockRecorder, mh *resource.MockHostManagerMockRecorder) {
				gomock.InOrder(
					mr.HostManager().Return(hostManager).Times(1),
					mh.Load(gomock.Any()).Return(host, true).Times(1),
					mr.HostManager().Return(hostManager).Times(1),
					mh.Delete(gomock.Any()).Return().Times(1),
				)
			},
			expect: func(t *testing.T, peer *resource.Peer, err error) {
				assert := assert.New(t)
				assert.NoError(err)
			},
		},
		{
			name: "peer state is PeerStateLeave",
			mock: func(host *resource.Host, mockPeer *resource.Peer, hostManager resource.HostManager, ms *mocks.MockSchedulingMockRecorder, mr *resource.MockResourceMockRecorder, mh *resource.MockHostManagerMockRecorder) {
				host.Peers.Store(mockPeer.ID, mockPeer)
				mockPeer.FSM.SetState(resource.PeerStateLeave)
				gomock.InOrder(
					mr.HostManager().Return(hostManager).Times(1),
					mh.Load(gomock.Any()).Return(host, true).Times(1),
					mr.HostManager().Return(hostManager).Times(1),
					mh.Delete(gomock.Any()).Return().Times(1),
				)
			},
			expect: func(t *testing.T, peer *resource.Peer, err error) {
				assert := assert.New(t)
				assert.NoError(err)
			},
		},
		{
			name: "peer state is PeerStatePending",
			mock: func(host *resource.Host, mockPeer *resource.Peer, hostManager resource.HostManager, ms *mocks.MockSchedulingMockRecorder, mr *resource.MockResourceMockRecorder, mh *resource.MockHostManagerMockRecorder) {
				host.Peers.Store(mockPeer.ID, mockPeer)
				mockPeer.FSM.SetState(resource.PeerStatePending)
				gomock.InOrder(
					mr.HostManager().Return(hostManager).Times(1),
					mh.Load(gomock.Any()).Return(host, true).Times(1),
					mr.HostManager().Return(hostManager).Times(1),
					mh.Delete(gomock.Any()).Return().Times(1),
				)
			},
			expect: func(t *testing.T, peer *resource.Peer, err error) {
				assert := assert.New(t)
				assert.NoError(err)
				assert.Equal(peer.FSM.Current(), resource.PeerStateLeave)
			},
		},
		{
			name: "peer state is PeerStateReceivedEmpty",
			mock: func(host *resource.Host, mockPeer *resource.Peer, hostManager resource.HostManager, ms *mocks.MockSchedulingMockRecorder, mr *resource.MockResourceMockRecorder, mh *resource.MockHostManagerMockRecorder) {
				host.Peers.Store(mockPeer.ID, mockPeer)
				mockPeer.FSM.SetState(resource.PeerStateReceivedEmpty)
				gomock.InOrder(
					mr.HostManager().Return(hostManager).Times(1),
					mh.Load(gomock.Any()).Return(host, true).Times(1),
					mr.HostManager().Return(hostManager).Times(1),
					mh.Delete(gomock.Any()).Return().Times(1),
				)
			},
			expect: func(t *testing.T, peer *resource.Peer, err error) {
				assert := assert.New(t)
				assert.NoError(err)
				assert.Equal(peer.FSM.Current(), resource.PeerStateLeave)
			},
		},
		{
			name: "peer state is PeerStateReceivedTiny",
			mock: func(host *resource.Host, mockPeer *resource.Peer, hostManager resource.HostManager, ms *mocks.MockSchedulingMockRecorder, mr *resource.MockResourceMockRecorder, mh *resource.MockHostManagerMockRecorder) {
				host.Peers.Store(mockPeer.ID, mockPeer)
				mockPeer.FSM.SetState(resource.PeerStateReceivedTiny)
				gomock.InOrder(
					mr.HostManager().Return(hostManager).Times(1),
					mh.Load(gomock.Any()).Return(host, true).Times(1),
					mr.HostManager().Return(hostManager).Times(1),
					mh.Delete(gomock.Any()).Return().Times(1),
				)
			},
			expect: func(t *testing.T, peer *resource.Peer, err error) {
				assert := assert.New(t)
				assert.NoError(err)
				assert.Equal(peer.FSM.Current(), resource.PeerStateLeave)
			},
		},
		{
			name: "peer state is PeerStateReceivedSmall",
			mock: func(host *resource.Host, mockPeer *resource.Peer, hostManager resource.HostManager, ms *mocks.MockSchedulingMockRecorder, mr *resource.MockResourceMockRecorder, mh *resource.MockHostManagerMockRecorder) {
				host.Peers.Store(mockPeer.ID, mockPeer)
				mockPeer.FSM.SetState(resource.PeerStateReceivedSmall)
				gomock.InOrder(
					mr.HostManager().Return(hostManager).Times(1),
					mh.Load(gomock.Any()).Return(host, true).Times(1),
					mr.HostManager().Return(hostManager).Times(1),
					mh.Delete(gomock.Any()).Return().Times(1),
				)
			},
			expect: func(t *testing.T, peer *resource.Peer, err error) {
				assert := assert.New(t)
				assert.NoError(err)
				assert.Equal(peer.FSM.Current(), resource.PeerStateLeave)
			},
		},
		{
			name: "peer state is PeerStateReceivedNormal",
			mock: func(host *resource.Host, mockPeer *resource.Peer, hostManager resource.HostManager, ms *mocks.MockSchedulingMockRecorder, mr *resource.MockResourceMockRecorder, mh *resource.MockHostManagerMockRecorder) {
				host.Peers.Store(mockPeer.ID, mockPeer)
				mockPeer.FSM.SetState(resource.PeerStateReceivedNormal)
				gomock.InOrder(
					mr.HostManager().Return(hostManager).Times(1),
					mh.Load(gomock.Any()).Return(host, true).Times(1),
					mr.HostManager().Return(hostManager).Times(1),
					mh.Delete(gomock.Any()).Return().Times(1),
				)
			},
			expect: func(t *testing.T, peer *resource.Peer, err error) {
				assert := assert.New(t)
				assert.NoError(err)
				assert.Equal(peer.FSM.Current(), resource.PeerStateLeave)
			},
		},
		{
			name: "peer state is PeerStateRunning",
			mock: func(host *resource.Host, mockPeer *resource.Peer, hostManager resource.HostManager, ms *mocks.MockSchedulingMockRecorder, mr *resource.MockResourceMockRecorder, mh *resource.MockHostManagerMockRecorder) {
				host.Peers.Store(mockPeer.ID, mockPeer)
				mockPeer.FSM.SetState(resource.PeerStateRunning)
				gomock.InOrder(
					mr.HostManager().Return(hostManager).Times(1),
					mh.Load(gomock.Any()).Return(host, true).Times(1),
					mr.HostManager().Return(hostManager).Times(1),
					mh.Delete(gomock.Any()).Return().Times(1),
				)
			},
			expect: func(t *testing.T, peer *resource.Peer, err error) {
				assert := assert.New(t)
				assert.NoError(err)
				assert.Equal(peer.FSM.Current(), resource.PeerStateLeave)
			},
		},
		{
			name: "peer state is PeerStateBackToSource",
			mock: func(host *resource.Host, mockPeer *resource.Peer, hostManager resource.HostManager, ms *mocks.MockSchedulingMockRecorder, mr *resource.MockResourceMockRecorder, mh *resource.MockHostManagerMockRecorder) {
				host.Peers.Store(mockPeer.ID, mockPeer)
				mockPeer.FSM.SetState(resource.PeerStateBackToSource)
				gomock.InOrder(
					mr.HostManager().Return(hostManager).Times(1),
					mh.Load(gomock.Any()).Return(host, true).Times(1),
					mr.HostManager().Return(hostManager).Times(1),
					mh.Delete(gomock.Any()).Return().Times(1),
				)
			},
			expect: func(t *testing.T, peer *resource.Peer, err error) {
				assert := assert.New(t)
				assert.NoError(err)
				assert.Equal(peer.FSM.Current(), resource.PeerStateLeave)
			},
		},
		{
			name: "peer state is PeerStateSucceeded",
			mock: func(host *resource.Host, mockPeer *resource.Peer, hostManager resource.HostManager, ms *mocks.MockSchedulingMockRecorder, mr *resource.MockResourceMockRecorder, mh *resource.MockHostManagerMockRecorder) {
				host.Peers.Store(mockPeer.ID, mockPeer)
				mockPeer.FSM.SetState(resource.PeerStateSucceeded)
				gomock.InOrder(
					mr.HostManager().Return(hostManager).Times(1),
					mh.Load(gomock.Any()).Return(host, true).Times(1),
					mr.HostManager().Return(hostManager).Times(1),
					mh.Delete(gomock.Any()).Return().Times(1),
				)
			},
			expect: func(t *testing.T, peer *resource.Peer, err error) {
				assert := assert.New(t)
				assert.NoError(err)
				assert.Equal(peer.FSM.Current(), resource.PeerStateLeave)
			},
		},
		{
			name: "peer state is PeerStateFailed",
			mock: func(host *resource.Host, mockPeer *resource.Peer, hostManager resource.HostManager, ms *mocks.MockSchedulingMockRecorder, mr *resource.MockResourceMockRecorder, mh *resource.MockHostManagerMockRecorder) {
				host.Peers.Store(mockPeer.ID, mockPeer)
				mockPeer.FSM.SetState(resource.PeerStateFailed)
				gomock.InOrder(
					mr.HostManager().Return(hostManager).Times(1),
					mh.Load(gomock.Any()).Return(host, true).Times(1),
					mr.HostManager().Return(hostManager).Times(1),
					mh.Delete(gomock.Any()).Return().Times(1),
				)
			},
			expect: func(t *testing.T, peer *resource.Peer, err error) {
				assert := assert.New(t)
				assert.NoError(err)
				assert.Equal(peer.FSM.Current(), resource.PeerStateLeave)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			scheduling := mocks.NewMockScheduling(ctl)
			res := resource.NewMockResource(ctl)
			dynconfig := configmocks.NewMockDynconfigInterface(ctl)
			hostManager := resource.NewMockHostManager(ctl)
			host := resource.NewHost(
				mockRawHost.ID, mockRawHost.IP, mockRawHost.Hostname,
				mockRawHost.Port, mockRawHost.DownloadPort, mockRawHost.Type)
			mockTask := resource.NewTask(mockTaskID, mockTaskURL, mockTaskTag, mockTaskApplication, commonv2.TaskType_STANDARD, mockTaskFilteredQueryParams, mockTaskHeader, mockTaskBackToSourceLimit, resource.WithDigest(mockTaskDigest), resource.WithPieceLength(mockTaskPieceLength))
			mockPeer := resource.NewPeer(mockSeedPeerID, mockTask, host)
			svc := NewV1(&config.Config{Scheduler: mockSchedulerConfig, Metrics: config.MetricsConfig{EnableHost: true}}, res, scheduling, dynconfig)

			tc.mock(host, mockPeer, hostManager, scheduling.EXPECT(), res.EXPECT(), hostManager.EXPECT())
			tc.expect(t, mockPeer, svc.LeaveHost(context.Background(), &schedulerv1.LeaveHostRequest{
				Id: idgen.HostIDV2(host.IP, host.Hostname, true),
			}))
		})
	}
}

func TestServiceV1_prefetchTask(t *testing.T) {
	fmt.Println("TestServiceV1_prefetchTask")
	tests := []struct {
		name   string
		config *config.Config
		req    *schedulerv1.PeerTaskRequest
		mock   func(task *resource.Task, peer *resource.Peer, taskManager resource.TaskManager, seedPeer resource.SeedPeer, mr *resource.MockResourceMockRecorder, mt *resource.MockTaskManagerMockRecorder, mc *resource.MockSeedPeerMockRecorder)
		expect func(t *testing.T, task *resource.Task, err error)
	}{
		{
			name: "prefetch task with seed peer",
			config: &config.Config{
				Scheduler: mockSchedulerConfig,
				SeedPeer:  mockSeedPeerConfig,
			},
			req: &schedulerv1.PeerTaskRequest{
				Url: mockTaskURL,
				UrlMeta: &commonv1.UrlMeta{
					Digest:      mockTaskDigest.String(),
					Tag:         mockTaskTag,
					Range:       mockURLMetaRange,
					Filter:      strings.Join(mockTaskFilteredQueryParams, idgen.FilteredQueryParamsSeparator),
					Header:      mockTaskHeader,
					Application: mockTaskApplication,
					Priority:    commonv1.Priority_LEVEL0,
				},
				PeerId:      mockPeerID,
				PeerHost:    mockPeerHost,
				Prefetch:    true,
				IsMigrating: false,
				TaskId:      mockTaskID,
			},
			mock: func(task *resource.Task, peer *resource.Peer, taskManager resource.TaskManager, seedPeer resource.SeedPeer, mr *resource.MockResourceMockRecorder, mt *resource.MockTaskManagerMockRecorder, mc *resource.MockSeedPeerMockRecorder) {
				task.FSM.SetState(resource.TaskStateRunning)
				peer.FSM.SetState(resource.PeerStateRunning)
				gomock.InOrder(
					mr.TaskManager().Return(taskManager).Times(1),
					mt.Load(gomock.Eq("7aecbd0437cf6b429dc623686d36208135b3d2d1831a90b644458964297943a4")).Return(task, true).Times(1),
					mr.SeedPeer().Return(seedPeer).Times(1),
					mc.TriggerTask(gomock.Any(), gomock.Any(), gomock.Any()).Return(peer, &schedulerv1.PeerResult{}, nil).Times(1),
				)
			},
			expect: func(t *testing.T, task *resource.Task, err error) {
				assert := assert.New(t)
				assert.True(task.FSM.Is(resource.TaskStateSucceeded))
				assert.Equal(task.Header, map[string]string{"Content-Length": "100"})
			},
		},
		{
			name: "prefetch task without seed peer",
			config: &config.Config{
				Scheduler: mockSchedulerConfig,
			},
			req: &schedulerv1.PeerTaskRequest{
				Url: mockTaskURL,
				UrlMeta: &commonv1.UrlMeta{
					Digest:      mockTaskDigest.String(),
					Tag:         mockTaskTag,
					Range:       mockURLMetaRange,
					Filter:      strings.Join(mockTaskFilteredQueryParams, idgen.FilteredQueryParamsSeparator),
					Header:      mockTaskHeader,
					Application: mockTaskApplication,
					Priority:    commonv1.Priority_LEVEL0,
				},
				PeerId:      mockPeerID,
				PeerHost:    mockPeerHost,
				Prefetch:    true,
				IsMigrating: false,
				TaskId:      mockTaskID,
			},
			mock: func(task *resource.Task, peer *resource.Peer, taskManager resource.TaskManager, seedPeer resource.SeedPeer, mr *resource.MockResourceMockRecorder, mt *resource.MockTaskManagerMockRecorder, mc *resource.MockSeedPeerMockRecorder) {
				task.FSM.SetState(resource.TaskStateRunning)
				peer.FSM.SetState(resource.PeerStateRunning)
			},
			expect: func(t *testing.T, task *resource.Task, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "seed peer is disabled")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			scheduling := mocks.NewMockScheduling(ctl)
			res := resource.NewMockResource(ctl)
			dynconfig := configmocks.NewMockDynconfigInterface(ctl)
			seedPeer := resource.NewMockSeedPeer(ctl)
			mockHost := resource.NewHost(
				mockRawHost.ID, mockRawHost.IP, mockRawHost.Hostname,
				mockRawHost.Port, mockRawHost.DownloadPort, mockRawHost.Type)
			task := resource.NewTask(mockTaskID, mockTaskURL, mockTaskTag, mockTaskApplication, commonv2.TaskType_STANDARD, mockTaskFilteredQueryParams, mockTaskHeader, mockTaskBackToSourceLimit, resource.WithDigest(mockTaskDigest), resource.WithPieceLength(mockTaskPieceLength))
			peer := resource.NewPeer(mockPeerID, task, mockHost)
			svc := NewV1(tc.config, res, scheduling, dynconfig)
			taskManager := resource.NewMockTaskManager(ctl)

			tc.mock(task, peer, taskManager, seedPeer, res.EXPECT(), taskManager.EXPECT(), seedPeer.EXPECT())
			task, err := svc.prefetchTask(context.Background(), tc.req)
			tc.expect(t, task, err)
		})
	}
}

func TestServiceV1_triggerTask(t *testing.T) {
	tests := []struct {
		name   string
		config *config.Config
		run    func(t *testing.T, svc *V1, mockTask *resource.Task, mockHost *resource.Host, mockPeer *resource.Peer, mockSeedPeer *resource.Peer, dynconfig config.DynconfigInterface, seedPeer resource.SeedPeer, mr *resource.MockResourceMockRecorder, mc *resource.MockSeedPeerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder)
	}{
		{
			name: "task state is TaskStateRunning and it has available peers",
			config: &config.Config{
				Scheduler: mockSchedulerConfig,
			},
			run: func(t *testing.T, svc *V1, mockTask *resource.Task, mockHost *resource.Host, mockPeer *resource.Peer, mockSeedPeer *resource.Peer, dynconfig config.DynconfigInterface, seedPeer resource.SeedPeer, mr *resource.MockResourceMockRecorder, mc *resource.MockSeedPeerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				mockTask.FSM.SetState(resource.TaskStateRunning)
				mockSeedPeer.FSM.SetState(resource.PeerStateRunning)
				mockTask.StorePeer(mockSeedPeer)

				err := svc.triggerTask(context.Background(), &schedulerv1.PeerTaskRequest{
					UrlMeta: &commonv1.UrlMeta{
						Priority: commonv1.Priority_LEVEL0,
					},
				}, mockTask, mockHost, mockPeer, dynconfig)
				assert := assert.New(t)
				assert.NoError(err)
				assert.Equal(mockTask.FSM.Current(), resource.TaskStateRunning)
			},
		},
		{
			name: "task state is TaskStateSucceeded and it has available peers",
			config: &config.Config{
				Scheduler: mockSchedulerConfig,
			},
			run: func(t *testing.T, svc *V1, mockTask *resource.Task, mockHost *resource.Host, mockPeer *resource.Peer, mockSeedPeer *resource.Peer, dynconfig config.DynconfigInterface, seedPeer resource.SeedPeer, mr *resource.MockResourceMockRecorder, mc *resource.MockSeedPeerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				mockTask.FSM.SetState(resource.TaskStateSucceeded)
				mockSeedPeer.FSM.SetState(resource.PeerStateRunning)
				mockTask.StorePeer(mockSeedPeer)

				err := svc.triggerTask(context.Background(), &schedulerv1.PeerTaskRequest{
					UrlMeta: &commonv1.UrlMeta{
						Priority: commonv1.Priority_LEVEL0,
					},
				}, mockTask, mockHost, mockPeer, dynconfig)
				assert := assert.New(t)
				assert.NoError(err)
				assert.Equal(mockTask.FSM.Current(), resource.TaskStateSucceeded)
			},
		},
		{
			name: "task state is TaskStateFailed and host type is HostTypeSuperSeed",
			config: &config.Config{
				Scheduler: mockSchedulerConfig,
			},
			run: func(t *testing.T, svc *V1, mockTask *resource.Task, mockHost *resource.Host, mockPeer *resource.Peer, mockSeedPeer *resource.Peer, dynconfig config.DynconfigInterface, seedPeer resource.SeedPeer, mr *resource.MockResourceMockRecorder, mc *resource.MockSeedPeerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				mockTask.FSM.SetState(resource.TaskStateFailed)
				mockHost.Type = pkgtypes.HostTypeSuperSeed

				err := svc.triggerTask(context.Background(), &schedulerv1.PeerTaskRequest{
					UrlMeta: &commonv1.UrlMeta{
						Priority: commonv1.Priority_LEVEL0,
					},
				}, mockTask, mockHost, mockPeer, dynconfig)
				assert := assert.New(t)
				assert.NoError(err)
				assert.Equal(mockPeer.NeedBackToSource.Load(), true)
				assert.Equal(mockTask.FSM.Current(), resource.TaskStateRunning)
			},
		},
		{
			name: "task state is TaskStatePending and host type is HostTypeStrongSeed",
			config: &config.Config{
				Scheduler: mockSchedulerConfig,
			},
			run: func(t *testing.T, svc *V1, mockTask *resource.Task, mockHost *resource.Host, mockPeer *resource.Peer, mockSeedPeer *resource.Peer, dynconfig config.DynconfigInterface, seedPeer resource.SeedPeer, mr *resource.MockResourceMockRecorder, mc *resource.MockSeedPeerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				mockTask.FSM.SetState(resource.TaskStatePending)
				mockHost.Type = pkgtypes.HostTypeStrongSeed

				err := svc.triggerTask(context.Background(), &schedulerv1.PeerTaskRequest{
					UrlMeta: &commonv1.UrlMeta{
						Priority: commonv1.Priority_LEVEL0,
					},
				}, mockTask, mockHost, mockPeer, dynconfig)
				assert := assert.New(t)
				assert.NoError(err)
				assert.Equal(mockPeer.NeedBackToSource.Load(), true)
				assert.Equal(mockTask.FSM.Current(), resource.TaskStateRunning)
			},
		},
		{
			name: "task state is TaskStateRunning and host type is HostTypeWeakSeed",
			config: &config.Config{
				Scheduler: mockSchedulerConfig,
			},
			run: func(t *testing.T, svc *V1, mockTask *resource.Task, mockHost *resource.Host, mockPeer *resource.Peer, mockSeedPeer *resource.Peer, dynconfig config.DynconfigInterface, seedPeer resource.SeedPeer, mr *resource.MockResourceMockRecorder, mc *resource.MockSeedPeerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				mockTask.FSM.SetState(resource.TaskStateRunning)
				mockHost.Type = pkgtypes.HostTypeWeakSeed

				err := svc.triggerTask(context.Background(), &schedulerv1.PeerTaskRequest{
					UrlMeta: &commonv1.UrlMeta{
						Priority: commonv1.Priority_LEVEL0,
					},
				}, mockTask, mockHost, mockPeer, dynconfig)
				assert := assert.New(t)
				assert.NoError(err)
				assert.Equal(mockPeer.NeedBackToSource.Load(), true)
				assert.Equal(mockTask.FSM.Current(), resource.TaskStateRunning)
			},
		},
		{
			name: "task state is TaskStateSucceeded and host type is HostTypeStrongSeed",
			config: &config.Config{
				Scheduler: mockSchedulerConfig,
			},
			run: func(t *testing.T, svc *V1, mockTask *resource.Task, mockHost *resource.Host, mockPeer *resource.Peer, mockSeedPeer *resource.Peer, dynconfig config.DynconfigInterface, seedPeer resource.SeedPeer, mr *resource.MockResourceMockRecorder, mc *resource.MockSeedPeerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				mockTask.FSM.SetState(resource.TaskStateSucceeded)
				mockHost.Type = pkgtypes.HostTypeStrongSeed

				err := svc.triggerTask(context.Background(), &schedulerv1.PeerTaskRequest{
					UrlMeta: &commonv1.UrlMeta{
						Priority: commonv1.Priority_LEVEL0,
					},
				}, mockTask, mockHost, mockPeer, dynconfig)
				assert := assert.New(t)
				assert.NoError(err)
				assert.Equal(mockPeer.NeedBackToSource.Load(), true)
				assert.Equal(mockTask.FSM.Current(), resource.TaskStateRunning)
			},
		},
		{
			name: "task state is TaskStateLeave and host type is HostTypeStrongSeed",
			config: &config.Config{
				Scheduler: mockSchedulerConfig,
			},
			run: func(t *testing.T, svc *V1, mockTask *resource.Task, mockHost *resource.Host, mockPeer *resource.Peer, mockSeedPeer *resource.Peer, dynconfig config.DynconfigInterface, seedPeer resource.SeedPeer, mr *resource.MockResourceMockRecorder, mc *resource.MockSeedPeerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				mockTask.FSM.SetState(resource.TaskStateLeave)
				mockHost.Type = pkgtypes.HostTypeStrongSeed

				err := svc.triggerTask(context.Background(), &schedulerv1.PeerTaskRequest{
					UrlMeta: &commonv1.UrlMeta{
						Priority: commonv1.Priority_LEVEL0,
					},
				}, mockTask, mockHost, mockPeer, dynconfig)
				assert := assert.New(t)
				assert.NoError(err)
				assert.Equal(mockPeer.NeedBackToSource.Load(), true)
				assert.Equal(mockTask.FSM.Current(), resource.TaskStateRunning)
			},
		},
		{
			name: "priority is Priority_LEVEL6 and seed peer is enabled",
			config: &config.Config{
				Scheduler: mockSchedulerConfig,
				SeedPeer: config.SeedPeerConfig{
					Enable: true,
				},
			},
			run: func(t *testing.T, svc *V1, mockTask *resource.Task, mockHost *resource.Host, mockPeer *resource.Peer, mockSeedPeer *resource.Peer, dynconfig config.DynconfigInterface, seedPeer resource.SeedPeer, mr *resource.MockResourceMockRecorder, mc *resource.MockSeedPeerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				mockTask.FSM.SetState(resource.TaskStatePending)
				mockPeer.Task.Application = "baw"

				var wg sync.WaitGroup
				wg.Add(2)
				defer wg.Wait()

				gomock.InOrder(
					md.GetApplications().Return([]*managerv2.Application{
						{
							Name: "baw",
							Priority: &managerv2.ApplicationPriority{
								Value: commonv2.Priority_LEVEL6,
							},
						},
					}, nil).Times(1),
					mr.SeedPeer().Do(func() { wg.Done() }).Return(seedPeer).Times(1),
					mc.TriggerTask(gomock.Any(), gomock.Any(), gomock.Any()).Do(func(ctx context.Context, rg *nethttp.Range, task *resource.Task) { wg.Done() }).Return(mockPeer, &schedulerv1.PeerResult{}, nil).Times(1),
				)

				err := svc.triggerTask(context.Background(), &schedulerv1.PeerTaskRequest{
					UrlMeta: &commonv1.UrlMeta{
						Priority: commonv1.Priority_LEVEL0,
					},
				}, mockTask, mockHost, mockPeer, dynconfig)
				assert := assert.New(t)
				assert.NoError(err)
				assert.Equal(mockPeer.NeedBackToSource.Load(), false)
				assert.Equal(mockTask.FSM.Current(), resource.TaskStateRunning)
			},
		},
		{
			name: "priority is Priority_LEVEL6 and seed peer downloads failed",
			config: &config.Config{
				Scheduler: mockSchedulerConfig,
				SeedPeer: config.SeedPeerConfig{
					Enable: true,
				},
			},
			run: func(t *testing.T, svc *V1, mockTask *resource.Task, mockHost *resource.Host, mockPeer *resource.Peer, mockSeedPeer *resource.Peer, dynconfig config.DynconfigInterface, seedPeer resource.SeedPeer, mr *resource.MockResourceMockRecorder, mc *resource.MockSeedPeerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				mockTask.FSM.SetState(resource.TaskStatePending)
				mockSeedPeer.FSM.SetState(resource.PeerStateFailed)
				mockTask.StorePeer(mockSeedPeer)

				md.GetApplications().Return(nil, errors.New("foo")).Times(1)

				err := svc.triggerTask(context.Background(), &schedulerv1.PeerTaskRequest{
					UrlMeta: &commonv1.UrlMeta{
						Priority: commonv1.Priority_LEVEL0,
					},
				}, mockTask, mockHost, mockPeer, dynconfig)
				assert := assert.New(t)
				assert.NoError(err)
				assert.Equal(mockPeer.NeedBackToSource.Load(), true)
				assert.Equal(mockTask.FSM.Current(), resource.TaskStateRunning)
			},
		},
		{
			name: "priority is Priority_LEVEL5",
			config: &config.Config{
				Scheduler: mockSchedulerConfig,
				SeedPeer: config.SeedPeerConfig{
					Enable: true,
				},
			},
			run: func(t *testing.T, svc *V1, mockTask *resource.Task, mockHost *resource.Host, mockPeer *resource.Peer, mockSeedPeer *resource.Peer, dynconfig config.DynconfigInterface, seedPeer resource.SeedPeer, mr *resource.MockResourceMockRecorder, mc *resource.MockSeedPeerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				mockTask.FSM.SetState(resource.TaskStatePending)
				mockPeer.Task.Application = "bas"

				md.GetApplications().Return([]*managerv2.Application{
					{
						Name: "bas",
						Priority: &managerv2.ApplicationPriority{
							Value: commonv2.Priority_LEVEL5,
						},
					},
				}, nil).Times(1)

				err := svc.triggerTask(context.Background(), &schedulerv1.PeerTaskRequest{
					UrlMeta: &commonv1.UrlMeta{
						Priority: commonv1.Priority_LEVEL0,
					},
				}, mockTask, mockHost, mockPeer, dynconfig)
				assert := assert.New(t)
				assert.NoError(err)
				assert.Equal(mockPeer.NeedBackToSource.Load(), true)
				assert.Equal(mockTask.FSM.Current(), resource.TaskStateRunning)
			},
		},
		{
			name: "priority is Priority_LEVEL4",
			config: &config.Config{
				Scheduler: mockSchedulerConfig,
				SeedPeer: config.SeedPeerConfig{
					Enable: true,
				},
			},
			run: func(t *testing.T, svc *V1, mockTask *resource.Task, mockHost *resource.Host, mockPeer *resource.Peer, mockSeedPeer *resource.Peer, dynconfig config.DynconfigInterface, seedPeer resource.SeedPeer, mr *resource.MockResourceMockRecorder, mc *resource.MockSeedPeerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				mockTask.FSM.SetState(resource.TaskStatePending)
				mockPeer.Task.Application = "bas"

				md.GetApplications().Return([]*managerv2.Application{
					{
						Name: "bas",
						Priority: &managerv2.ApplicationPriority{
							Value: commonv2.Priority_LEVEL4,
						},
					},
				}, nil).Times(1)

				err := svc.triggerTask(context.Background(), &schedulerv1.PeerTaskRequest{
					UrlMeta: &commonv1.UrlMeta{
						Priority: commonv1.Priority_LEVEL0,
					},
				}, mockTask, mockHost, mockPeer, dynconfig)
				assert := assert.New(t)
				assert.NoError(err)
				assert.Equal(mockPeer.NeedBackToSource.Load(), true)
				assert.Equal(mockTask.FSM.Current(), resource.TaskStateRunning)
			},
		},
		{
			name: "priority is Priority_LEVEL3",
			config: &config.Config{
				Scheduler: mockSchedulerConfig,
				SeedPeer: config.SeedPeerConfig{
					Enable: true,
				},
			},
			run: func(t *testing.T, svc *V1, mockTask *resource.Task, mockHost *resource.Host, mockPeer *resource.Peer, mockSeedPeer *resource.Peer, dynconfig config.DynconfigInterface, seedPeer resource.SeedPeer, mr *resource.MockResourceMockRecorder, mc *resource.MockSeedPeerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				mockTask.FSM.SetState(resource.TaskStatePending)
				mockPeer.Task.Application = "bae"

				md.GetApplications().Return([]*managerv2.Application{
					{
						Name: "bae",
						Priority: &managerv2.ApplicationPriority{
							Value: commonv2.Priority_LEVEL3,
						},
					},
				}, nil).Times(1)

				err := svc.triggerTask(context.Background(), &schedulerv1.PeerTaskRequest{
					UrlMeta: &commonv1.UrlMeta{
						Priority: commonv1.Priority_LEVEL0,
					},
				}, mockTask, mockHost, mockPeer, dynconfig)
				assert := assert.New(t)
				assert.NoError(err)
				assert.Equal(mockPeer.NeedBackToSource.Load(), true)
				assert.Equal(mockTask.FSM.Current(), resource.TaskStateRunning)
			},
		},
		{
			name: "priority is Priority_LEVEL2",
			config: &config.Config{
				Scheduler: mockSchedulerConfig,
				SeedPeer: config.SeedPeerConfig{
					Enable: true,
				},
			},
			run: func(t *testing.T, svc *V1, mockTask *resource.Task, mockHost *resource.Host, mockPeer *resource.Peer, mockSeedPeer *resource.Peer, dynconfig config.DynconfigInterface, seedPeer resource.SeedPeer, mr *resource.MockResourceMockRecorder, mc *resource.MockSeedPeerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				mockTask.FSM.SetState(resource.TaskStatePending)
				mockPeer.Task.Application = "bae"

				md.GetApplications().Return([]*managerv2.Application{
					{
						Name: "bae",
						Priority: &managerv2.ApplicationPriority{
							Value: commonv2.Priority_LEVEL2,
						},
					},
				}, nil).Times(1)

				err := svc.triggerTask(context.Background(), &schedulerv1.PeerTaskRequest{
					UrlMeta: &commonv1.UrlMeta{
						Priority: commonv1.Priority_LEVEL0,
					},
				}, mockTask, mockHost, mockPeer, dynconfig)
				assert := assert.New(t)
				assert.Error(err)
			},
		},
		{
			name: "priority is Priority_LEVEL1",
			config: &config.Config{
				Scheduler: mockSchedulerConfig,
				SeedPeer: config.SeedPeerConfig{
					Enable: true,
				},
			},
			run: func(t *testing.T, svc *V1, mockTask *resource.Task, mockHost *resource.Host, mockPeer *resource.Peer, mockSeedPeer *resource.Peer, dynconfig config.DynconfigInterface, seedPeer resource.SeedPeer, mr *resource.MockResourceMockRecorder, mc *resource.MockSeedPeerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				mockTask.FSM.SetState(resource.TaskStatePending)
				mockPeer.Task.Application = "bat"

				md.GetApplications().Return([]*managerv2.Application{
					{
						Name: "bat",
						Priority: &managerv2.ApplicationPriority{
							Value: commonv2.Priority_LEVEL1,
						},
					},
				}, nil).Times(1)

				err := svc.triggerTask(context.Background(), &schedulerv1.PeerTaskRequest{
					UrlMeta: &commonv1.UrlMeta{
						Priority: commonv1.Priority_LEVEL0,
					},
				}, mockTask, mockHost, mockPeer, dynconfig)
				assert := assert.New(t)
				assert.Error(err)
			},
		},
		{
			name: "priority is Priority_LEVEL0",
			config: &config.Config{
				Scheduler: mockSchedulerConfig,
				SeedPeer: config.SeedPeerConfig{
					Enable: true,
				},
			},
			run: func(t *testing.T, svc *V1, mockTask *resource.Task, mockHost *resource.Host, mockPeer *resource.Peer, mockSeedPeer *resource.Peer, dynconfig config.DynconfigInterface, seedPeer resource.SeedPeer, mr *resource.MockResourceMockRecorder, mc *resource.MockSeedPeerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				mockTask.FSM.SetState(resource.TaskStatePending)
				mockPeer.Task.Application = "bat"

				var wg sync.WaitGroup
				wg.Add(2)
				defer wg.Wait()

				gomock.InOrder(
					md.GetApplications().Return([]*managerv2.Application{
						{
							Name: "bat",
							Priority: &managerv2.ApplicationPriority{
								Value: commonv2.Priority_LEVEL0,
							},
						},
					}, nil).Times(1),
					mr.SeedPeer().Do(func() { wg.Done() }).Return(seedPeer).Times(1),
					mc.TriggerTask(gomock.Any(), gomock.Any(), gomock.Any()).Do(func(ctx context.Context, rg *nethttp.Range, task *resource.Task) { wg.Done() }).Return(mockPeer, &schedulerv1.PeerResult{}, nil).Times(1),
				)

				err := svc.triggerTask(context.Background(), &schedulerv1.PeerTaskRequest{
					UrlMeta: &commonv1.UrlMeta{
						Priority: commonv1.Priority_LEVEL0,
					},
				}, mockTask, mockHost, mockPeer, dynconfig)
				assert := assert.New(t)
				assert.NoError(err)
				assert.Equal(mockPeer.NeedBackToSource.Load(), false)
				assert.Equal(mockTask.FSM.Current(), resource.TaskStateRunning)
			},
		},
		{
			name: "register priority is Priority_LEVEL6 and seed peer is enabled",
			config: &config.Config{
				Scheduler: mockSchedulerConfig,
				SeedPeer: config.SeedPeerConfig{
					Enable: true,
				},
			},
			run: func(t *testing.T, svc *V1, mockTask *resource.Task, mockHost *resource.Host, mockPeer *resource.Peer, mockSeedPeer *resource.Peer, dynconfig config.DynconfigInterface, seedPeer resource.SeedPeer, mr *resource.MockResourceMockRecorder, mc *resource.MockSeedPeerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				mockTask.FSM.SetState(resource.TaskStatePending)
				mockPeer.Priority = commonv2.Priority_LEVEL6

				var wg sync.WaitGroup
				wg.Add(2)
				defer wg.Wait()

				gomock.InOrder(
					mr.SeedPeer().Do(func() { wg.Done() }).Return(seedPeer).Times(1),
					mc.TriggerTask(gomock.Any(), gomock.Any(), gomock.Any()).Do(func(ctx context.Context, rg *nethttp.Range, task *resource.Task) { wg.Done() }).Return(mockPeer, &schedulerv1.PeerResult{}, nil).Times(1),
				)

				err := svc.triggerTask(context.Background(), &schedulerv1.PeerTaskRequest{
					UrlMeta: &commonv1.UrlMeta{
						Priority: commonv1.Priority_LEVEL6,
					},
				}, mockTask, mockHost, mockPeer, dynconfig)
				assert := assert.New(t)
				assert.NoError(err)
				assert.Equal(mockPeer.NeedBackToSource.Load(), false)
				assert.Equal(mockTask.FSM.Current(), resource.TaskStateRunning)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			scheduling := mocks.NewMockScheduling(ctl)
			res := resource.NewMockResource(ctl)
			dynconfig := configmocks.NewMockDynconfigInterface(ctl)
			svc := NewV1(tc.config, res, scheduling, dynconfig)

			mockHost := resource.NewHost(
				mockRawHost.ID, mockRawHost.IP, mockRawHost.Hostname,
				mockRawHost.Port, mockRawHost.DownloadPort, mockRawHost.Type)
			mockSeedHost := resource.NewHost(
				mockRawSeedHost.ID, mockRawSeedHost.IP, mockRawSeedHost.Hostname,
				mockRawSeedHost.Port, mockRawSeedHost.DownloadPort, mockRawSeedHost.Type)
			mockTask := resource.NewTask(mockTaskID, mockTaskURL, mockTaskTag, mockTaskApplication, commonv2.TaskType_STANDARD, mockTaskFilteredQueryParams, mockTaskHeader, mockTaskBackToSourceLimit, resource.WithDigest(mockTaskDigest), resource.WithPieceLength(mockTaskPieceLength))
			mockPeer := resource.NewPeer(mockPeerID, mockTask, mockHost)
			mockSeedPeer := resource.NewPeer(mockSeedPeerID, mockTask, mockSeedHost)
			seedPeer := resource.NewMockSeedPeer(ctl)
			tc.run(t, svc, mockTask, mockHost, mockPeer, mockSeedPeer, dynconfig, seedPeer, res.EXPECT(), seedPeer.EXPECT(), dynconfig.EXPECT())
		})
	}
}

func TestServiceV1_storeTask(t *testing.T) {
	tests := []struct {
		name string
		run  func(t *testing.T, svc *V1, taskManager resource.TaskManager, mr *resource.MockResourceMockRecorder, mt *resource.MockTaskManagerMockRecorder)
	}{
		{
			name: "task already exists",
			run: func(t *testing.T, svc *V1, taskManager resource.TaskManager, mr *resource.MockResourceMockRecorder, mt *resource.MockTaskManagerMockRecorder) {
				mockTask := resource.NewTask(mockTaskID, "", mockTaskTag, mockTaskApplication, commonv2.TaskType_STANDARD, nil, nil, mockTaskBackToSourceLimit)

				gomock.InOrder(
					mr.TaskManager().Return(taskManager).Times(1),
					mt.Load(gomock.Eq(mockTaskID)).Return(mockTask, true).Times(1),
				)

				task := svc.storeTask(context.Background(), &schedulerv1.PeerTaskRequest{
					TaskId: mockTaskID,
					Url:    mockTaskURL,
					UrlMeta: &commonv1.UrlMeta{
						Priority: commonv1.Priority_LEVEL0,
						Filter:   strings.Join(mockTaskFilteredQueryParams, idgen.FilteredQueryParamsSeparator),
						Header:   mockTaskHeader,
					},
					PeerHost: mockPeerHost,
				}, commonv2.TaskType_STANDARD)

				assert := assert.New(t)
				assert.EqualValues(task, mockTask)
			},
		},
		{
			name: "task does not exist",
			run: func(t *testing.T, svc *V1, taskManager resource.TaskManager, mr *resource.MockResourceMockRecorder, mt *resource.MockTaskManagerMockRecorder) {
				gomock.InOrder(
					mr.TaskManager().Return(taskManager).Times(1),
					mt.Load(gomock.Eq(mockTaskID)).Return(nil, false).Times(1),
					mr.TaskManager().Return(taskManager).Times(1),
					mt.Store(gomock.Any()).Return().Times(1),
				)

				task := svc.storeTask(context.Background(), &schedulerv1.PeerTaskRequest{
					TaskId: mockTaskID,
					Url:    mockTaskURL,
					UrlMeta: &commonv1.UrlMeta{
						Priority:    commonv1.Priority_LEVEL0,
						Digest:      mockTaskDigest.String(),
						Tag:         mockTaskTag,
						Application: mockTaskApplication,
						Filter:      strings.Join(mockTaskFilteredQueryParams, idgen.FilteredQueryParamsSeparator),
						Header:      mockTaskHeader,
					},
					PeerHost: mockPeerHost,
				}, commonv2.TaskType_PERSISTENT_CACHE)

				assert := assert.New(t)
				assert.Equal(task.ID, mockTaskID)
				assert.Equal(task.Type, commonv2.TaskType_PERSISTENT_CACHE)
				assert.Equal(task.URL, mockTaskURL)
				assert.EqualValues(task.Digest, mockTaskDigest)
				assert.Equal(task.Tag, mockTaskTag)
				assert.Equal(task.Application, mockTaskApplication)
				assert.EqualValues(task.FilteredQueryParams, mockTaskFilteredQueryParams)
				assert.EqualValues(task.Header, mockTaskHeader)
				assert.Equal(task.PieceLength, int32(0))
				assert.Empty(task.DirectPiece)
				assert.Equal(task.ContentLength.Load(), int64(-1))
				assert.Equal(task.TotalPieceCount.Load(), int32(0))
				assert.Equal(task.BackToSourceLimit.Load(), int32(200))
				assert.Equal(task.BackToSourcePeers.Len(), uint(0))
				assert.Equal(task.FSM.Current(), resource.TaskStatePending)
				assert.Empty(task.Pieces)
				assert.Equal(task.PeerCount(), 0)
				assert.NotEqual(task.CreatedAt.Load(), 0)
				assert.NotEqual(task.UpdatedAt.Load(), 0)
				assert.NotNil(task.Log)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			scheduling := mocks.NewMockScheduling(ctl)
			res := resource.NewMockResource(ctl)
			dynconfig := configmocks.NewMockDynconfigInterface(ctl)
			svc := NewV1(&config.Config{Scheduler: mockSchedulerConfig}, res, scheduling, dynconfig)
			taskManager := resource.NewMockTaskManager(ctl)
			tc.run(t, svc, taskManager, res.EXPECT(), taskManager.EXPECT())
		})
	}
}

func TestServiceV1_storeHost(t *testing.T) {
	tests := []struct {
		name     string
		peerHost *schedulerv1.PeerHost
		mock     func(mockHost *resource.Host, hostManager resource.HostManager, mr *resource.MockResourceMockRecorder, mh *resource.MockHostManagerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder)
		expect   func(t *testing.T, host *resource.Host)
	}{
		{
			name:     "host already exists",
			peerHost: mockPeerHost,
			mock: func(mockHost *resource.Host, hostManager resource.HostManager, mr *resource.MockResourceMockRecorder, mh *resource.MockHostManagerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				gomock.InOrder(
					mr.HostManager().Return(hostManager).Times(1),
					mh.Load(gomock.Eq(mockRawHost.ID)).Return(mockHost, true).Times(1),
				)
			},
			expect: func(t *testing.T, host *resource.Host) {
				assert := assert.New(t)
				assert.Equal(host.ID, mockRawHost.ID)
				assert.Equal(host.Port, mockRawHost.Port)
				assert.Equal(host.DownloadPort, mockRawHost.DownloadPort)
				assert.Equal(host.Network.Location, mockRawHost.Network.Location)
				assert.Equal(host.Network.IDC, mockRawHost.Network.IDC)
				assert.NotEqual(host.UpdatedAt.Load(), mockRawHost.UpdatedAt.Load())
			},
		},
		{
			name:     "host does not exist",
			peerHost: mockPeerHost,
			mock: func(mockHost *resource.Host, hostManager resource.HostManager, mr *resource.MockResourceMockRecorder, mh *resource.MockHostManagerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				gomock.InOrder(
					mr.HostManager().Return(hostManager).Times(1),
					mh.Load(gomock.Eq(mockRawHost.ID)).Return(nil, false).Times(1),
					md.GetSchedulerClusterClientConfig().Return(types.SchedulerClusterClientConfig{LoadLimit: 10}, nil).Times(1),
					mr.HostManager().Return(hostManager).Times(1),
					mh.Store(gomock.Any()).Return().Times(1),
				)
			},
			expect: func(t *testing.T, host *resource.Host) {
				assert := assert.New(t)
				assert.Equal(host.ID, mockRawHost.ID)
				assert.Equal(host.ConcurrentUploadLimit.Load(), int32(10))
			},
		},
		{
			name:     "host does not exist and dynconfig get cluster client config failed",
			peerHost: mockPeerHost,
			mock: func(mockHost *resource.Host, hostManager resource.HostManager, mr *resource.MockResourceMockRecorder, mh *resource.MockHostManagerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				gomock.InOrder(
					mr.HostManager().Return(hostManager).Times(1),
					mh.Load(gomock.Eq(mockRawHost.ID)).Return(nil, false).Times(1),
					md.GetSchedulerClusterClientConfig().Return(types.SchedulerClusterClientConfig{}, errors.New("foo")).Times(1),
					mr.HostManager().Return(hostManager).Times(1),
					mh.Store(gomock.Any()).Return().Times(1),
				)
			},
			expect: func(t *testing.T, host *resource.Host) {
				assert := assert.New(t)
				assert.Equal(host.ID, mockRawHost.ID)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			scheduling := mocks.NewMockScheduling(ctl)
			res := resource.NewMockResource(ctl)
			dynconfig := configmocks.NewMockDynconfigInterface(ctl)
			svc := NewV1(&config.Config{Scheduler: mockSchedulerConfig}, res, scheduling, dynconfig)
			hostManager := resource.NewMockHostManager(ctl)
			mockHost := resource.NewHost(
				mockRawHost.ID, mockRawHost.IP, mockRawHost.Hostname,
				mockRawHost.Port, mockRawHost.DownloadPort, mockRawHost.Type)

			tc.mock(mockHost, hostManager, res.EXPECT(), hostManager.EXPECT(), dynconfig.EXPECT())
			host := svc.storeHost(context.Background(), tc.peerHost)
			tc.expect(t, host)
		})
	}
}

func TestServiceV1_storePeer(t *testing.T) {
	tests := []struct {
		name string
		run  func(t *testing.T, svc *V1, peerManager resource.PeerManager, mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder)
	}{
		{
			name: "peer already exists",
			run: func(t *testing.T, svc *V1, peerManager resource.PeerManager, mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder) {
				mockHost := resource.NewHost(
					mockRawHost.ID, mockRawHost.IP, mockRawHost.Hostname,
					mockRawHost.Port, mockRawHost.DownloadPort, mockRawHost.Type)
				mockTask := resource.NewTask(mockTaskID, mockTaskURL, mockTaskTag, mockTaskApplication, commonv2.TaskType_STANDARD, mockTaskFilteredQueryParams, mockTaskHeader, mockTaskBackToSourceLimit, resource.WithDigest(mockTaskDigest), resource.WithPieceLength(mockTaskPieceLength))
				mockPeer := resource.NewPeer(mockPeerID, mockTask, mockHost)

				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(mockPeerID)).Return(mockPeer, true).Times(1),
				)

				peer := svc.storePeer(context.Background(), mockPeerID, commonv1.Priority_LEVEL0, mockURLMetaRange, mockTask, mockHost)

				assert := assert.New(t)
				assert.EqualValues(peer, mockPeer)
			},
		},
		{
			name: "peer does not exists",
			run: func(t *testing.T, svc *V1, peerManager resource.PeerManager, mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder) {
				mockHost := resource.NewHost(
					mockRawHost.ID, mockRawHost.IP, mockRawHost.Hostname,
					mockRawHost.Port, mockRawHost.DownloadPort, mockRawHost.Type)
				mockTask := resource.NewTask(mockTaskID, mockTaskURL, mockTaskTag, mockTaskApplication, commonv2.TaskType_STANDARD, mockTaskFilteredQueryParams, mockTaskHeader, mockTaskBackToSourceLimit, resource.WithDigest(mockTaskDigest), resource.WithPieceLength(mockTaskPieceLength))
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(mockPeerID)).Return(nil, false).Times(1),
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Store(gomock.Any()).Return().Times(1),
				)

				peer := svc.storePeer(context.Background(), mockPeerID, commonv1.Priority_LEVEL1, mockURLMetaRange, mockTask, mockHost)

				assert := assert.New(t)
				assert.Equal(peer.ID, mockPeerID)
				assert.EqualValues(peer.Range, &mockPeerRange)
				assert.Equal(peer.Priority, commonv2.Priority_LEVEL1)
				assert.Empty(peer.Pieces)
				assert.Empty(peer.FinishedPieces)
				assert.Equal(len(peer.PieceCosts()), 0)
				assert.Empty(peer.ReportPieceResultStream)
				assert.Empty(peer.AnnouncePeerStream)
				assert.Equal(peer.FSM.Current(), resource.PeerStatePending)
				assert.EqualValues(peer.Task, mockTask)
				assert.EqualValues(peer.Host, mockHost)
				assert.Equal(peer.BlockParents.Len(), uint(0))
				assert.Equal(peer.NeedBackToSource.Load(), false)
				assert.NotEqual(peer.PieceUpdatedAt.Load(), 0)
				assert.NotEqual(peer.CreatedAt.Load(), 0)
				assert.NotEqual(peer.UpdatedAt.Load(), 0)
				assert.NotNil(peer.Log)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			scheduling := mocks.NewMockScheduling(ctl)
			res := resource.NewMockResource(ctl)
			dynconfig := configmocks.NewMockDynconfigInterface(ctl)
			svc := NewV1(&config.Config{Scheduler: mockSchedulerConfig}, res, scheduling, dynconfig)
			peerManager := resource.NewMockPeerManager(ctl)

			tc.run(t, svc, peerManager, res.EXPECT(), peerManager.EXPECT())
		})
	}
}

func TestServiceV1_triggerSeedPeerTask(t *testing.T) {
	tests := []struct {
		name   string
		mock   func(task *resource.Task, peer *resource.Peer, seedPeer resource.SeedPeer, mr *resource.MockResourceMockRecorder, mc *resource.MockSeedPeerMockRecorder)
		expect func(t *testing.T, task *resource.Task, peer *resource.Peer)
	}{
		{
			name: "trigger seed peer task",
			mock: func(task *resource.Task, peer *resource.Peer, seedPeer resource.SeedPeer, mr *resource.MockResourceMockRecorder, mc *resource.MockSeedPeerMockRecorder) {
				task.FSM.SetState(resource.TaskStateRunning)
				peer.FSM.SetState(resource.PeerStateRunning)
				gomock.InOrder(
					mr.SeedPeer().Return(seedPeer).Times(1),
					mc.TriggerTask(gomock.Any(), gomock.Any(), gomock.Any()).Return(peer, &schedulerv1.PeerResult{
						TotalPieceCount: 3,
						ContentLength:   1024,
					}, nil).Times(1),
				)
			},
			expect: func(t *testing.T, task *resource.Task, peer *resource.Peer) {
				assert := assert.New(t)
				assert.True(task.FSM.Is(resource.TaskStateSucceeded))
				assert.Equal(task.TotalPieceCount.Load(), int32(3))
				assert.Equal(task.ContentLength.Load(), int64(1024))
				assert.True(peer.FSM.Is(resource.PeerStateSucceeded))
			},
		},
		{
			name: "trigger seed peer task failed",
			mock: func(task *resource.Task, peer *resource.Peer, seedPeer resource.SeedPeer, mr *resource.MockResourceMockRecorder, mc *resource.MockSeedPeerMockRecorder) {
				task.FSM.SetState(resource.TaskStateRunning)
				gomock.InOrder(
					mr.SeedPeer().Return(seedPeer).Times(1),
					mc.TriggerTask(gomock.Any(), gomock.Any(), gomock.Any()).Return(peer, &schedulerv1.PeerResult{}, errors.New("foo")).Times(1),
				)
			},
			expect: func(t *testing.T, task *resource.Task, peer *resource.Peer) {
				assert := assert.New(t)
				assert.True(task.FSM.Is(resource.TaskStateFailed))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			scheduling := mocks.NewMockScheduling(ctl)
			res := resource.NewMockResource(ctl)
			dynconfig := configmocks.NewMockDynconfigInterface(ctl)
			seedPeer := resource.NewMockSeedPeer(ctl)
			mockHost := resource.NewHost(
				mockRawHost.ID, mockRawHost.IP, mockRawHost.Hostname,
				mockRawHost.Port, mockRawHost.DownloadPort, mockRawHost.Type)
			task := resource.NewTask(mockTaskID, mockTaskURL, mockTaskTag, mockTaskApplication, commonv2.TaskType_STANDARD, mockTaskFilteredQueryParams, mockTaskHeader, mockTaskBackToSourceLimit, resource.WithDigest(mockTaskDigest), resource.WithPieceLength(mockTaskPieceLength))
			peer := resource.NewPeer(mockPeerID, task, mockHost)
			svc := NewV1(&config.Config{Scheduler: mockSchedulerConfig, SeedPeer: mockSeedPeerConfig}, res, scheduling, dynconfig)

			tc.mock(task, peer, seedPeer, res.EXPECT(), seedPeer.EXPECT())
			svc.triggerSeedPeerTask(context.Background(), &mockPeerRange, task)
			tc.expect(t, task, peer)
		})
	}
}

func TestServiceV1_handleBeginOfPiece(t *testing.T) {
	tests := []struct {
		name   string
		mock   func(peer *resource.Peer, scheduling *mocks.MockSchedulingMockRecorder)
		expect func(t *testing.T, peer *resource.Peer)
	}{
		{
			name: "peer state is PeerStateBackToSource",
			mock: func(peer *resource.Peer, scheduling *mocks.MockSchedulingMockRecorder) {
				peer.FSM.SetState(resource.PeerStateBackToSource)
			},
			expect: func(t *testing.T, peer *resource.Peer) {
				assert := assert.New(t)
				assert.True(peer.FSM.Is(resource.PeerStateBackToSource))
			},
		},
		{
			name: "peer state is PeerStateReceivedTiny",
			mock: func(peer *resource.Peer, scheduling *mocks.MockSchedulingMockRecorder) {
				peer.FSM.SetState(resource.PeerStateReceivedTiny)
			},
			expect: func(t *testing.T, peer *resource.Peer) {
				assert := assert.New(t)
				assert.True(peer.FSM.Is(resource.PeerStateRunning))
			},
		},
		{
			name: "peer state is PeerStateReceivedSmall",
			mock: func(peer *resource.Peer, scheduling *mocks.MockSchedulingMockRecorder) {
				peer.FSM.SetState(resource.PeerStateReceivedSmall)
			},
			expect: func(t *testing.T, peer *resource.Peer) {
				assert := assert.New(t)
				assert.True(peer.FSM.Is(resource.PeerStateRunning))
			},
		},
		{
			name: "peer state is PeerStateReceivedNormal",
			mock: func(peer *resource.Peer, scheduling *mocks.MockSchedulingMockRecorder) {
				peer.FSM.SetState(resource.PeerStateReceivedNormal)
				scheduling.ScheduleParentAndCandidateParents(gomock.Any(), gomock.Eq(peer), gomock.Eq(set.NewSafeSet[string]())).Return().Times(1)
			},
			expect: func(t *testing.T, peer *resource.Peer) {
				assert := assert.New(t)
				assert.True(peer.FSM.Is(resource.PeerStateRunning))
			},
		},
		{
			name: "peer state is PeerStateSucceeded",
			mock: func(peer *resource.Peer, scheduling *mocks.MockSchedulingMockRecorder) {
				peer.FSM.SetState(resource.PeerStateSucceeded)
			},
			expect: func(t *testing.T, peer *resource.Peer) {
				assert := assert.New(t)
				assert.True(peer.FSM.Is(resource.PeerStateSucceeded))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			scheduling := mocks.NewMockScheduling(ctl)
			res := resource.NewMockResource(ctl)
			dynconfig := configmocks.NewMockDynconfigInterface(ctl)
			mockHost := resource.NewHost(
				mockRawHost.ID, mockRawHost.IP, mockRawHost.Hostname,
				mockRawHost.Port, mockRawHost.DownloadPort, mockRawHost.Type)
			mockTask := resource.NewTask(mockTaskID, mockTaskURL, mockTaskTag, mockTaskApplication, commonv2.TaskType_STANDARD, mockTaskFilteredQueryParams, mockTaskHeader, mockTaskBackToSourceLimit, resource.WithDigest(mockTaskDigest), resource.WithPieceLength(mockTaskPieceLength))
			peer := resource.NewPeer(mockPeerID, mockTask, mockHost)
			svc := NewV1(&config.Config{Scheduler: mockSchedulerConfig}, res, scheduling, dynconfig)

			tc.mock(peer, scheduling.EXPECT())
			svc.handleBeginOfPiece(context.Background(), peer)
			tc.expect(t, peer)
		})
	}
}

func TestServiceV1_handlePieceSuccess(t *testing.T) {
	mockHost := resource.NewHost(
		mockRawHost.ID, mockRawHost.IP, mockRawHost.Hostname,
		mockRawHost.Port, mockRawHost.DownloadPort, mockRawHost.Type)
	mockTask := resource.NewTask(mockTaskID, mockTaskURL, mockTaskTag, mockTaskApplication, commonv2.TaskType_STANDARD, mockTaskFilteredQueryParams, mockTaskHeader, mockTaskBackToSourceLimit, resource.WithDigest(mockTaskDigest), resource.WithPieceLength(mockTaskPieceLength))

	tests := []struct {
		name   string
		piece  *schedulerv1.PieceResult
		peer   *resource.Peer
		mock   func(peer *resource.Peer, peerManager resource.PeerManager, mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder)
		expect func(t *testing.T, peer *resource.Peer)
	}{
		{
			name: "piece success",
			piece: &schedulerv1.PieceResult{
				DstPid: mockSeedPeerID,
				PieceInfo: &commonv1.PieceInfo{
					PieceNum:     1,
					RangeStart:   2,
					RangeSize:    10,
					PieceMd5:     mockPieceMD5.Encoded,
					DownloadCost: 1,
				},
			},
			peer: resource.NewPeer(mockPeerID, mockTask, mockHost),
			mock: func(peer *resource.Peer, peerManager resource.PeerManager, mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder) {
				peer.FSM.SetState(resource.PeerStateRunning)
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(mockSeedPeerID)).Return(nil, false).Times(1),
				)
			},
			expect: func(t *testing.T, peer *resource.Peer) {
				assert := assert.New(t)
				piece, loaded := peer.LoadPiece(1)
				assert.True(loaded)
				assert.Equal(piece.Number, int32(1))
				assert.Equal(piece.ParentID, mockSeedPeerID)
				assert.Equal(piece.Offset, uint64(2))
				assert.Equal(piece.Length, uint64(10))
				assert.EqualValues(piece.Digest, mockPieceMD5)
				assert.Equal(piece.TrafficType, commonv2.TrafficType_REMOTE_PEER)
				assert.Equal(piece.Cost, time.Duration(1*time.Millisecond))
				assert.NotEqual(piece.CreatedAt.Nanosecond(), 0)
				assert.Equal(peer.FinishedPieces.Count(), uint(1))
				assert.EqualValues(peer.PieceCosts(), []time.Duration{time.Duration(1 * time.Millisecond)})
			},
		},
		{
			name: "piece success without digest",
			piece: &schedulerv1.PieceResult{
				DstPid: mockSeedPeerID,
				PieceInfo: &commonv1.PieceInfo{
					PieceNum:     1,
					RangeStart:   2,
					RangeSize:    10,
					DownloadCost: 1,
				},
			},
			peer: resource.NewPeer(mockPeerID, mockTask, mockHost),
			mock: func(peer *resource.Peer, peerManager resource.PeerManager, mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder) {
				peer.FSM.SetState(resource.PeerStateRunning)
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(mockSeedPeerID)).Return(peer, true).Times(1),
				)
			},
			expect: func(t *testing.T, peer *resource.Peer) {
				assert := assert.New(t)
				piece, loaded := peer.LoadPiece(1)
				assert.True(loaded)
				assert.Equal(piece.Number, int32(1))
				assert.Equal(piece.ParentID, mockSeedPeerID)
				assert.Equal(piece.Offset, uint64(2))
				assert.Equal(piece.Length, uint64(10))
				assert.Nil(piece.Digest)
				assert.Equal(piece.TrafficType, commonv2.TrafficType_REMOTE_PEER)
				assert.Equal(piece.Cost, time.Duration(1*time.Millisecond))
				assert.NotEqual(piece.CreatedAt.Nanosecond(), 0)
				assert.Equal(peer.FinishedPieces.Count(), uint(1))
				assert.EqualValues(peer.PieceCosts(), []time.Duration{time.Duration(1 * time.Millisecond)})
				assert.NotEqual(peer.UpdatedAt.Load(), 0)
			},
		},
		{
			name: "piece state is PeerStateBackToSource",
			piece: &schedulerv1.PieceResult{
				PieceInfo: &commonv1.PieceInfo{
					PieceNum:     1,
					RangeStart:   2,
					RangeSize:    10,
					DownloadCost: 1,
				},
			},
			peer: resource.NewPeer(mockPeerID, mockTask, mockHost),
			mock: func(peer *resource.Peer, peerManager resource.PeerManager, mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder) {
				peer.FSM.SetState(resource.PeerStateBackToSource)
			},
			expect: func(t *testing.T, peer *resource.Peer) {
				assert := assert.New(t)
				piece, loaded := peer.LoadPiece(1)
				assert.True(loaded)
				assert.Equal(piece.Number, int32(1))
				assert.Empty(piece.ParentID)
				assert.Equal(piece.Offset, uint64(2))
				assert.Equal(piece.Length, uint64(10))
				assert.Nil(piece.Digest)
				assert.Equal(piece.TrafficType, commonv2.TrafficType_BACK_TO_SOURCE)
				assert.Equal(piece.Cost, time.Duration(1*time.Millisecond))
				assert.NotEqual(piece.CreatedAt.Nanosecond(), 0)
				assert.Equal(peer.FinishedPieces.Count(), uint(1))
				assert.EqualValues(peer.PieceCosts(), []time.Duration{time.Duration(1 * time.Millisecond)})
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			scheduling := mocks.NewMockScheduling(ctl)
			res := resource.NewMockResource(ctl)
			dynconfig := configmocks.NewMockDynconfigInterface(ctl)
			peerManager := resource.NewMockPeerManager(ctl)
			svc := NewV1(&config.Config{Scheduler: mockSchedulerConfig, Metrics: config.MetricsConfig{EnableHost: true}}, res, scheduling, dynconfig)

			tc.mock(tc.peer, peerManager, res.EXPECT(), peerManager.EXPECT())
			svc.handlePieceSuccess(context.Background(), tc.peer, tc.piece)
			tc.expect(t, tc.peer)
		})
	}
}

func TestServiceV1_handlePieceFail(t *testing.T) {

	tests := []struct {
		name   string
		config *config.Config
		piece  *schedulerv1.PieceResult
		peer   *resource.Peer
		parent *resource.Peer
		run    func(t *testing.T, svc *V1, peer *resource.Peer, parent *resource.Peer, piece *schedulerv1.PieceResult, peerManager resource.PeerManager, seedPeer resource.SeedPeer, ms *mocks.MockSchedulingMockRecorder, mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder, mc *resource.MockSeedPeerMockRecorder)
	}{
		{
			name: "peer state is PeerStateBackToSource",
			config: &config.Config{
				Scheduler: mockSchedulerConfig,
				SeedPeer:  config.SeedPeerConfig{Enable: true},
				Metrics:   config.MetricsConfig{EnableHost: true},
			},
			piece: &schedulerv1.PieceResult{},
			run: func(t *testing.T, svc *V1, peer *resource.Peer, parent *resource.Peer, piece *schedulerv1.PieceResult, peerManager resource.PeerManager, seedPeer resource.SeedPeer, ms *mocks.MockSchedulingMockRecorder, mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder, mc *resource.MockSeedPeerMockRecorder) {
				peer.FSM.SetState(resource.PeerStateBackToSource)

				svc.handlePieceFailure(context.Background(), peer, piece)
				assert := assert.New(t)
				assert.True(peer.FSM.Is(resource.PeerStateBackToSource))
				assert.Equal(parent.Host.UploadFailedCount.Load(), int64(0))
			},
		},
		{
			name: "can not found parent",
			config: &config.Config{
				Scheduler: mockSchedulerConfig,
				SeedPeer:  config.SeedPeerConfig{Enable: true},
				Metrics:   config.MetricsConfig{EnableHost: true},
			},
			piece: &schedulerv1.PieceResult{
				Code:   commonv1.Code_ClientWaitPieceReady,
				DstPid: mockSeedPeerID,
			},
			run: func(t *testing.T, svc *V1, peer *resource.Peer, parent *resource.Peer, piece *schedulerv1.PieceResult, peerManager resource.PeerManager, seedPeer resource.SeedPeer, ms *mocks.MockSchedulingMockRecorder, mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder, mc *resource.MockSeedPeerMockRecorder) {
				peer.FSM.SetState(resource.PeerStateRunning)
				blocklist := set.NewSafeSet[string]()
				blocklist.Add(mockSeedPeerID)
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(parent.ID)).Return(nil, false).Times(1),
					ms.ScheduleParentAndCandidateParents(gomock.Any(), gomock.Eq(peer), gomock.Eq(blocklist)).Return().Times(1),
				)

				svc.handlePieceFailure(context.Background(), peer, piece)
				assert := assert.New(t)
				assert.True(peer.FSM.Is(resource.PeerStateRunning))
				assert.Equal(parent.Host.UploadFailedCount.Load(), int64(0))
			},
		},
		{
			name: "piece result code is Code_PeerTaskNotFound and parent state set PeerEventDownloadFailed",
			config: &config.Config{
				Scheduler: mockSchedulerConfig,
				SeedPeer:  config.SeedPeerConfig{Enable: true},
				Metrics:   config.MetricsConfig{EnableHost: true},
			},
			piece: &schedulerv1.PieceResult{
				Code:   commonv1.Code_PeerTaskNotFound,
				DstPid: mockSeedPeerID,
			},
			run: func(t *testing.T, svc *V1, peer *resource.Peer, parent *resource.Peer, piece *schedulerv1.PieceResult, peerManager resource.PeerManager, seedPeer resource.SeedPeer, ms *mocks.MockSchedulingMockRecorder, mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder, mc *resource.MockSeedPeerMockRecorder) {
				peer.FSM.SetState(resource.PeerStateRunning)
				parent.FSM.SetState(resource.PeerStateRunning)
				blocklist := set.NewSafeSet[string]()
				blocklist.Add(parent.ID)
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(parent.ID)).Return(parent, true).Times(1),
					ms.ScheduleParentAndCandidateParents(gomock.Any(), gomock.Eq(peer), gomock.Eq(blocklist)).Return().Times(1),
				)

				svc.handlePieceFailure(context.Background(), peer, piece)
				assert := assert.New(t)
				assert.True(peer.FSM.Is(resource.PeerStateRunning))
				assert.True(parent.FSM.Is(resource.PeerStateFailed))
				assert.Equal(parent.Host.UploadFailedCount.Load(), int64(1))
			},
		},
		{
			name: "piece result code is Code_ClientPieceNotFound and parent is not seed peer",
			config: &config.Config{
				Scheduler: mockSchedulerConfig,
				SeedPeer:  config.SeedPeerConfig{Enable: true},
				Metrics:   config.MetricsConfig{EnableHost: true},
			},
			piece: &schedulerv1.PieceResult{
				Code:   commonv1.Code_ClientPieceNotFound,
				DstPid: mockSeedPeerID,
			},
			run: func(t *testing.T, svc *V1, peer *resource.Peer, parent *resource.Peer, piece *schedulerv1.PieceResult, peerManager resource.PeerManager, seedPeer resource.SeedPeer, ms *mocks.MockSchedulingMockRecorder, mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder, mc *resource.MockSeedPeerMockRecorder) {
				peer.FSM.SetState(resource.PeerStateRunning)
				peer.Host.Type = pkgtypes.HostTypeNormal
				blocklist := set.NewSafeSet[string]()
				blocklist.Add(parent.ID)
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(parent.ID)).Return(parent, true).Times(1),
					ms.ScheduleParentAndCandidateParents(gomock.Any(), gomock.Eq(peer), gomock.Eq(blocklist)).Return().Times(1),
				)

				svc.handlePieceFailure(context.Background(), peer, piece)
				assert := assert.New(t)
				assert.True(peer.FSM.Is(resource.PeerStateRunning))
				assert.Equal(parent.Host.UploadFailedCount.Load(), int64(1))
			},
		},
		{
			name: "piece result code is Code_ClientPieceRequestFail",
			config: &config.Config{
				Scheduler: mockSchedulerConfig,
				SeedPeer:  config.SeedPeerConfig{Enable: true},
				Metrics:   config.MetricsConfig{EnableHost: true},
			},
			piece: &schedulerv1.PieceResult{
				Code:   commonv1.Code_ClientPieceRequestFail,
				DstPid: mockSeedPeerID,
			},
			run: func(t *testing.T, svc *V1, peer *resource.Peer, parent *resource.Peer, piece *schedulerv1.PieceResult, peerManager resource.PeerManager, seedPeer resource.SeedPeer, ms *mocks.MockSchedulingMockRecorder, mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder, mc *resource.MockSeedPeerMockRecorder) {
				peer.FSM.SetState(resource.PeerStateRunning)
				parent.FSM.SetState(resource.PeerStateRunning)
				blocklist := set.NewSafeSet[string]()
				blocklist.Add(parent.ID)
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(parent.ID)).Return(parent, true).Times(1),
					ms.ScheduleParentAndCandidateParents(gomock.Any(), gomock.Eq(peer), gomock.Eq(blocklist)).Return().Times(1),
				)

				svc.handlePieceFailure(context.Background(), peer, piece)
				assert := assert.New(t)
				assert.True(peer.FSM.Is(resource.PeerStateRunning))
				assert.True(parent.FSM.Is(resource.PeerStateRunning))
				assert.Equal(parent.Host.UploadFailedCount.Load(), int64(1))
			},
		},
		{
			name: "piece result code is unknow",
			config: &config.Config{
				Scheduler: mockSchedulerConfig,
				SeedPeer:  config.SeedPeerConfig{Enable: true},
				Metrics:   config.MetricsConfig{EnableHost: true},
			},
			piece: &schedulerv1.PieceResult{
				Code:   commonv1.Code_ClientPieceRequestFail,
				DstPid: mockSeedPeerID,
			},
			run: func(t *testing.T, svc *V1, peer *resource.Peer, parent *resource.Peer, piece *schedulerv1.PieceResult, peerManager resource.PeerManager, seedPeer resource.SeedPeer, ms *mocks.MockSchedulingMockRecorder, mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder, mc *resource.MockSeedPeerMockRecorder) {
				peer.FSM.SetState(resource.PeerStateRunning)
				parent.FSM.SetState(resource.PeerStateRunning)
				blocklist := set.NewSafeSet[string]()
				blocklist.Add(parent.ID)
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(parent.ID)).Return(parent, true).Times(1),
					ms.ScheduleParentAndCandidateParents(gomock.Any(), gomock.Eq(peer), gomock.Eq(blocklist)).Return().Times(1),
				)

				svc.handlePieceFailure(context.Background(), peer, piece)
				assert := assert.New(t)
				assert.True(peer.FSM.Is(resource.PeerStateRunning))
				assert.True(parent.FSM.Is(resource.PeerStateRunning))
				assert.Equal(parent.Host.UploadFailedCount.Load(), int64(1))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			scheduling := mocks.NewMockScheduling(ctl)
			res := resource.NewMockResource(ctl)
			dynconfig := configmocks.NewMockDynconfigInterface(ctl)
			peerManager := resource.NewMockPeerManager(ctl)
			mockHost := resource.NewHost(
				mockRawHost.ID, mockRawHost.IP, mockRawHost.Hostname,
				mockRawHost.Port, mockRawHost.DownloadPort, mockRawHost.Type)
			mockTask := resource.NewTask(mockTaskID, mockTaskURL, mockTaskTag, mockTaskApplication, commonv2.TaskType_STANDARD, mockTaskFilteredQueryParams, mockTaskHeader, mockTaskBackToSourceLimit, resource.WithDigest(mockTaskDigest), resource.WithPieceLength(mockTaskPieceLength))
			peer := resource.NewPeer(mockPeerID, mockTask, mockHost)
			parent := resource.NewPeer(mockSeedPeerID, mockTask, mockHost)
			seedPeer := resource.NewMockSeedPeer(ctl)
			svc := NewV1(tc.config, res, scheduling, dynconfig)

			tc.run(t, svc, peer, parent, tc.piece, peerManager, seedPeer, scheduling.EXPECT(), res.EXPECT(), peerManager.EXPECT(), seedPeer.EXPECT())
		})
	}
}

func TestServiceV1_handlePeerSuccess(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, err := w.Write([]byte{1}); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusOK)
	}))
	defer s.Close()

	tests := []struct {
		name   string
		mock   func(peer *resource.Peer)
		expect func(t *testing.T, peer *resource.Peer)
	}{
		{
			name: "peer is tiny type and download piece success",
			mock: func(peer *resource.Peer) {
				peer.FSM.SetState(resource.PeerStateBackToSource)
				peer.Task.ContentLength.Store(1)
				peer.Task.TotalPieceCount.Store(1)
			},
			expect: func(t *testing.T, peer *resource.Peer) {
				assert := assert.New(t)
				assert.Equal(peer.Task.DirectPiece, []byte{1})
				assert.True(peer.FSM.Is(resource.PeerStateSucceeded))
				assert.NotEmpty(peer.Cost.Load().Nanoseconds())
			},
		},
		{
			name: "get task size scope failed",
			mock: func(peer *resource.Peer) {
				peer.FSM.SetState(resource.PeerStateBackToSource)
				peer.Task.ContentLength.Store(-1)
				peer.Task.TotalPieceCount.Store(1)
			},
			expect: func(t *testing.T, peer *resource.Peer) {
				assert := assert.New(t)
				assert.Equal(peer.Task.DirectPiece, []byte{})
				assert.True(peer.FSM.Is(resource.PeerStateSucceeded))
				assert.NotEmpty(peer.Cost.Load().Nanoseconds())
			},
		},
		{
			name: "peer is tiny type and download piece failed",
			mock: func(peer *resource.Peer) {
				peer.FSM.SetState(resource.PeerStateBackToSource)
			},
			expect: func(t *testing.T, peer *resource.Peer) {
				assert := assert.New(t)
				assert.Empty(peer.Task.DirectPiece)
				assert.True(peer.FSM.Is(resource.PeerStateSucceeded))
				assert.NotEmpty(peer.Cost.Load().Nanoseconds())
			},
		},
		{
			name: "peer is small and state is PeerStateBackToSource",
			mock: func(peer *resource.Peer) {
				peer.FSM.SetState(resource.PeerStateBackToSource)
				peer.Task.ContentLength.Store(resource.TinyFileSize + 1)
				peer.Task.TotalPieceCount.Store(1)
			},
			expect: func(t *testing.T, peer *resource.Peer) {
				assert := assert.New(t)
				assert.Empty(peer.Task.DirectPiece)
				assert.True(peer.FSM.Is(resource.PeerStateSucceeded))
				assert.NotEmpty(peer.Cost.Load().Nanoseconds())
			},
		},
		{
			name: "peer is small and state is PeerStateRunning",
			mock: func(peer *resource.Peer) {
				peer.FSM.SetState(resource.PeerStateRunning)
				peer.Task.ContentLength.Store(resource.TinyFileSize + 1)
			},
			expect: func(t *testing.T, peer *resource.Peer) {
				assert := assert.New(t)
				assert.Empty(peer.Task.DirectPiece)
				assert.True(peer.FSM.Is(resource.PeerStateSucceeded))
				assert.NotEmpty(peer.Cost.Load().Nanoseconds())
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			scheduling := mocks.NewMockScheduling(ctl)
			res := resource.NewMockResource(ctl)
			dynconfig := configmocks.NewMockDynconfigInterface(ctl)

			url, err := url.Parse(s.URL)
			if err != nil {
				t.Fatal(err)
			}

			ip, rawPort, err := net.SplitHostPort(url.Host)
			if err != nil {
				t.Fatal(err)
			}

			port, err := strconv.ParseInt(rawPort, 10, 32)
			if err != nil {
				t.Fatal(err)
			}

			mockRawHost.IP = ip
			mockRawHost.DownloadPort = int32(port)
			mockHost := resource.NewHost(
				mockRawHost.ID, mockRawHost.IP, mockRawHost.Hostname,
				mockRawHost.Port, mockRawHost.DownloadPort, mockRawHost.Type)
			mockTask := resource.NewTask(mockTaskID, mockTaskURL, mockTaskTag, mockTaskApplication, commonv2.TaskType_STANDARD, mockTaskFilteredQueryParams, mockTaskHeader, mockTaskBackToSourceLimit, resource.WithDigest(mockTaskDigest), resource.WithPieceLength(mockTaskPieceLength))
			peer := resource.NewPeer(mockPeerID, mockTask, mockHost)
			svc := NewV1(&config.Config{Scheduler: mockSchedulerConfig, Metrics: config.MetricsConfig{EnableHost: true}}, res, scheduling, dynconfig)

			tc.mock(peer)
			svc.handlePeerSuccess(context.Background(), peer)
			tc.expect(t, peer)
		})
	}
}

func TestServiceV1_handlePeerFail(t *testing.T) {

	tests := []struct {
		name   string
		mock   func(peer *resource.Peer, child *resource.Peer, ms *mocks.MockSchedulingMockRecorder)
		expect func(t *testing.T, peer *resource.Peer, child *resource.Peer)
	}{
		{
			name: "peer state is PeerStateFailed",
			mock: func(peer *resource.Peer, child *resource.Peer, ms *mocks.MockSchedulingMockRecorder) {
				peer.FSM.SetState(resource.PeerStateFailed)
			},
			expect: func(t *testing.T, peer *resource.Peer, child *resource.Peer) {
				assert := assert.New(t)
				assert.True(peer.FSM.Is(resource.PeerStateFailed))
			},
		},
		{
			name: "peer state is PeerStateLeave",
			mock: func(peer *resource.Peer, child *resource.Peer, ms *mocks.MockSchedulingMockRecorder) {
				peer.FSM.SetState(resource.PeerStateLeave)
			},
			expect: func(t *testing.T, peer *resource.Peer, child *resource.Peer) {
				assert := assert.New(t)
				assert.True(peer.FSM.Is(resource.PeerStateLeave))
			},
		},
		{
			name: "peer state is PeerStateRunning and children need to be scheduled",
			mock: func(peer *resource.Peer, child *resource.Peer, ms *mocks.MockSchedulingMockRecorder) {
				peer.Task.StorePeer(peer)
				peer.Task.StorePeer(child)
				if err := peer.Task.AddPeerEdge(peer, child); err != nil {
					t.Fatal(err)
				}
				peer.FSM.SetState(resource.PeerStateRunning)
				child.FSM.SetState(resource.PeerStateRunning)

				ms.ScheduleParentAndCandidateParents(gomock.Any(), gomock.Eq(child), gomock.Eq(set.NewSafeSet[string]())).Return().Times(1)
			},
			expect: func(t *testing.T, peer *resource.Peer, child *resource.Peer) {
				assert := assert.New(t)
				assert.True(peer.FSM.Is(resource.PeerStateFailed))
			},
		},
		{
			name: "peer state is PeerStateRunning and it has no children",
			mock: func(peer *resource.Peer, child *resource.Peer, ms *mocks.MockSchedulingMockRecorder) {
				peer.Task.StorePeer(peer)
				peer.FSM.SetState(resource.PeerStateRunning)
			},
			expect: func(t *testing.T, peer *resource.Peer, child *resource.Peer) {
				assert := assert.New(t)
				assert.True(peer.FSM.Is(resource.PeerStateFailed))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			scheduling := mocks.NewMockScheduling(ctl)
			res := resource.NewMockResource(ctl)
			dynconfig := configmocks.NewMockDynconfigInterface(ctl)
			svc := NewV1(&config.Config{Scheduler: mockSchedulerConfig, Metrics: config.MetricsConfig{EnableHost: true}}, res, scheduling, dynconfig)
			mockHost := resource.NewHost(
				mockRawHost.ID, mockRawHost.IP, mockRawHost.Hostname,
				mockRawHost.Port, mockRawHost.DownloadPort, mockRawHost.Type)
			mockTask := resource.NewTask(mockTaskID, mockTaskURL, mockTaskTag, mockTaskApplication, commonv2.TaskType_STANDARD, mockTaskFilteredQueryParams, mockTaskHeader, mockTaskBackToSourceLimit, resource.WithDigest(mockTaskDigest), resource.WithPieceLength(mockTaskPieceLength))
			peer := resource.NewPeer(mockSeedPeerID, mockTask, mockHost)
			child := resource.NewPeer(mockPeerID, mockTask, mockHost)

			tc.mock(peer, child, scheduling.EXPECT())
			svc.handlePeerFailure(context.Background(), peer)
			tc.expect(t, peer, child)
		})
	}
}

func TestServiceV1_handleTaskSuccess(t *testing.T) {
	tests := []struct {
		name   string
		result *schedulerv1.PeerResult
		mock   func(task *resource.Task)
		expect func(t *testing.T, task *resource.Task)
	}{
		{
			name:   "task state is TaskStatePending",
			result: &schedulerv1.PeerResult{},
			mock: func(task *resource.Task) {
				task.FSM.SetState(resource.TaskStatePending)
			},
			expect: func(t *testing.T, task *resource.Task) {
				assert := assert.New(t)
				assert.True(task.FSM.Is(resource.TaskStatePending))
			},
		},
		{
			name:   "task state is TaskStateSucceeded",
			result: &schedulerv1.PeerResult{},
			mock: func(task *resource.Task) {
				task.FSM.SetState(resource.TaskStateSucceeded)
			},
			expect: func(t *testing.T, task *resource.Task) {
				assert := assert.New(t)
				assert.True(task.FSM.Is(resource.TaskStateSucceeded))
			},
		},
		{
			name: "task state is TaskStateRunning",
			result: &schedulerv1.PeerResult{
				TotalPieceCount: 1,
				ContentLength:   1,
			},
			mock: func(task *resource.Task) {
				task.FSM.SetState(resource.TaskStateRunning)
			},
			expect: func(t *testing.T, task *resource.Task) {
				assert := assert.New(t)
				assert.True(task.FSM.Is(resource.TaskStateSucceeded))
				assert.Equal(task.TotalPieceCount.Load(), int32(1))
				assert.Equal(task.ContentLength.Load(), int64(1))
			},
		},
		{
			name: "task state is TaskStateFailed",
			result: &schedulerv1.PeerResult{
				TotalPieceCount: 1,
				ContentLength:   1,
			},
			mock: func(task *resource.Task) {
				task.FSM.SetState(resource.TaskStateFailed)
			},
			expect: func(t *testing.T, task *resource.Task) {
				assert := assert.New(t)
				assert.True(task.FSM.Is(resource.TaskStateSucceeded))
				assert.Equal(task.TotalPieceCount.Load(), int32(1))
				assert.Equal(task.ContentLength.Load(), int64(1))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			scheduling := mocks.NewMockScheduling(ctl)
			res := resource.NewMockResource(ctl)
			dynconfig := configmocks.NewMockDynconfigInterface(ctl)
			svc := NewV1(&config.Config{Scheduler: mockSchedulerConfig, Metrics: config.MetricsConfig{EnableHost: true}}, res, scheduling, dynconfig)
			task := resource.NewTask(mockTaskID, mockTaskURL, mockTaskTag, mockTaskApplication, commonv2.TaskType_STANDARD, mockTaskFilteredQueryParams, mockTaskHeader, mockTaskBackToSourceLimit, resource.WithDigest(mockTaskDigest), resource.WithPieceLength(mockTaskPieceLength))

			tc.mock(task)
			svc.handleTaskSuccess(context.Background(), task, tc.result)
			tc.expect(t, task)
		})
	}
}

func TestServiceV1_handleTaskFail(t *testing.T) {
	rst := status.Newf(codes.Aborted, "response is not valid")
	st, err := rst.WithDetails(&errordetailsv1.SourceError{Temporary: false})
	if err != nil {
		t.Fatal(err)
	}

	rtst := status.Newf(codes.Aborted, "response is not valid")
	tst, err := rtst.WithDetails(&errordetailsv1.SourceError{Temporary: true})
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name            string
		backToSourceErr *errordetailsv1.SourceError
		seedPeerErr     error
		mock            func(task *resource.Task)
		expect          func(t *testing.T, task *resource.Task)
	}{
		{
			name: "task state is TaskStatePending",
			mock: func(task *resource.Task) {
				task.FSM.SetState(resource.TaskStatePending)
			},
			expect: func(t *testing.T, task *resource.Task) {
				assert := assert.New(t)
				assert.True(task.FSM.Is(resource.TaskStatePending))
			},
		},
		{
			name: "task state is TaskStateSucceeded",
			mock: func(task *resource.Task) {
				task.FSM.SetState(resource.TaskStateSucceeded)
			},
			expect: func(t *testing.T, task *resource.Task) {
				assert := assert.New(t)
				assert.True(task.FSM.Is(resource.TaskStateSucceeded))
			},
		},
		{
			name: "task state is TaskStateRunning",
			mock: func(task *resource.Task) {
				task.FSM.SetState(resource.TaskStateRunning)
			},
			expect: func(t *testing.T, task *resource.Task) {
				assert := assert.New(t)
				assert.True(task.FSM.Is(resource.TaskStateFailed))
			},
		},
		{
			name: "task state is TaskStateFailed",
			mock: func(task *resource.Task) {
				task.FSM.SetState(resource.TaskStateFailed)
			},
			expect: func(t *testing.T, task *resource.Task) {
				assert := assert.New(t)
				assert.True(task.FSM.Is(resource.TaskStateFailed))
			},
		},
		{
			name:            "peer back-to-source fails due to an unrecoverable error",
			backToSourceErr: &errordetailsv1.SourceError{Temporary: false},
			mock: func(task *resource.Task) {
				task.FSM.SetState(resource.TaskStateRunning)
			},
			expect: func(t *testing.T, task *resource.Task) {
				assert := assert.New(t)
				assert.True(task.FSM.Is(resource.TaskStateFailed))
				assert.Equal(task.PeerFailedCount.Load(), int32(0))
			},
		},
		{
			name:            "peer back-to-source fails due to an temporary error",
			backToSourceErr: &errordetailsv1.SourceError{Temporary: true},
			mock: func(task *resource.Task) {
				task.FSM.SetState(resource.TaskStateRunning)
			},
			expect: func(t *testing.T, task *resource.Task) {
				assert := assert.New(t)
				assert.True(task.FSM.Is(resource.TaskStateFailed))
			},
		},
		{
			name:        "seed peer back-to-source fails due to an unrecoverable error",
			seedPeerErr: st.Err(),
			mock: func(task *resource.Task) {
				task.FSM.SetState(resource.TaskStateRunning)
			},
			expect: func(t *testing.T, task *resource.Task) {
				assert := assert.New(t)
				assert.True(task.FSM.Is(resource.TaskStateFailed))
				assert.Equal(task.PeerFailedCount.Load(), int32(0))
			},
		},
		{
			name:        "seed peer back-to-source fails due to an temporary error",
			seedPeerErr: tst.Err(),
			mock: func(task *resource.Task) {
				task.FSM.SetState(resource.TaskStateRunning)
			},
			expect: func(t *testing.T, task *resource.Task) {
				assert := assert.New(t)
				assert.True(task.FSM.Is(resource.TaskStateFailed))
				assert.Equal(task.PeerFailedCount.Load(), int32(0))
			},
		},
		{
			name: "number of failed peers in the task is greater than FailedPeerCountLimit",
			mock: func(task *resource.Task) {
				task.FSM.SetState(resource.TaskStateRunning)
				task.PeerFailedCount.Store(201)
			},
			expect: func(t *testing.T, task *resource.Task) {
				assert := assert.New(t)
				assert.True(task.FSM.Is(resource.TaskStateFailed))
				assert.Equal(task.PeerFailedCount.Load(), int32(0))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			scheduling := mocks.NewMockScheduling(ctl)
			res := resource.NewMockResource(ctl)
			dynconfig := configmocks.NewMockDynconfigInterface(ctl)
			svc := NewV1(&config.Config{Scheduler: mockSchedulerConfig, Metrics: config.MetricsConfig{EnableHost: true}}, res, scheduling, dynconfig)
			task := resource.NewTask(mockTaskID, mockTaskURL, mockTaskTag, mockTaskApplication, commonv2.TaskType_STANDARD, mockTaskFilteredQueryParams, mockTaskHeader, mockTaskBackToSourceLimit, resource.WithDigest(mockTaskDigest), resource.WithPieceLength(mockTaskPieceLength))

			tc.mock(task)
			svc.handleTaskFailure(context.Background(), task, tc.backToSourceErr, tc.seedPeerErr)
			tc.expect(t, task)
		})
	}
}
