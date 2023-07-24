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

import (
	"context"

	"google.golang.org/grpc"

	commonv1 "d7y.io/api/v2/pkg/apis/common/v1"
	schedulerv1 "d7y.io/api/v2/pkg/apis/scheduler/v1"

	"d7y.io/dragonfly/v2/internal/dferrors"
	"d7y.io/dragonfly/v2/pkg/dfnet"
)

// when scheduler is not available, use dummySchedulerClient to back source
type dummySchedulerClient struct {
}

func (d *dummySchedulerClient) RegisterPeerTask(ctx context.Context, request *schedulerv1.PeerTaskRequest, option ...grpc.CallOption) (*schedulerv1.RegisterResult, error) {
	panic("should not call this function")
}

func (d *dummySchedulerClient) ReportPieceResult(ctx context.Context, request *schedulerv1.PeerTaskRequest, option ...grpc.CallOption) (schedulerv1.Scheduler_ReportPieceResultClient, error) {
	return &dummyPeerPacketStream{}, nil
}

func (d *dummySchedulerClient) ReportPeerResult(ctx context.Context, result *schedulerv1.PeerResult, option ...grpc.CallOption) error {
	return nil
}

func (d *dummySchedulerClient) LeaveTask(ctx context.Context, target *schedulerv1.PeerTarget, option ...grpc.CallOption) error {
	return nil
}

func (d *dummySchedulerClient) AnnounceHost(context.Context, *schedulerv1.AnnounceHostRequest, ...grpc.CallOption) error {
	return nil
}

func (d *dummySchedulerClient) LeaveHost(ctx context.Context, target *schedulerv1.LeaveHostRequest, option ...grpc.CallOption) error {
	return nil
}

func (d *dummySchedulerClient) StatTask(ctx context.Context, request *schedulerv1.StatTaskRequest, option ...grpc.CallOption) (*schedulerv1.Task, error) {
	panic("should not call this function")
}

func (d *dummySchedulerClient) AnnounceTask(ctx context.Context, request *schedulerv1.AnnounceTaskRequest, option ...grpc.CallOption) error {
	panic("should not call this function")
}

func (d *dummySchedulerClient) SyncProbes(ctx context.Context, req *schedulerv1.SyncProbesRequest, opts ...grpc.CallOption) (schedulerv1.Scheduler_SyncProbesClient, error) {
	panic("should not call this function")
}

func (d *dummySchedulerClient) Close() error {
	return nil
}

func (d *dummySchedulerClient) UpdateState(addrs []dfnet.NetAddr) {
}

func (d *dummySchedulerClient) GetState() []dfnet.NetAddr {
	return nil
}

type dummyPeerPacketStream struct {
	grpc.ClientStream
}

func (d *dummyPeerPacketStream) Recv() (*schedulerv1.PeerPacket, error) {
	// TODO set commonv1.Code_SchedNeedBackSource in *scheduler.PeerPacket instead of error
	return nil, dferrors.New(commonv1.Code_SchedNeedBackSource, "")
}

func (d *dummyPeerPacketStream) Send(pr *schedulerv1.PieceResult) error {
	return nil
}

func (d *dummyPeerPacketStream) CloseSend() error {
	return nil
}
