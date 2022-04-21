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

	"d7y.io/dragonfly/v2/internal/dferrors"
	"d7y.io/dragonfly/v2/pkg/dfnet"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	"d7y.io/dragonfly/v2/pkg/rpc/scheduler"
	schedulerclient "d7y.io/dragonfly/v2/pkg/rpc/scheduler/client"
)

// when scheduler is not available, use dummySchedulerClient to back source
type dummySchedulerClient struct {
}

func (d *dummySchedulerClient) RegisterPeerTask(ctx context.Context, request *scheduler.PeerTaskRequest, option ...grpc.CallOption) (*scheduler.RegisterResult, error) {
	panic("should not call this function")
}

func (d *dummySchedulerClient) ReportPieceResult(ctx context.Context, s string, request *scheduler.PeerTaskRequest, option ...grpc.CallOption) (schedulerclient.PeerPacketStream, error) {
	return &dummyPeerPacketStream{}, nil
}

func (d *dummySchedulerClient) ReportPeerResult(ctx context.Context, result *scheduler.PeerResult, option ...grpc.CallOption) error {
	return nil
}

func (d *dummySchedulerClient) LeaveTask(ctx context.Context, target *scheduler.PeerTarget, option ...grpc.CallOption) error {
	return nil
}

func (d *dummySchedulerClient) StatTask(ctx context.Context, request *scheduler.StatTaskRequest, option ...grpc.CallOption) (*scheduler.Task, error) {
	panic("should not call this function")
}

func (d *dummySchedulerClient) AnnounceTask(ctx context.Context, request *scheduler.AnnounceTaskRequest, option ...grpc.CallOption) error {
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
}

func (d *dummyPeerPacketStream) Recv() (pp *scheduler.PeerPacket, err error) {
	// TODO set base.Code_SchedNeedBackSource in *scheduler.PeerPacket instead of error
	return nil, dferrors.New(base.Code_SchedNeedBackSource, "")
}

func (d *dummyPeerPacketStream) Send(pr *scheduler.PieceResult) (err error) {
	return nil
}
