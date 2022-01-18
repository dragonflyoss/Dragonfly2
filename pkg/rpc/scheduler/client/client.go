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

//go:generate mockgen -destination ./mocks/mock_client.go -package mocks d7y.io/dragonfly/v2/pkg/rpc/scheduler/client SchedulerClient

package client

import (
	"context"
	"fmt"

	"d7y.io/dragonfly/v2/internal/dfnet"
	"d7y.io/dragonfly/v2/pkg/idgen"
	"d7y.io/dragonfly/v2/pkg/rpc"
	"d7y.io/dragonfly/v2/pkg/rpc/pickreq"
	"d7y.io/dragonfly/v2/pkg/rpc/scheduler"
	grpc_retry "github.com/grpc-ecosystem/go-grpc-middleware/retry"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
)

var _ SchedulerClient = (*schedulerClient)(nil)

func GetClientByAddrs(addrs []dfnet.NetAddr, opts ...grpc.DialOption) (SchedulerClient, error) {
	if len(addrs) == 0 {
		return nil, errors.New("address list of scheduler is empty")
	}

	resolver := rpc.NewD7yResolver("scheduler", addrs)

	dialOpts := append(append(append(
		rpc.DefaultClientOpts,
		grpc.WithDefaultServiceConfig(fmt.Sprintf(`{"loadBalancingPolicy": "%s"}`, rpc.D7yBalancerPolicy))),
		grpc.WithResolvers(resolver)),
		opts...)

	// "scheduler.Scheduler" is the scheduler._Scheduler_serviceDesc.ServiceName
	conn, err := grpc.Dial(
		fmt.Sprintf("%s:///%s", "scheduler", "scheduler.Scheduler"),
		dialOpts...,
	)
	if err != nil {
		return nil, err
	}

	sc := &schedulerClient{
		cc:              conn,
		schedulerClient: scheduler.NewSchedulerClient(conn),
		resolver:        resolver,
	}
	return sc, nil
}

// SchedulerClient see scheduler.SchedulerClient
type SchedulerClient interface {
	// RegisterPeerTask register peer task to scheduler
	RegisterPeerTask(ctx context.Context, in *scheduler.PeerTaskRequest, opts ...grpc.CallOption) (*scheduler.RegisterResult, error)
	// ReportPieceResult IsMigrating of ptr will be set to true
	ReportPieceResult(ctx context.Context, taskID string, ptr *scheduler.PeerTaskRequest, opts ...grpc.CallOption) (scheduler.Scheduler_ReportPieceResultClient, error)

	ReportPeerResult(ctx context.Context, in *scheduler.PeerResult, opts ...grpc.CallOption) error

	LeaveTask(ctx context.Context, in *scheduler.PeerTarget, opts ...grpc.CallOption) error

	UpdateAddresses(addrs []dfnet.NetAddr) error

	Close() error
}

type schedulerClient struct {
	schedulerClient scheduler.SchedulerClient
	cc              *grpc.ClientConn
	resolver        *rpc.D7yResolver
}

func (sc *schedulerClient) RegisterPeerTask(ctx context.Context, ptr *scheduler.PeerTaskRequest, opts ...grpc.CallOption) (*scheduler.RegisterResult, error) {
	ctx = pickreq.NewContext(ctx, &pickreq.PickRequest{
		HashKey: idgen.TaskID(ptr.Url, ptr.UrlMeta),
	})
	return sc.schedulerClient.RegisterPeerTask(ctx, ptr, opts...)
}

func (sc *schedulerClient) ReportPieceResult(ctx context.Context, taskID string, ptr *scheduler.PeerTaskRequest, opts ...grpc.CallOption) (scheduler.Scheduler_ReportPieceResultClient, error) {
	ctx = pickreq.NewContext(ctx, &pickreq.PickRequest{
		HashKey: taskID,
		IsStick: true,
	})

	reportStream, err := sc.schedulerClient.ReportPieceResult(ctx, append(opts, grpc_retry.Disable())...)
	if err == nil {
		err = reportStream.Send(scheduler.NewZeroPieceResult(taskID, ptr.PeerId))
	}
	return reportStream, err
}

func (sc *schedulerClient) ReportPeerResult(ctx context.Context, pr *scheduler.PeerResult, opts ...grpc.CallOption) error {
	ctx = pickreq.NewContext(ctx, &pickreq.PickRequest{
		HashKey: pr.TaskId,
		IsStick: true,
	})
	_, err := sc.schedulerClient.ReportPeerResult(ctx, pr, opts...)
	// TODO if fail then report to manager
	return err
}

func (sc *schedulerClient) LeaveTask(ctx context.Context, pt *scheduler.PeerTarget, opts ...grpc.CallOption) (err error) {
	ctx = pickreq.NewContext(ctx, &pickreq.PickRequest{
		HashKey: pt.TaskId,
		IsStick: true,
	})
	_, err = sc.schedulerClient.LeaveTask(ctx, pt, opts...)
	return err
}

func (sc *schedulerClient) UpdateAddresses(addrs []dfnet.NetAddr) error {
	return sc.resolver.UpdateAddresses(addrs)
}

func (sc *schedulerClient) Close() error {
	return sc.cc.Close()
}
