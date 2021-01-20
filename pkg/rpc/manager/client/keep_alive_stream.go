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

package client

import (
	"context"
	"errors"
	"github.com/dragonflyoss/Dragonfly2/pkg/rpc"
	"github.com/dragonflyoss/Dragonfly2/pkg/rpc/manager"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type configStream struct {
	mc   *managerClient
	ctx  context.Context
	opts []grpc.CallOption

	// client for one target
	client  manager.ManagerClient
	nextNum int
	// stream for one client
	stream manager.Manager_KeepAliveClient

	rpc.RetryMeta
}

func newConfigStream(mc *managerClient, ctx context.Context, opts []grpc.CallOption) (*configStream, error) {
	cs := &configStream{
		mc:   mc,
		ctx:  ctx,
		opts: opts,

		RetryMeta: rpc.RetryMeta{
			MaxAttempts: 5,
			MaxBackOff:  4.0,
			InitBackoff: 0.5,
		},
	}
	xc, _, nextNum := mc.GetClientSafely()
	cs.client, cs.nextNum = xc.(manager.ManagerClient), nextNum

	if err := cs.initStream(); err != nil {
		return nil, err
	} else {
		return cs, nil
	}
}

func (cs *configStream) send(heartReq *manager.HeartRequest) error {
	return cs.stream.Send(heartReq)
}

func (cs *configStream) closeSend() error {
	return cs.stream.CloseSend()
}

func (cs *configStream) recv() (mcr *manager.ManagementConfig, err error) {
	if mcr, err = cs.stream.Recv(); err != nil {
		mcr, err = cs.retryRecv(err)
	}
	return
}

func (cs *configStream) initStream() error {
	stream, err := rpc.ExecuteWithRetry(func() (interface{}, error) {
		return cs.client.KeepAlive(cs.ctx, cs.opts...)
	}, cs.InitBackoff, cs.MaxBackOff, cs.MaxAttempts)

	if err != nil {
		err = cs.replaceClient(err)
	} else {
		cs.stream = stream.(manager.Manager_KeepAliveClient)
		cs.Times = 1
	}

	return err
}

func (cs *configStream) retryRecv(cause error) (*manager.ManagementConfig, error) {
	code := status.Code(cause)
	if code == codes.DeadlineExceeded || code == codes.Aborted {
		return nil, cause
	}

	var needMig = code == codes.FailedPrecondition
	if !needMig {
		if cause = cs.replaceStream(); cause != nil {
			needMig = true
		}
	}

	if needMig {
		if err := cs.replaceClient(cause); err != nil {
			return nil, err
		}
	}

	return cs.recv()
}

func (cs *configStream) replaceStream() error {
	if cs.Times >= cs.MaxAttempts {
		return errors.New("times of replacing stream reaches limit")
	}

	stream, err := rpc.ExecuteWithRetry(func() (interface{}, error) {
		return cs.client.KeepAlive(cs.ctx, cs.opts...)
	}, cs.InitBackoff, cs.MaxBackOff, cs.MaxAttempts)

	if err == nil {
		cs.stream = stream.(manager.Manager_KeepAliveClient)
		cs.Times++
	}

	return err
}

func (cs *configStream) replaceClient(cause error) error {
	if err := cs.mc.TryMigrate(cs.nextNum, cause); err != nil {
		return err
	}

	xc, _, nextNum := cs.mc.GetClientSafely()
	cs.client, cs.nextNum = xc.(manager.ManagerClient), nextNum

	stream, err := rpc.ExecuteWithRetry(func() (interface{}, error) {
		return cs.client.KeepAlive(cs.ctx, cs.opts...)
	}, cs.InitBackoff, cs.MaxBackOff, cs.MaxAttempts)

	if err != nil {
		return cs.replaceClient(err)
	} else {
		cs.stream = stream.(manager.Manager_KeepAliveClient)
		cs.Times = 1
	}

	return err
}
