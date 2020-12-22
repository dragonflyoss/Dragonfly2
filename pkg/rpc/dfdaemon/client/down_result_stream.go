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
	"github.com/dragonflyoss/Dragonfly2/pkg/basic"
	"github.com/dragonflyoss/Dragonfly2/pkg/rpc"
	"github.com/dragonflyoss/Dragonfly2/pkg/rpc/dfdaemon"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type downResultStream struct {
	dc   *daemonClient
	ctx  context.Context
	req  *dfdaemon.DownRequest
	opts []grpc.CallOption

	// client for one target
	client  dfdaemon.DaemonClient
	nextNum int
	// stream for one client
	stream dfdaemon.Daemon_DownloadClient

	rpc.RetryMeta
}

func newDownResultStream(dc *daemonClient, ctx context.Context, req *dfdaemon.DownRequest, opts []grpc.CallOption) (*downResultStream, error) {
	drs := &downResultStream{
		dc:   dc,
		ctx:  ctx,
		req:  req,
		opts: opts,

		RetryMeta: rpc.RetryMeta{
			MaxAttempts: 5,
			MaxBackOff:  4.0,
			InitBackoff: 0.5,
		},
	}
	xc, _, nextNum := dc.GetClientSafely()
	drs.client, drs.nextNum = xc.(dfdaemon.DaemonClient), nextNum

	if err := drs.initStream(); err != nil {
		return nil, err
	} else {
		return drs, nil
	}
}

func (drs *downResultStream) recv() (dr *dfdaemon.DownResult, err error) {
	if dr, err = drs.stream.Recv(); err != nil {
		dr, err = drs.retryRecv(err)
	}
	return
}

func (drs *downResultStream) initStream() error {
	stream, err := rpc.ExecuteWithRetry(func() (interface{}, error) {
		return drs.client.Download(drs.ctx, drs.req, drs.opts...)
	}, drs.InitBackoff, drs.MaxBackOff, drs.MaxAttempts)

	if err != nil {
		err = drs.replaceClient(err)
	} else {
		drs.stream = stream.(dfdaemon.Daemon_DownloadClient)
		drs.Times = 1
	}

	return err
}

func (drs *downResultStream) retryRecv(cause error) (*dfdaemon.DownResult, error) {
	code := status.Code(cause)
	if code == codes.DeadlineExceeded || code == codes.Aborted {
		return nil, cause
	}

	var needMig = code == codes.FailedPrecondition
	if !needMig {
		if cause = drs.replaceStream(); cause != nil {
			needMig = true
		}
	}

	if needMig {
		if err := drs.replaceClient(cause); err != nil {
			return nil, err
		}
	}

	return drs.recv()
}

func (drs *downResultStream) replaceStream() error {
	if drs.Times >= drs.MaxAttempts {
		return errors.New("times of replacing stream reaches limit")
	} else if drs.dc.NetAddrs[drs.nextNum-1].Type == basic.UNIX {
		return errors.New("unix socket skips replacing stream")
	}

	stream, err := rpc.ExecuteWithRetry(func() (interface{}, error) {
		return drs.client.Download(drs.ctx, drs.req, drs.opts...)
	}, drs.InitBackoff, drs.MaxBackOff, drs.MaxAttempts)

	if err == nil {
		drs.stream = stream.(dfdaemon.Daemon_DownloadClient)
		drs.Times++
	}

	return err
}

func (drs *downResultStream) replaceClient(cause error) error {
	if err := drs.dc.TryMigrate(drs.nextNum, cause); err != nil {
		return err
	}

	xc, _, nextNum := drs.dc.GetClientSafely()
	drs.client, drs.nextNum = xc.(dfdaemon.DaemonClient), nextNum

	stream, err := rpc.ExecuteWithRetry(func() (interface{}, error) {
		return drs.client.Download(drs.ctx, drs.req, drs.opts...)
	}, drs.InitBackoff, drs.MaxBackOff, drs.MaxAttempts)

	if err != nil {
		return drs.replaceClient(err)
	} else {
		drs.stream = stream.(dfdaemon.Daemon_DownloadClient)
		drs.Times = 1
	}

	return err
}
