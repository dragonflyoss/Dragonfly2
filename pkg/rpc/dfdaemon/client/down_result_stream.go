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
	"d7y.io/dragonfly/v2/pkg/rpc"
	"d7y.io/dragonfly/v2/pkg/rpc/dfdaemon"
	"errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type downResultStream struct {
	dc      *daemonClient
	ctx     context.Context
	hashKey string
	req     *dfdaemon.DownRequest
	opts    []grpc.CallOption
	// stream for one client
	stream dfdaemon.Daemon_DownloadClient
	failedServers []string
	rpc.RetryMeta
}

func newDownResultStream(dc *daemonClient, ctx context.Context, hashKey string, req *dfdaemon.DownRequest, opts []grpc.CallOption) (*downResultStream, error) {
	drs := &downResultStream{
		dc:      dc,
		ctx:     ctx,
		hashKey: hashKey,
		req:     req,
		opts:    opts,

		RetryMeta: rpc.RetryMeta{
			MaxAttempts: 5,
			MaxBackOff:  5.0,
			InitBackoff: 1.0,
		},
	}

	if err := drs.initStream(); err != nil {
		return nil, err
	} else {
		return drs, nil
	}
}

func (drs *downResultStream) initStream() error {
	stream, err := rpc.ExecuteWithRetry(func() (interface{}, error) {
		return drs.dc.getDaemonClient(drs.hashKey).Download(drs.ctx, drs.req, drs.opts...)
	}, drs.InitBackoff, drs.MaxBackOff, drs.MaxAttempts, nil)

	if err != nil {
		err = drs.replaceClient(err)
	} else {
		drs.stream = stream.(dfdaemon.Daemon_DownloadClient)
		drs.StreamTimes = 1
	}

	return err
}

func (drs *downResultStream) recv() (dr *dfdaemon.DownResult, err error) {
	drs.dc.UpdateAccessNodeMap(drs.hashKey)
	if dr, err = drs.stream.Recv(); err != nil {
		dr, err = drs.retryRecv(err)
	}

	return
}

func (drs *downResultStream) retryRecv(cause error) (*dfdaemon.DownResult, error) {
	code := status.Code(cause)
	if code == codes.DeadlineExceeded {
		return nil, cause
	}

	if err := drs.replaceStream(cause); err != nil {
		if err := drs.replaceClient(cause); err != nil {
			return nil, cause
		}
	}

	return drs.recv()
}

func (drs *downResultStream) replaceStream(cause error) error {
	if drs.StreamTimes >= drs.MaxAttempts {
		return errors.New("times of replacing stream reaches limit")
	}

	stream, err := rpc.ExecuteWithRetry(func() (interface{}, error) {
		return drs.dc.getDaemonClient(drs.hashKey).Download(drs.ctx, drs.req, drs.opts...)
	}, drs.InitBackoff, drs.MaxBackOff, drs.MaxAttempts, cause)

	if err == nil {
		drs.stream = stream.(dfdaemon.Daemon_DownloadClient)
		drs.StreamTimes++
	}

	return err
}

func (drs *downResultStream) replaceClient(cause error) error {
	if preNode, err := drs.dc.TryMigrate(drs.hashKey, cause, drs.failedServers); err != nil {
		return err
	} else {
		drs.failedServers = append(drs.failedServers, preNode)
	}

	stream, err := rpc.ExecuteWithRetry(func() (interface{}, error) {
		return drs.dc.getDaemonClient(drs.hashKey).Download(drs.ctx, drs.req, drs.opts...)
	}, drs.InitBackoff, drs.MaxBackOff, drs.MaxAttempts, cause)

	if err != nil {
		err = drs.replaceClient(cause)
	} else {
		drs.stream = stream.(dfdaemon.Daemon_DownloadClient)
		drs.StreamTimes = 1
	}

	return err
}
