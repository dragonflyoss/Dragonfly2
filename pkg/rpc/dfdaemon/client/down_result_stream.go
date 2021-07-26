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
	"io"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/rpc"
	"d7y.io/dragonfly/v2/pkg/rpc/dfdaemon"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type DownResultStream struct {
	dc      *daemonClient
	ctx     context.Context
	hashKey string
	req     *dfdaemon.DownRequest
	opts    []grpc.CallOption
	// stream for one client
	stream        dfdaemon.Daemon_DownloadClient
	failedServers []string
	rpc.RetryMeta
}

func newDownResultStream(ctx context.Context, dc *daemonClient, hashKey string, req *dfdaemon.DownRequest, opts []grpc.CallOption) (*DownResultStream, error) {
	drs := &DownResultStream{
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
	}
	return drs, nil
}

func (drs *DownResultStream) initStream() error {
	stream, err := rpc.ExecuteWithRetry(func() (interface{}, error) {
		client, _, err := drs.dc.getDaemonClient(drs.hashKey, false)
		if err != nil {
			return nil, err
		}
		return client.Download(drs.ctx, drs.req, drs.opts...)
	}, drs.InitBackoff, drs.MaxBackOff, drs.MaxAttempts, nil)
	if err == nil {
		drs.stream = stream.(dfdaemon.Daemon_DownloadClient)
		drs.StreamTimes = 1
	}
	if err != nil {
		err = drs.replaceClient(err)
	}

	return err
}

func (drs *DownResultStream) Recv() (dr *dfdaemon.DownResult, err error) {
	defer func() {
		if dr != nil {
			if dr.TaskId != drs.hashKey {
				logger.WithTaskAndPeerID(dr.TaskId, dr.PeerId).Warnf("down result stream correct taskId from %s to %s", drs.hashKey, dr.TaskId)
				drs.dc.Connection.CorrectKey2NodeRelation(drs.hashKey, dr.TaskId)
				drs.hashKey = dr.TaskId
			}
		}
	}()
	drs.dc.UpdateAccessNodeMapByHashKey(drs.hashKey)
	if dr, err = drs.stream.Recv(); err != nil && err != io.EOF {
		dr, err = drs.retryRecv(err)
	}

	return
}

func (drs *DownResultStream) retryRecv(cause error) (*dfdaemon.DownResult, error) {
	if status.Code(cause) == codes.DeadlineExceeded {
		return nil, cause
	}

	if err := drs.replaceStream(cause); err != nil {
		if err := drs.replaceClient(cause); err != nil {
			return nil, cause
		}
	}

	return drs.Recv()
}

func (drs *DownResultStream) replaceStream(cause error) error {
	if drs.StreamTimes >= drs.MaxAttempts {
		return errors.New("times of replacing stream reaches limit")
	}

	stream, err := rpc.ExecuteWithRetry(func() (interface{}, error) {
		client, _, err := drs.dc.getDaemonClient(drs.hashKey, true)
		if err != nil {
			return nil, err
		}
		return client.Download(drs.ctx, drs.req, drs.opts...)
	}, drs.InitBackoff, drs.MaxBackOff, drs.MaxAttempts, cause)
	if err == nil {
		drs.stream = stream.(dfdaemon.Daemon_DownloadClient)
		drs.StreamTimes++
	}
	return err
}

func (drs *DownResultStream) replaceClient(cause error) error {
	preNode, err := drs.dc.TryMigrate(drs.hashKey, cause, drs.failedServers)
	if err != nil {
		return err
	}
	drs.failedServers = append(drs.failedServers, preNode)

	stream, err := rpc.ExecuteWithRetry(func() (interface{}, error) {
		client, _, err := drs.dc.getDaemonClient(drs.hashKey, true)
		if err != nil {
			return nil, err
		}
		return client.Download(drs.ctx, drs.req, drs.opts...)
	}, drs.InitBackoff, drs.MaxBackOff, drs.MaxAttempts, cause)
	if err == nil {
		drs.stream = stream.(dfdaemon.Daemon_DownloadClient)
		drs.StreamTimes = 1
	}
	if err != nil {
		err = drs.replaceClient(cause)
	}
	return err
}
