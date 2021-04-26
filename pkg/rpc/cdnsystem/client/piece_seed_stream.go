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

	logger "d7y.io/dragonfly/v2/pkg/dflog"
	"d7y.io/dragonfly/v2/pkg/rpc"
	"d7y.io/dragonfly/v2/pkg/rpc/cdnsystem"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type PieceSeedStream struct {
	// client for one target
	sc      *cdnClient
	ctx     context.Context
	hashKey string
	sr      *cdnsystem.SeedRequest
	opts    []grpc.CallOption
	// stream for one client
	stream cdnsystem.Seeder_ObtainSeedsClient
	// server list which cannot serve
	failedServers []string
	rpc.RetryMeta
}

func newPieceSeedStream(sc *cdnClient, ctx context.Context, hashKey string, sr *cdnsystem.SeedRequest, opts []grpc.CallOption) (*PieceSeedStream, error) {
	pss := &PieceSeedStream{
		sc:      sc,
		ctx:     ctx,
		hashKey: hashKey,
		sr:      sr,
		opts:    opts,
		RetryMeta: rpc.RetryMeta{
			MaxAttempts: 5,
			InitBackoff: 0.5,
			MaxBackOff:  4.0,
		},
	}

	if err := pss.initStream(); err != nil {
		return nil, err
	} else {
		return pss, nil
	}
}

func (pss *PieceSeedStream) initStream() error {
	stream, err := rpc.ExecuteWithRetry(func() (interface{}, error) {
		client, cdnServerNode, err := pss.sc.getCdnClient(pss.hashKey, false)
		if err != nil {
			return nil, err
		}
		logger.WithTaskID(pss.hashKey).Infof("invoke cdn node %s ObtainSeeds", cdnServerNode)
		return client.ObtainSeeds(pss.ctx, pss.sr, pss.opts...)
	}, pss.InitBackoff, pss.MaxBackOff, pss.MaxAttempts, nil)
	if err == nil {
		pss.stream = stream.(cdnsystem.Seeder_ObtainSeedsClient)
		pss.StreamTimes = 1
	}
	if err != nil {
		err = pss.replaceClient(pss.hashKey, err)
	}
	return err
}

func (pss *PieceSeedStream) Recv() (ps *cdnsystem.PieceSeed, err error) {
	pss.sc.UpdateAccessNodeMap(pss.hashKey)
	if ps, err = pss.stream.Recv(); err != nil && err != io.EOF {
		ps, err = pss.retryRecv(err)
	}
	return
}

func (pss *PieceSeedStream) retryRecv(cause error) (*cdnsystem.PieceSeed, error) {
	if status.Code(cause) == codes.DeadlineExceeded {
		return nil, cause
	}

	if err := pss.replaceStream(pss.hashKey, cause); err != nil {
		if err := pss.replaceClient(pss.hashKey, cause); err != nil {
			return nil, cause
		}
	}

	return pss.Recv()
}

func (pss *PieceSeedStream) replaceStream(key string, cause error) error {
	if pss.StreamTimes >= pss.MaxAttempts {
		return errors.New("times of replacing stream reaches limit")
	}

	stream, err := rpc.ExecuteWithRetry(func() (interface{}, error) {
		client, _, err := pss.sc.getCdnClient(key, true)
		if err != nil {
			return nil, err
		}
		return client.ObtainSeeds(pss.ctx, pss.sr, pss.opts...)
	}, pss.InitBackoff, pss.MaxBackOff, pss.MaxAttempts, cause)
	if err == nil {
		pss.stream = stream.(cdnsystem.Seeder_ObtainSeedsClient)
		pss.StreamTimes++
	}
	return err
}

func (pss *PieceSeedStream) replaceClient(key string, cause error) error {
	if preNode, err := pss.sc.TryMigrate(key, cause, pss.failedServers); err != nil {
		return err
	} else {
		pss.failedServers = append(pss.failedServers, preNode)
	}

	stream, err := rpc.ExecuteWithRetry(func() (interface{}, error) {
		client, _, err := pss.sc.getCdnClient(key, true)
		if err != nil {
			return nil, err
		}
		return client.ObtainSeeds(pss.ctx, pss.sr, pss.opts...)
	}, pss.InitBackoff, pss.MaxBackOff, pss.MaxAttempts, cause)
	if err == nil {
		pss.stream = stream.(cdnsystem.Seeder_ObtainSeedsClient)
		pss.StreamTimes = 1
	}
	if err != nil {
		err = pss.replaceClient(key, cause)
	}
	return err
}
