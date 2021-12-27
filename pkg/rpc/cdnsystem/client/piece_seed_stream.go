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

	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"d7y.io/dragonfly/v2/internal/dferrors"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/rpc"
	"d7y.io/dragonfly/v2/pkg/rpc/cdnsystem"
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
	rpc.RetryMeta
}

func newPieceSeedStream(ctx context.Context, sc *cdnClient, hashKey string, sr *cdnsystem.SeedRequest, opts []grpc.CallOption) (*PieceSeedStream, error) {
	pss := &PieceSeedStream{
		sc:      sc,
		ctx:     ctx,
		hashKey: hashKey,
		sr:      sr,
		opts:    opts,
		RetryMeta: rpc.RetryMeta{
			MaxAttempts: 3,
			InitBackoff: 0.2,
			MaxBackOff:  2.0,
		},
	}

	if err := pss.initStream(); err != nil {
		return nil, err
	}
	return pss, nil
}

func (pss *PieceSeedStream) initStream() error {
	client, err := pss.sc.getCdnClient()
	if err != nil {
		return err
	}
	stream, err := client.ObtainSeeds(context.WithValue(pss.ctx, rpc.PickKey{}, &rpc.PickReq{Key: pss.hashKey, Attempt: pss.StreamTimes + 1}), pss.sr, pss.opts...)
	if err != nil {
		if errors.Cause(err) == dferrors.ErrNoCandidateNode {
			return errors.Wrapf(err, "get grpc server instance failed")
		}
		logger.WithTaskID(pss.hashKey).Errorf("initStream: invoke cdn ObtainSeeds failed: %v", err)
		return pss.replaceClient(pss.hashKey, err)
	}
	pss.stream = stream
	pss.StreamTimes = 1
	return nil
}

func (pss *PieceSeedStream) Recv() (ps *cdnsystem.PieceSeed, err error) {
	return pss.stream.Recv()
}

func (pss *PieceSeedStream) retryRecv(cause error) (*cdnsystem.PieceSeed, error) {
	if status.Code(cause) == codes.DeadlineExceeded || status.Code(cause) == codes.Canceled {
		return nil, cause
	}

	if err := pss.replaceStream(cause); err != nil {
		return nil, err
	}
	return pss.Recv()
}

func (pss *PieceSeedStream) replaceStream(cause error) error {
	if pss.StreamTimes >= pss.MaxAttempts {
		logger.WithTaskID(pss.hashKey).Info("replace stream reach max attempt")
		return cause
	}
	client, err := pss.sc.getCdnClient()
	if err != nil {
		return err
	}
	stream, err :=  client.ObtainSeeds(context.WithValue(pss.ctx, rpc.PickKey{}, &rpc.PickReq{Key: pss.hashKey, Attempt: pss.StreamTimes + 1}), pss.sr, pss.opts...)
	if err != nil {
		logger.WithTaskID(pss.hashKey).Infof("replaceStream: invoke cdn ObtainSeeds failed: %v", err)
		pss.StreamTimes++
		return pss.replaceStream(cause)
	}
	pss.stream = stream
	pss.StreamTimes++
	return nil
}

func (pss *PieceSeedStream) replaceClient(key string, cause error) error {
	client, err := pss.sc.getCdnClient()
	if err != nil {
		return err
	}
	stream, err := client.ObtainSeeds(context.WithValue(pss.ctx, rpc.PickKey{}, &rpc.PickReq{Key: pss.hashKey, Attempt: pss.StreamTimes + 1}), pss.sr, pss.opts...)
	if err != nil {
		logger.WithTaskID(pss.hashKey).Infof("replaceClient: invoke cdn ObtainSeeds failed: %v", err)
		return pss.replaceClient(key, cause)
	}
	pss.stream = stream
	pss.StreamTimes = 1
	return nil
}
