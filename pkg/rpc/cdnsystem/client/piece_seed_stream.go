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
	"github.com/dragonflyoss/Dragonfly2/pkg/rpc/base"
	"github.com/dragonflyoss/Dragonfly2/pkg/rpc/cdnsystem"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"sync"
	"time"
)

type pieceSeedStream struct {
	sc         *seederClient
	ctx        context.Context
	sr         *cdnsystem.SeedRequest
	opts       []grpc.CallOption
	client     cdnsystem.SeederClient 			  // client for one target
	nextNum    int
	target     string
	stream     cdnsystem.Seeder_ObtainSeedsClient // stream for one client
	begin      time.Time
	onceFinish sync.Once
	rpc.RetryMeta
}

func newPieceSeedStream(sc *seederClient, ctx context.Context, sr *cdnsystem.SeedRequest, opts []grpc.CallOption) (*pieceSeedStream, error) {
	pss := &pieceSeedStream{
		sc:   sc,
		ctx:  ctx,
		sr:   sr,
		opts: opts,

		begin: time.Now(),

		RetryMeta: rpc.RetryMeta{
			MaxAttempts: 5,
			InitBackoff: 0.5,
			MaxBackOff:  4.0,
		},
	}

	xc, target, nextNum := sc.GetClientSafely()
	pss.client, pss.target, pss.nextNum = xc.(cdnsystem.SeederClient), target, nextNum

	if err := pss.initStream(); err != nil {
		return nil, err
	} else {
		return pss, nil
	}
}

func (pss *pieceSeedStream) recv() (ps *cdnsystem.PieceSeed, err error) {
	if ps, err = pss.stream.Recv(); err != nil {
		ps, err = pss.retryRecv(err)
	}

	if err != nil || ps.Done {
		pss.onceFinish.Do(func() {
			var last *cdnsystem.PieceSeed
			if err != nil {
				last = &cdnsystem.PieceSeed{State: base.NewState(base.Code_UNKNOWN_ERROR, err.Error())}
			} else {
				last = ps
			}
			statSeedFinish(last, pss.sr.TaskId, pss.sr.Url, pss.begin)
		})
	}

	return
}

func (pss *pieceSeedStream) initStream() error {
	stream, err := rpc.ExecuteWithRetry(func() (interface{}, error) {
		return pss.client.ObtainSeeds(pss.ctx, pss.sr, pss.opts...)
	}, pss.InitBackoff, pss.MaxBackOff, pss.MaxAttempts)

	if err != nil {
		err = pss.replaceClient(err)
	} else {
		pss.stream = stream.(cdnsystem.Seeder_ObtainSeedsClient)
		pss.Times = 1
	}

	statSeedStart(pss.sr, pss.target, err == nil)

	return err
}

func (pss *pieceSeedStream) retryRecv(cause error) (*cdnsystem.PieceSeed, error) {
	code := status.Code(cause)
	if code == codes.DeadlineExceeded || code == codes.Aborted {
		return nil, cause
	}

	var needMig = code == codes.FailedPrecondition
	if !needMig {
		if cause = pss.replaceStream(); cause != nil {
			needMig = true
		}
	}

	if needMig {
		if err := pss.replaceClient(cause); err != nil {
			return nil, err
		}
	}

	return pss.recv()
}

func (pss *pieceSeedStream) replaceStream() error {
	if pss.Times >= pss.MaxAttempts {
		return errors.New("times of replacing stream reaches limit")
	}

	stream, err := rpc.ExecuteWithRetry(func() (interface{}, error) {
		return pss.client.ObtainSeeds(pss.ctx, pss.sr, pss.opts...)
	}, pss.InitBackoff, pss.MaxBackOff, pss.MaxAttempts)

	if err == nil {
		pss.stream = stream.(cdnsystem.Seeder_ObtainSeedsClient)
		pss.Times++
	}

	return err
}

func (pss *pieceSeedStream) replaceClient(cause error) error {
	if err := pss.sc.TryMigrate(pss.nextNum, cause); err != nil {
		return err
	}

	xc, target, nextNum := pss.sc.GetClientSafely()
	pss.client, pss.target, pss.nextNum = xc.(cdnsystem.SeederClient), target, nextNum

	stream, err := rpc.ExecuteWithRetry(func() (interface{}, error) {
		return pss.client.ObtainSeeds(pss.ctx, pss.sr, pss.opts...)
	}, pss.InitBackoff, pss.MaxBackOff, pss.MaxAttempts)

	if err != nil {
		return pss.replaceClient(err)
	} else {
		pss.stream = stream.(cdnsystem.Seeder_ObtainSeedsClient)
		pss.Times = 1
	}

	return err
}

func statSeedStart(sr *cdnsystem.SeedRequest, target string, success bool) {
	// logger.StatSeedLogger.Info("trigger seed making",
	// 	zap.Bool("success", success),
	// 	zap.String("taskId", sr.TaskId),
	// 	zap.String("url", sr.Url),
	// 	zap.String("seeder", target))
}

func statSeedFinish(last *cdnsystem.PieceSeed, taskId string, url string, begin time.Time) {
	// logger.StatSeedLogger.Info("seed making finish",
	// 	zap.Bool("success", last.State.Success),
	// 	zap.String("taskId", taskId),
	// 	zap.String("url", url),
	// 	//zap.String("seeder", last.SeedAddr),
	// 	zap.Int64("cost", time.Now().Sub(begin).Milliseconds()),
	// 	zap.Int64("contentLength", last.ContentLength),
	// 	zap.Int("code", int(last.State.Code)))
}
