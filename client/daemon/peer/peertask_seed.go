/*
 *     Copyright 2022 The Dragonfly Authors
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
	"fmt"

	"go.opentelemetry.io/otel/trace"
	"golang.org/x/time/rate"

	"d7y.io/dragonfly/v2/client/clientutil"
	"d7y.io/dragonfly/v2/client/config"
	"d7y.io/dragonfly/v2/client/daemon/metrics"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/idgen"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	"d7y.io/dragonfly/v2/pkg/rpc/scheduler"
)

type SeedTaskRequest struct {
	scheduler.PeerTaskRequest
	Limit      float64
	Callsystem string
	Range      *clientutil.Range
}

// SeedTask represents a seed peer task
type SeedTask interface {
	Start(ctx context.Context) (chan *SeedTaskProgress, error)
}

type seedTask struct {
	*logger.SugaredLoggerOnWith
	ctx               context.Context
	span              trace.Span
	pieceCh           chan *PieceInfo
	peerTaskConductor *peerTaskConductor

	request *SeedTaskRequest
	// progressCh holds progress status
	progressCh chan *SeedTaskProgress
	callsystem string
}

type SeedTaskProgress struct {
	State           *ProgressState
	TaskID          string
	PeerID          string
	ContentLength   int64
	CompletedLength int64
	PeerTaskDone    bool
}

func (ptm *peerTaskManager) newSeedTask(
	ctx context.Context,
	request *SeedTaskRequest,
	limit rate.Limit) (context.Context, *seedTask, error) {
	metrics.SeedTaskCount.Add(1)

	taskID := idgen.TaskID(request.Url, request.UrlMeta)
	ptc, err := ptm.getPeerTaskConductor(ctx, taskID, &request.PeerTaskRequest, limit, nil, request.Range, "", true)
	if err != nil {
		return nil, nil, err
	}

	ctx, span := tracer.Start(ctx, config.SpanSeedTask, trace.WithSpanKind(trace.SpanKindClient))
	pt := &seedTask{
		SugaredLoggerOnWith: ptc.SugaredLoggerOnWith,
		ctx:                 ctx,
		span:                span,
		peerTaskConductor:   ptc,
		pieceCh:             ptc.broker.Subscribe(),
		request:             request,
		progressCh:          make(chan *SeedTaskProgress),
		callsystem:          request.Callsystem,
	}
	return ctx, pt, nil
}

func (s *seedTask) Start(ctx context.Context) (chan *SeedTaskProgress, error) {
	go s.syncProgress()
	// return a progress channel for request download progress
	return s.progressCh, nil
}

func (s *seedTask) syncProgress() {
	var (
		success    bool
		failCode   = base.Code_ClientError
		failReason string
	)
sync:
	for {
		select {
		case <-s.peerTaskConductor.successCh:
			success = true
			break sync
		case <-s.peerTaskConductor.failCh:
			failCode, failReason = s.peerTaskConductor.failedCode, s.peerTaskConductor.failedReason
			break sync
		case <-s.ctx.Done():
			failCode, failReason = base.Code_ClientContextCanceled, s.ctx.Err().Error()
			s.Warnf("wait piece info failed, seed task context done due to %s", failReason)
			break sync
		case piece := <-s.pieceCh:
			if piece.Finished {
				success = true
				break sync
			}
			pg := &SeedTaskProgress{
				State: &ProgressState{
					Success: true,
					Code:    base.Code_Success,
					Msg:     "downloading",
				},
				TaskID:          s.peerTaskConductor.GetTaskID(),
				PeerID:          s.peerTaskConductor.GetPeerID(),
				ContentLength:   s.peerTaskConductor.GetContentLength(),
				CompletedLength: s.peerTaskConductor.completedLength.Load(),
				PeerTaskDone:    false,
			}

			select {
			case s.progressCh <- pg:
				s.Debugf("progress sent, %d/%d", pg.CompletedLength, pg.ContentLength)
			case <-s.ctx.Done():
				failCode, failReason = base.Code_ClientContextCanceled, s.ctx.Err().Error()
				s.Warnf("send progress failed, seed task context done due to %s", failReason)
				break sync
			}
		}
	}

	if success {
		s.sendSuccessProgress()
	} else {
		s.span.RecordError(fmt.Errorf(s.peerTaskConductor.failedReason))
		s.sendFailProgress(failCode, failReason)
	}
	s.span.End()
}

func (s *seedTask) sendSuccessProgress() {
	pg := &SeedTaskProgress{
		State: &ProgressState{
			Success: true,
			Code:    base.Code_Success,
			Msg:     "done",
		},
		TaskID:          s.peerTaskConductor.GetTaskID(),
		PeerID:          s.peerTaskConductor.GetPeerID(),
		ContentLength:   s.peerTaskConductor.GetContentLength(),
		CompletedLength: s.peerTaskConductor.completedLength.Load(),
		PeerTaskDone:    true,
	}
	// send progress
	select {
	case s.progressCh <- pg:
		s.Infof("finish progress sent")
	case <-s.ctx.Done():
		s.Warnf("finish progress sent failed, context done")
	}
}

func (s *seedTask) sendFailProgress(code base.Code, msg string) {
	pg := &SeedTaskProgress{
		State: &ProgressState{
			Success: false,
			Code:    code,
			Msg:     msg,
		},
		TaskID:          s.peerTaskConductor.GetTaskID(),
		PeerID:          s.peerTaskConductor.GetPeerID(),
		ContentLength:   s.peerTaskConductor.GetContentLength(),
		CompletedLength: s.peerTaskConductor.completedLength.Load(),
		PeerTaskDone:    true,
	}

	// wait client received progress
	s.Infof("try to send unfinished progress, completed length: %d, state: (%t, %d, %s)",
		pg.CompletedLength, pg.State.Success, pg.State.Code, pg.State.Msg)
	select {
	case s.progressCh <- pg:
		s.Infof("unfinished progress sent")
	case <-s.ctx.Done():
		s.Warnf("send unfinished progress failed, context done: %v", s.ctx.Err())
	}
}
