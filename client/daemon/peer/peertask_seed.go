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

	"go.opentelemetry.io/otel/trace"
	"golang.org/x/time/rate"

	"d7y.io/dragonfly/v2/client/clientutil"
	"d7y.io/dragonfly/v2/client/config"
	"d7y.io/dragonfly/v2/client/daemon/metrics"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/idgen"
	"d7y.io/dragonfly/v2/pkg/rpc/scheduler"
)

type SeedTaskRequest struct {
	scheduler.PeerTaskRequest
	Limit      float64
	Callsystem string
	Range      *clientutil.Range
}

type SeedTaskResponse struct {
	SubscribeResponse
	Context context.Context
	Span    trace.Span
	TaskID  string
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
	limit rate.Limit) (*SeedTaskResponse, error) {
	metrics.SeedTaskCount.Add(1)

	taskID := idgen.TaskID(request.Url, request.UrlMeta)
	ptc, err := ptm.getPeerTaskConductor(ctx, taskID, &request.PeerTaskRequest, limit, nil, request.Range, "", true)
	if err != nil {
		return nil, err
	}

	ctx, span := tracer.Start(ctx, config.SpanSeedTask, trace.WithSpanKind(trace.SpanKindClient))
	resp := &SeedTaskResponse{
		Context: ctx,
		Span:    span,
		TaskID:  taskID,
		SubscribeResponse: SubscribeResponse{
			Storage:          ptc.storage,
			PieceInfoChannel: ptc.broker.Subscribe(),
			Success:          ptc.successCh,
			Fail:             ptc.failCh,
		},
	}
	return resp, nil
}
