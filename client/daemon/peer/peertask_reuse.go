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

package peer

import (
	"context"
	"fmt"
	"io"
	"time"

	"d7y.io/dragonfly/v2/client/config"
	"d7y.io/dragonfly/v2/client/daemon/storage"
	"d7y.io/dragonfly/v2/internal/dfcodes"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/internal/idgen"
	"d7y.io/dragonfly/v2/pkg/rpc/scheduler"
	"github.com/go-http-utils/headers"
	"go.opentelemetry.io/otel/semconv"
	"go.opentelemetry.io/otel/trace"
)

var _ *logger.SugaredLoggerOnWith // pin this package for no log code generation

func (ptm *peerTaskManager) tryReuseFilePeerTask(ctx context.Context,
	request *FilePeerTaskRequest) (chan *FilePeerTaskProgress, bool) {
	taskID := idgen.TaskID(request.Url, request.Filter, request.UrlMeta, request.BizId)
	reuse := ptm.storageManager.FindCompletedTask(taskID)
	if reuse == nil {
		return nil, false
	}

	log := logger.With("peer", request.PeerId, "task", taskID, "component", "reuseFilePeerTask")

	_, span := tracer.Start(ctx, config.SpanReusePeerTask, trace.WithSpanKind(trace.SpanKindClient))
	span.SetAttributes(config.AttributePeerHost.String(ptm.host.Uuid))
	span.SetAttributes(semconv.NetHostIPKey.String(ptm.host.Ip))
	span.SetAttributes(config.AttributeTaskID.String(taskID))
	span.SetAttributes(config.AttributePeerID.String(request.PeerId))
	span.SetAttributes(config.AttributeReusePeerID.String(reuse.PeerID))
	span.SetAttributes(semconv.HTTPURLKey.String(request.Url))
	defer span.End()

	log.Infof("reuse from peer task: %s, size: %d", reuse.PeerID, reuse.ContentLength)
	span.AddEvent("reuse peer task", trace.WithAttributes(config.AttributePeerID.String(reuse.PeerID)))

	start := time.Now()
	err := ptm.storageManager.Store(
		context.Background(),
		&storage.StoreRequest{
			CommonTaskRequest: storage.CommonTaskRequest{
				PeerID:      reuse.PeerID,
				TaskID:      taskID,
				Destination: request.Output,
			},
			MetadataOnly: false,
			StoreOnly:    true,
			TotalPieces:  reuse.TotalPieces,
		})
	if err != nil {
		log.Errorf("store error when reuse peer task: %s", err)
		span.SetAttributes(config.AttributePeerTaskSuccess.Bool(false))
		span.RecordError(err)
		return nil, false
	}
	var cost = time.Now().Sub(start).Milliseconds()
	log.Infof("reuse file peer task done, cost: %dms", cost)

	pg := &FilePeerTaskProgress{
		State: &ProgressState{
			Success: true,
			Code:    dfcodes.Success,
			Msg:     "Success",
		},
		TaskID:          taskID,
		PeerID:          request.PeerId,
		ContentLength:   reuse.ContentLength,
		CompletedLength: reuse.ContentLength,
		PeerTaskDone:    true,
		DoneCallback:    func() {},
	}

	// make a new buffered channel, because we did not need to call newFilePeerTask
	progressCh := make(chan *FilePeerTaskProgress, 1)
	progressCh <- pg

	span.SetAttributes(config.AttributePeerTaskSuccess.Bool(true))
	span.SetAttributes(config.AttributePeerTaskCost.Int64(cost))
	return progressCh, true
}

func (ptm *peerTaskManager) tryReuseStreamPeerTask(ctx context.Context,
	request *scheduler.PeerTaskRequest) (io.ReadCloser, map[string]string, bool) {
	taskID := idgen.TaskID(request.Url, request.Filter, request.UrlMeta, request.BizId)
	reuse := ptm.storageManager.FindCompletedTask(taskID)
	if reuse == nil {
		return nil, nil, false
	}

	log := logger.With("peer", request.PeerId, "task", taskID, "component", "reuseStreamPeerTask")
	log.Infof("reuse from peer task: %s, size: %d", reuse.PeerID, reuse.ContentLength)

	ctx, span := tracer.Start(ctx, config.SpanStreamPeerTask, trace.WithSpanKind(trace.SpanKindClient))
	span.SetAttributes(config.AttributePeerHost.String(ptm.host.Uuid))
	span.SetAttributes(semconv.NetHostIPKey.String(ptm.host.Ip))
	span.SetAttributes(config.AttributeTaskID.String(taskID))
	span.SetAttributes(config.AttributePeerID.String(request.PeerId))
	span.SetAttributes(config.AttributeReusePeerID.String(reuse.PeerID))
	span.SetAttributes(semconv.HTTPURLKey.String(request.Url))
	defer span.End()

	rc, err := ptm.storageManager.ReadAllPieces(ctx, &reuse.PeerTaskMetaData)
	if err != nil {
		log.Errorf("read all pieces error when reuse peer task: %s", err)
		span.SetAttributes(config.AttributePeerTaskSuccess.Bool(false))
		span.RecordError(err)
		return nil, nil, false
	}

	attr := map[string]string{}
	attr[headers.ContentLength] = fmt.Sprintf("%d", reuse.ContentLength)
	attr[config.HeaderDragonflyTask] = taskID
	attr[config.HeaderDragonflyPeer] = request.PeerId

	// TODO record time when file closed, need add a type to implement Close and WriteTo
	span.SetAttributes(config.AttributePeerTaskSuccess.Bool(true))
	return rc, attr, true
}
