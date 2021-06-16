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
	"d7y.io/dragonfly/v2/pkg/dfcodes"
	logger "d7y.io/dragonfly/v2/pkg/dflog"
	"d7y.io/dragonfly/v2/pkg/rpc/scheduler"
	"github.com/go-http-utils/headers"
	"go.opentelemetry.io/otel/semconv"
	"go.opentelemetry.io/otel/trace"
)

func (ptm *peerTaskManager) tryReuseFilePeerTask(
	ctx context.Context,
	request *FilePeerTaskRequest,
	reuse *storage.ReusePeerTask) (chan *FilePeerTaskProgress, bool) {
	log := logger.With("peer", request.PeerId, "task", reuse.TaskID, "component", "reuseFilePeerTask")

	_, span := tracer.Start(ctx, config.SpanReusePeerTask, trace.WithSpanKind(trace.SpanKindClient))
	span.SetAttributes(config.AttributePeerHost.String(ptm.host.Uuid))
	span.SetAttributes(semconv.NetHostIPKey.String(ptm.host.Ip))
	span.SetAttributes(config.AttributePeerID.String(request.PeerId))
	span.SetAttributes(semconv.HTTPURLKey.String(request.Url))

	log.Infof("reuse from peer task: %s, size: %d", reuse.PeerID, reuse.ContentLength)
	span.AddEvent("reuse peer task", trace.WithAttributes(config.AttributePeerID.String(reuse.PeerID)))

	start := time.Now()
	err := ptm.storageManager.Store(
		context.Background(),
		&storage.StoreRequest{
			CommonTaskRequest: storage.CommonTaskRequest{
				PeerID:      reuse.PeerID,
				TaskID:      reuse.TaskID,
				Destination: request.Output,
			},
			MetadataOnly: false,
			StoreOnly:    true,
			TotalPieces:  reuse.TotalPieces,
		})
	if err != nil {
		log.Errorf("store error when reuse peer task: %s", err)
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
		TaskID:          reuse.TaskID,
		PeerID:          request.PeerId,
		ContentLength:   reuse.ContentLength,
		CompletedLength: reuse.ContentLength,
		PeerTaskDone:    true,
		DoneCallback:    func() {},
	}

	progressCh := make(chan *FilePeerTaskProgress, 1)
	progressCh <- pg

	span.SetAttributes(config.AttributePeerTaskSuccess.Bool(true))
	span.End()
	return progressCh, true
}

func (ptm *peerTaskManager) tryReuseStreamPeerTask(
	ctx context.Context,
	request *scheduler.PeerTaskRequest,
	reuse *storage.ReusePeerTask) (io.ReadCloser, map[string]string, bool) {
	log := logger.With("peer", request.PeerId, "task", reuse.TaskID, "component", "reuseStreamPeerTask")
	ctx, span := tracer.Start(ctx, config.SpanStreamPeerTask, trace.WithSpanKind(trace.SpanKindClient))
	span.SetAttributes(config.AttributePeerHost.String(ptm.host.Uuid))
	span.SetAttributes(semconv.NetHostIPKey.String(ptm.host.Ip))
	span.SetAttributes(config.AttributePeerID.String(request.PeerId))
	span.SetAttributes(semconv.HTTPURLKey.String(request.Url))

	rc, err := ptm.storageManager.ReadAllPieces(ctx, &reuse.PeerTaskMetaData)
	if err != nil {
		return nil, nil, false
	}

	attr := map[string]string{}
	attr[headers.ContentLength] = fmt.Sprintf("%d", reuse.ContentLength)
	attr[config.HeaderDragonflyTask] = reuse.TaskID
	attr[config.HeaderDragonflyPeer] = request.PeerId

	// TODO record time when file closed
	span.SetAttributes(config.AttributePeerTaskSuccess.Bool(true))
	log.Infof("reuse from peer task: %s, size: %d", reuse.PeerID, reuse.ContentLength)
	return rc, attr, true
}
