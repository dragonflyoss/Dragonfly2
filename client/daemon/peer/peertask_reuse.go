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
	"math"
	"os"
	"time"

	"github.com/go-http-utils/headers"
	"go.opentelemetry.io/otel/semconv"
	"go.opentelemetry.io/otel/trace"

	"d7y.io/dragonfly/v2/client/clientutil"
	"d7y.io/dragonfly/v2/client/config"
	"d7y.io/dragonfly/v2/client/daemon/storage"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/idgen"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	"d7y.io/dragonfly/v2/pkg/util/rangeutils"
)

var _ *logger.SugaredLoggerOnWith // pin this package for no log code generation

func (ptm *peerTaskManager) tryReuseFilePeerTask(ctx context.Context,
	request *FileTaskRequest) (chan *FileTaskProgress, bool) {
	taskID := idgen.TaskID(request.Url, request.UrlMeta)
	reuse := ptm.storageManager.FindCompletedTask(taskID)
	var (
		rg     *clientutil.Range // the range of parent peer task data to read
		log    *logger.SugaredLoggerOnWith
		length int64
		err    error
	)
	if reuse == nil {
		taskID = idgen.ParentTaskID(request.Url, request.UrlMeta)
		reuse = ptm.storageManager.FindCompletedTask(taskID)
		if reuse == nil {
			return nil, false
		}
		var r *rangeutils.Range
		r, err = rangeutils.ParseRange(request.UrlMeta.Range, math.MaxInt)
		if err != nil {
			logger.Warnf("parse range %s error: %s", request.UrlMeta.Range, err)
			return nil, false
		}
		rg = &clientutil.Range{
			Start:  int64(r.StartIndex),
			Length: int64(r.EndIndex - r.StartIndex + 1),
		}
	}

	if rg == nil {
		log = logger.With("peer", request.PeerId, "task", taskID, "component", "reuseFilePeerTask")
		log.Infof("reuse from peer task: %s, total size: %d", reuse.PeerID, reuse.ContentLength)
		length = reuse.ContentLength
	} else {
		log = logger.With("peer", request.PeerId, "task", taskID, "range", request.UrlMeta.Range,
			"component", "reuseRangeFilePeerTask")
		log.Infof("reuse partial data from peer task: %s, total size: %d, range: %s",
			reuse.PeerID, reuse.ContentLength, request.UrlMeta.Range)
		length = rg.Length
	}

	_, span := tracer.Start(ctx, config.SpanReusePeerTask, trace.WithSpanKind(trace.SpanKindClient))
	span.SetAttributes(config.AttributePeerHost.String(ptm.host.Uuid))
	span.SetAttributes(semconv.NetHostIPKey.String(ptm.host.Ip))
	span.SetAttributes(config.AttributeTaskID.String(taskID))
	span.SetAttributes(config.AttributePeerID.String(request.PeerId))
	span.SetAttributes(config.AttributeReusePeerID.String(reuse.PeerID))
	span.SetAttributes(semconv.HTTPURLKey.String(request.Url))
	if rg != nil {
		span.SetAttributes(config.AttributeReuseRange.String(request.UrlMeta.Range))
	}
	defer span.End()

	log.Infof("reuse from peer task: %s, total size: %d, target size: %d", reuse.PeerID, reuse.ContentLength, length)
	span.AddEvent("reuse peer task", trace.WithAttributes(config.AttributePeerID.String(reuse.PeerID)))

	start := time.Now()
	if rg == nil {
		storeRequest := &storage.StoreRequest{
			CommonTaskRequest: storage.CommonTaskRequest{
				PeerID:      reuse.PeerID,
				TaskID:      taskID,
				Destination: request.Output,
			},
			MetadataOnly: false,
			StoreOnly:    true,
			TotalPieces:  reuse.TotalPieces,
		}
		err = ptm.storageManager.Store(context.Background(), storeRequest)
	} else {
		err = ptm.storePartialFile(ctx, request, log, reuse, rg)
	}

	if err != nil {
		log.Errorf("store error when reuse peer task: %s", err)
		span.SetAttributes(config.AttributePeerTaskSuccess.Bool(false))
		span.RecordError(err)
		return nil, false
	}

	var cost = time.Now().Sub(start).Milliseconds()
	log.Infof("reuse file peer task done, cost: %dms", cost)

	pg := &FileTaskProgress{
		State: &ProgressState{
			Success: true,
			Code:    base.Code_Success,
			Msg:     "Success",
		},
		TaskID:          taskID,
		PeerID:          request.PeerId,
		ContentLength:   length,
		CompletedLength: length,
		PeerTaskDone:    true,
		DoneCallback:    func() {},
	}

	// make a new buffered channel, because we did not need to call newFileTask
	progressCh := make(chan *FileTaskProgress, 1)
	progressCh <- pg

	span.SetAttributes(config.AttributePeerTaskSuccess.Bool(true))
	span.SetAttributes(config.AttributePeerTaskCost.Int64(cost))
	return progressCh, true
}

func (ptm *peerTaskManager) storePartialFile(ctx context.Context, request *FileTaskRequest,
	log *logger.SugaredLoggerOnWith, reuse *storage.ReusePeerTask, rg *clientutil.Range) error {
	f, err := os.OpenFile(request.Output, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
	if err != nil {
		log.Errorf("open dest file error when reuse peer task: %s", err)
		return err
	}
	rc, err := ptm.storageManager.ReadAllPieces(ctx,
		&storage.ReadAllPiecesRequest{PeerTaskMetadata: reuse.PeerTaskMetadata, Range: rg})
	if err != nil {
		log.Errorf("read pieces error when reuse peer task: %s", err)
		return err
	}
	defer rc.Close()
	n, err := io.Copy(f, rc)
	if err != nil {
		log.Errorf("copy data error when reuse peer task: %s", err)
		return err
	}
	if n != rg.Length {
		log.Errorf("copy data length not match when reuse peer task, actual: %d, desire: %d", n, rg.Length)
		return io.ErrShortBuffer
	}
	return nil
}

func (ptm *peerTaskManager) tryReuseStreamPeerTask(ctx context.Context,
	request *StreamTaskRequest) (io.ReadCloser, map[string]string, bool) {
	taskID := idgen.TaskID(request.URL, request.URLMeta)
	reuse := ptm.storageManager.FindCompletedTask(taskID)
	var (
		rg  *clientutil.Range // the range of parent peer task data to read
		log *logger.SugaredLoggerOnWith
	)
	if reuse == nil {
		// for ranged request, check the parent task
		if request.Range == nil {
			return nil, nil, false
		}
		taskID = idgen.ParentTaskID(request.URL, request.URLMeta)
		reuse = ptm.storageManager.FindCompletedTask(taskID)
		if reuse == nil {
			return nil, nil, false
		}
		rg = request.Range
	}

	if rg == nil {
		log = logger.With("peer", request.PeerID, "task", taskID, "component", "reuseStreamPeerTask")
		log.Infof("reuse from peer task: %s, total size: %d", reuse.PeerID, reuse.ContentLength)
	} else {
		log = logger.With("peer", request.PeerID, "task", taskID, "component", "reuseRangeStreamPeerTask")
		log.Infof("reuse partial data from peer task: %s, total size: %d, range: %s",
			reuse.PeerID, reuse.ContentLength, request.URLMeta.Range)
	}

	ctx, span := tracer.Start(ctx, config.SpanStreamTask, trace.WithSpanKind(trace.SpanKindClient))
	span.SetAttributes(config.AttributePeerHost.String(ptm.host.Uuid))
	span.SetAttributes(semconv.NetHostIPKey.String(ptm.host.Ip))
	span.SetAttributes(config.AttributeTaskID.String(taskID))
	span.SetAttributes(config.AttributePeerID.String(request.PeerID))
	span.SetAttributes(config.AttributeReusePeerID.String(reuse.PeerID))
	span.SetAttributes(semconv.HTTPURLKey.String(request.URL))
	if rg != nil {
		span.SetAttributes(config.AttributeReuseRange.String(request.URLMeta.Range))
	}
	defer span.End()

	rc, err := ptm.storageManager.ReadAllPieces(ctx,
		&storage.ReadAllPiecesRequest{PeerTaskMetadata: reuse.PeerTaskMetadata, Range: rg})
	if err != nil {
		log.Errorf("read pieces error when reuse peer task: %s", err)
		span.SetAttributes(config.AttributePeerTaskSuccess.Bool(false))
		span.RecordError(err)
		return nil, nil, false
	}

	attr := map[string]string{}
	attr[config.HeaderDragonflyTask] = taskID
	attr[config.HeaderDragonflyPeer] = request.PeerID
	if rg != nil {
		attr[config.HeaderDragonflyRange] = request.URLMeta.Range
		attr[headers.ContentRange] = fmt.Sprintf("bytes %d-%d/%d", rg.Start, rg.Start+rg.Length-1, reuse.ContentLength)
		attr[headers.ContentLength] = fmt.Sprintf("%d", rg.Length)
	} else {
		attr[headers.ContentLength] = fmt.Sprintf("%d", reuse.ContentLength)
	}

	// TODO record time when file closed, need add a type to implement Close and WriteTo
	span.SetAttributes(config.AttributePeerTaskSuccess.Bool(true))
	return rc, attr, true
}
