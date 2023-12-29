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
	"os"
	"time"

	"github.com/go-http-utils/headers"
	semconv "go.opentelemetry.io/otel/semconv/v1.7.0"
	"go.opentelemetry.io/otel/trace"

	commonv1 "d7y.io/api/v2/pkg/apis/common/v1"

	"d7y.io/dragonfly/v2/client/config"
	"d7y.io/dragonfly/v2/client/daemon/storage"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/idgen"
	"d7y.io/dragonfly/v2/pkg/net/http"
)

var _ *logger.SugaredLoggerOnWith // pin this package for no log code generation

// reuse task search logic:
// A. prefetch feature enabled
//    for ranged request, 1, find completed subtask, 2, find partial completed parent task
//    for non-ranged request, just find completed task
// B. prefetch feature disabled
//    for ranged request, 1, find completed normal task, 2, find partial completed parent task
//    for non-ranged request, just find completed task

func (ptm *peerTaskManager) tryReuseFilePeerTask(ctx context.Context,
	request *FileTaskRequest) (chan *FileTaskProgress, bool) {
	taskID := idgen.TaskIDV1(request.Url, request.UrlMeta)
	var (
		reuse      *storage.ReusePeerTask
		reuseRange *http.Range // the range of parent peer task data to read
		log        *logger.SugaredLoggerOnWith
		length     int64
		err        error
	)

	if ptm.enabledPrefetch(request.Range) {
		reuse = ptm.StorageManager.FindCompletedSubTask(taskID)
	} else {
		reuse = ptm.StorageManager.FindCompletedTask(taskID)
	}

	if reuse == nil {
		if request.Range == nil {
			return nil, false
		}
		// for ranged request, check the parent task
		reuseRange = request.Range
		taskID = idgen.ParentTaskIDV1(request.Url, request.UrlMeta)
		reuse = ptm.StorageManager.FindPartialCompletedTask(taskID, reuseRange)
		if reuse == nil {
			return nil, false
		}
	}

	logKV := []any{
		"peer", request.PeerId,
		"task", taskID,
	}

	if spanContext := trace.SpanFromContext(ctx).SpanContext(); spanContext.TraceID().IsValid() {
		logKV = append(logKV, "trace", spanContext.TraceID().String())
	}

	if reuseRange == nil {
		logKV = append(logKV, "component", "reuseFilePeerTask")
		log = logger.With(logKV...)
		log.Infof("reuse from peer task: %s, total size: %d", reuse.PeerID, reuse.ContentLength)
		length = reuse.ContentLength
	} else {
		logKV = append(logKV, "range", request.UrlMeta.Range, "component", "reuseRangeFilePeerTask")
		log = logger.With(logKV...)
		log.Infof("reuse partial data from peer task: %s, total size: %d, range: %s",
			reuse.PeerID, reuse.ContentLength, request.UrlMeta.Range)

		// correct range like: bytes=1024-
		if reuseRange.Start+reuseRange.Length > reuse.ContentLength {
			reuseRange.Length = reuse.ContentLength - reuseRange.Start
			if reuseRange.Length < 0 {
				return nil, false
			}
		}
		length = reuseRange.Length
	}

	_, span := tracer.Start(ctx, config.SpanReusePeerTask, trace.WithSpanKind(trace.SpanKindClient))
	span.SetAttributes(config.AttributePeerHost.String(ptm.PeerHost.Id))
	span.SetAttributes(semconv.NetHostIPKey.String(ptm.PeerHost.Ip))
	span.SetAttributes(config.AttributeTaskID.String(taskID))
	span.SetAttributes(config.AttributePeerID.String(request.PeerId))
	span.SetAttributes(config.AttributeReusePeerID.String(reuse.PeerID))
	span.SetAttributes(semconv.HTTPURLKey.String(request.Url))
	if reuseRange != nil {
		span.SetAttributes(config.AttributeReuseRange.String(request.UrlMeta.Range))
	}
	defer span.End()

	log.Infof("reuse from peer task: %s, total size: %d, target size: %d", reuse.PeerID, reuse.ContentLength, length)
	span.AddEvent("reuse peer task", trace.WithAttributes(config.AttributePeerID.String(reuse.PeerID)))

	start := time.Now()
	if reuseRange == nil || request.KeepOriginalOffset {
		storeRequest := &storage.StoreRequest{
			CommonTaskRequest: storage.CommonTaskRequest{
				PeerID:      reuse.PeerID,
				TaskID:      taskID,
				Destination: request.Output,
			},
			MetadataOnly:   false,
			StoreDataOnly:  true,
			TotalPieces:    reuse.TotalPieces,
			OriginalOffset: request.KeepOriginalOffset,
		}
		err = ptm.StorageManager.Store(ctx, storeRequest)
	} else {
		err = ptm.storePartialFile(ctx, request, log, reuse, reuseRange)
	}

	if err != nil {
		log.Errorf("store error when reuse peer task: %s", err)
		span.SetAttributes(config.AttributePeerTaskSuccess.Bool(false))
		span.RecordError(err)
		return nil, false
	}

	// check reuse target is valid
	stat, err := os.Stat(request.Output)
	if err != nil {
		log.Errorf("stat error when reuse peer task: %s", err)
		span.SetAttributes(config.AttributePeerTaskSuccess.Bool(false))
		span.RecordError(err)
		return nil, false
	}

	if request.KeepOriginalOffset {
		// KeepOriginalOffset case
		if length > 0 && stat.Size() == 0 {
			err = fmt.Errorf("reuse failed, output file size is zero, but target length %d is not zero", length)
			log.Errorf(err.Error())
			span.SetAttributes(config.AttributePeerTaskSuccess.Bool(false))
			span.RecordError(err)
			return nil, false
		}
	} else if length != stat.Size() {
		// normal case
		err = fmt.Errorf("reuse failed, output file size %d is not same with target length %d", stat.Size(), length)
		log.Errorf(err.Error())
		span.SetAttributes(config.AttributePeerTaskSuccess.Bool(false))
		span.RecordError(err)
		return nil, false
	}

	var cost = time.Since(start).Milliseconds()
	log.Infof("reuse file peer task done, cost: %dms", cost)

	pg := &FileTaskProgress{
		State: &ProgressState{
			Success: true,
			Code:    commonv1.Code_Success,
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
	log *logger.SugaredLoggerOnWith, reuse *storage.ReusePeerTask, rg *http.Range) error {
	f, err := os.OpenFile(request.Output, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
	if err != nil {
		log.Errorf("open dest file error when reuse peer task: %s", err)
		return err
	}
	rc, err := ptm.StorageManager.ReadAllPieces(ctx,
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

func (ptm *peerTaskManager) tryReuseStreamPeerTask(ctx context.Context, taskID string,
	request *StreamTaskRequest) (io.ReadCloser, map[string]string, bool) {
	var (
		reuse      *storage.ReusePeerTask
		reuseRange *http.Range // the range of parent peer task data to read
		log        *logger.SugaredLoggerOnWith
		length     int64
	)

	if ptm.enabledPrefetch(request.Range) {
		reuse = ptm.StorageManager.FindCompletedSubTask(taskID)
	} else {
		reuse = ptm.StorageManager.FindCompletedTask(taskID)
	}

	if reuse == nil {
		if request.Range == nil {
			return nil, nil, false
		}
		// for ranged request, check the parent task
		reuseRange = request.Range
		taskID = idgen.ParentTaskIDV1(request.URL, request.URLMeta)
		reuse = ptm.StorageManager.FindPartialCompletedTask(taskID, reuseRange)
		if reuse == nil {
			return nil, nil, false
		}
	}

	logKV := []any{
		"peer", request.PeerID,
		"task", taskID,
	}
	if spanContext := trace.SpanFromContext(ctx).SpanContext(); spanContext.TraceID().IsValid() {
		logKV = append(logKV, "trace", spanContext.TraceID().String())
	}

	if reuseRange == nil {
		logKV = append(logKV, "component", "reuseStreamPeerTask")
		log = logger.With(logKV...)
		log.Infof("reuse from peer task: %s, total size: %d", reuse.PeerID, reuse.ContentLength)
		length = reuse.ContentLength
	} else {
		logKV = append(logKV, "component", "reuseRangeStreamPeerTask")
		log = logger.With(logKV...)
		log.Infof("reuse partial data from peer task: %s, total size: %d, range: %s",
			reuse.PeerID, reuse.ContentLength, request.URLMeta.Range)

		// correct range like: bytes=1024-
		if reuseRange.Length > reuse.ContentLength-reuseRange.Start {
			reuseRange.Length = reuse.ContentLength - reuseRange.Start
			if reuseRange.Length < 0 {
				return nil, nil, false
			}
		}
		length = reuseRange.Length
	}

	ctx, span := tracer.Start(ctx, config.SpanStreamTask, trace.WithSpanKind(trace.SpanKindClient))
	span.SetAttributes(config.AttributePeerHost.String(ptm.PeerHost.Id))
	span.SetAttributes(semconv.NetHostIPKey.String(ptm.PeerHost.Ip))
	span.SetAttributes(config.AttributeTaskID.String(taskID))
	span.SetAttributes(config.AttributePeerID.String(request.PeerID))
	span.SetAttributes(config.AttributeReusePeerID.String(reuse.PeerID))
	span.SetAttributes(semconv.HTTPURLKey.String(request.URL))
	if reuseRange != nil {
		span.SetAttributes(config.AttributeReuseRange.String(request.URLMeta.Range))
	}
	defer span.End()

	rc, err := ptm.StorageManager.ReadAllPieces(ctx,
		&storage.ReadAllPiecesRequest{PeerTaskMetadata: reuse.PeerTaskMetadata, Range: reuseRange})
	if err != nil {
		log.Errorf("read pieces error when reuse peer task: %s", err)
		span.SetAttributes(config.AttributePeerTaskSuccess.Bool(false))
		span.RecordError(err)
		return nil, nil, false
	}

	exa, err := ptm.StorageManager.GetExtendAttribute(ctx, &reuse.PeerTaskMetadata)
	if err != nil {
		log.Errorf("get extend attribute error when reuse peer task: %s", err)
		span.SetAttributes(config.AttributePeerTaskSuccess.Bool(false))
		span.RecordError(err)
		return nil, nil, false
	}

	attr := map[string]string{}
	attr[config.HeaderDragonflyTask] = taskID
	attr[config.HeaderDragonflyPeer] = request.PeerID
	attr[headers.ContentLength] = fmt.Sprintf("%d", length)

	if exa != nil {
		for k, v := range exa.Header {
			attr[k] = v
		}
	}

	if reuseRange != nil {
		attr[config.HeaderDragonflyRange] = request.URLMeta.Range
		attr[headers.ContentRange] = fmt.Sprintf("bytes %d-%d/%d", reuseRange.Start,
			reuseRange.Start+reuseRange.Length-1, reuse.ContentLength)
	} else if request.Range != nil {
		// the length is from reuse task, ensure it equal with request
		if length != request.Range.Length {
			log.Errorf("target task length %d did not match range length %d", length, request.Range.Length)
			return nil, nil, false
		}
		attr[headers.ContentRange] = fmt.Sprintf("bytes %d-%d/*", request.Range.Start,
			request.Range.Start+request.Range.Length-1)
	}

	// TODO record time when file closed, need add a type to implement Close and WriteTo
	span.SetAttributes(config.AttributePeerTaskSuccess.Bool(true))
	return rc, attr, true
}

func (ptm *peerTaskManager) tryReuseSeedPeerTask(ctx context.Context,
	request *SeedTaskRequest) (*SeedTaskResponse, bool) {
	taskID := idgen.TaskIDV1(request.Url, request.UrlMeta)
	var (
		reuse      *storage.ReusePeerTask
		reuseRange *http.Range // the range of parent peer task data to read
		log        *logger.SugaredLoggerOnWith
	)

	if ptm.enabledPrefetch(request.Range) {
		reuse = ptm.StorageManager.FindCompletedSubTask(taskID)
	} else {
		reuse = ptm.StorageManager.FindCompletedTask(taskID)
	}

	if reuse == nil {
		return nil, false

		// if request.Range == nil {
		// return nil, false
		// }
		// TODO, mock SeedTaskResponse for sub task
		// for ranged request, check the parent task
		//reuseRange = request.Range
		//taskID = idgen.ParentTaskID(request.Url, request.UrlMeta)
		//reuse = ptm.StorageManager.FindPartialCompletedTask(taskID, reuseRange)
		//if reuse == nil {
		//	return nil, false
		//}
	}

	if reuseRange == nil {
		log = logger.With("peer", request.PeerId, "task", taskID, "component", "reuseSeedPeerTask")
		log.Infof("reuse from peer task: %s, total size: %d", reuse.PeerID, reuse.ContentLength)
	} else {
		log = logger.With("peer", request.PeerId, "task", taskID, "range", request.UrlMeta.Range,
			"component", "reuseRangeSeedPeerTask")
		log.Infof("reuse partial data from peer task: %s, total size: %d, range: %s",
			reuse.PeerID, reuse.ContentLength, request.UrlMeta.Range)
	}

	ctx, span := tracer.Start(ctx, config.SpanReusePeerTask, trace.WithSpanKind(trace.SpanKindClient))
	span.SetAttributes(config.AttributePeerHost.String(ptm.PeerHost.Id))
	span.SetAttributes(semconv.NetHostIPKey.String(ptm.PeerHost.Ip))
	span.SetAttributes(config.AttributeTaskID.String(taskID))
	span.SetAttributes(config.AttributePeerID.String(request.PeerId))
	span.SetAttributes(config.AttributeReusePeerID.String(reuse.PeerID))
	span.SetAttributes(semconv.HTTPURLKey.String(request.Url))
	if reuseRange != nil {
		span.SetAttributes(config.AttributeReuseRange.String(request.UrlMeta.Range))
	}

	successCh := make(chan struct{}, 1)
	successCh <- struct{}{}

	span.SetAttributes(config.AttributePeerTaskSuccess.Bool(true))
	return &SeedTaskResponse{
		Context: ctx,
		Span:    span,
		TaskID:  taskID,
		PeerID:  reuse.PeerID,
		SubscribeResponse: SubscribeResponse{
			Storage:          reuse.Storage,
			PieceInfoChannel: nil,
			Success:          successCh,
			Fail:             nil,
			FailReason: func() error {
				return nil
			},
		},
	}, true
}
