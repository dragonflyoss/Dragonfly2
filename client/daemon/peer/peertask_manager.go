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
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"sync"
	"time"

	"d7y.io/dragonfly/v2/pkg/idgen"
	"github.com/go-http-utils/headers"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/time/rate"

	"d7y.io/dragonfly/v2/client/config"
	"d7y.io/dragonfly/v2/client/daemon/storage"
	logger "d7y.io/dragonfly/v2/pkg/dflog"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	"d7y.io/dragonfly/v2/pkg/rpc/scheduler"
	schedulerclient "d7y.io/dragonfly/v2/pkg/rpc/scheduler/client"
)

// TaskManager processes all peer tasks request
type TaskManager interface {
	// StartFilePeerTask starts a peer task to download a file
	// return a progress channel for request download progress
	// tiny stands task file is tiny and task is done
	StartFilePeerTask(ctx context.Context, req *FilePeerTaskRequest) (
		progress chan *FilePeerTaskProgress, tiny *TinyData, err error)
	// StartStreamPeerTask starts a peer task with stream io
	// tiny stands task file is tiny and task is done
	StartStreamPeerTask(ctx context.Context, req *scheduler.PeerTaskRequest) (
		readCloser io.ReadCloser, attribute map[string]string, err error)

	IsPeerTaskRunning(pid string) bool

	// Stop stops the PeerTaskManager
	Stop(ctx context.Context) error
}

// Task represents common interface to operate a peer task
type Task interface {
	Context() context.Context
	Log() *logger.SugaredLoggerOnWith
	ReportPieceResult(pieceTask *base.PieceInfo, pieceResult *scheduler.PieceResult) error
	GetPeerID() string
	GetTaskID() string
	GetTotalPieces() int32
	GetContentLength() int64
	// SetContentLength will called after download completed, when download from source without content length
	SetContentLength(int64) error
	SetCallback(TaskCallback)
	AddTraffic(int64)
	GetTraffic() int64
}

// TaskCallback inserts some operations for peer task download lifecycle
type TaskCallback interface {
	Init(pt Task) error
	Done(pt Task) error
	Update(pt Task) error
	Fail(pt Task, code base.Code, reason string) error
	GetStartTime() time.Time
}

type TinyData struct {
	// span is used by peer task manager to record events without peer task
	span    trace.Span
	TaskID  string
	PeerID  string
	Content []byte
}

var tracer trace.Tracer

func init() {
	tracer = otel.Tracer("dfget-daemon")
}

type peerTaskManager struct {
	host            *scheduler.PeerHost
	schedulerClient schedulerclient.SchedulerClient
	schedulerOption config.SchedulerOption
	pieceManager    PieceManager
	storageManager  storage.Manager

	runningPeerTasks sync.Map

	perPeerRateLimit rate.Limit

	// enableMultiplex indicates reusing completed peer task storage
	// currently, only check completed peer task after register to scheduler
	// TODO multiplex the running peer task
	enableMultiplex bool
}

func NewPeerTaskManager(
	host *scheduler.PeerHost,
	pieceManager PieceManager,
	storageManager storage.Manager,
	schedulerClient schedulerclient.SchedulerClient,
	schedulerOption config.SchedulerOption,
	perPeerRateLimit rate.Limit,
	multiplex bool) (TaskManager, error) {

	ptm := &peerTaskManager{
		host:             host,
		runningPeerTasks: sync.Map{},
		pieceManager:     pieceManager,
		storageManager:   storageManager,
		schedulerClient:  schedulerClient,
		schedulerOption:  schedulerOption,
		perPeerRateLimit: perPeerRateLimit,
		enableMultiplex:  multiplex,
	}
	return ptm, nil
}

func (ptm *peerTaskManager) StartFilePeerTask(ctx context.Context, req *FilePeerTaskRequest) (chan *FilePeerTaskProgress, *TinyData, error) {
	if ptm.enableMultiplex {
		taskID := idgen.TaskID(req.Url, req.Filter, req.UrlMeta, req.BizId)
		reuse := ptm.storageManager.FindCompletedTask(taskID)
		if reuse != nil {
			progress, ok := ptm.tryReuseFilePeerTask(ctx, req, reuse)
			if ok {
				return progress, nil, nil
			}
		}
	}
	// TODO ensure scheduler is ok first
	start := time.Now()
	ctx, pt, tiny, err := newFilePeerTask(ctx, ptm.host, ptm.pieceManager,
		&req.PeerTaskRequest, ptm.schedulerClient, ptm.schedulerOption, ptm.perPeerRateLimit)
	if err != nil {
		return nil, nil, err
	}
	// tiny file content is returned by scheduler, just write to output
	if tiny != nil {
		defer tiny.span.End()
		log := logger.With("peer", tiny.PeerID, "task", tiny.TaskID, "component", "peerTaskManager")
		_, err = os.Stat(req.Output)
		if err == nil {
			// remove exist file
			log.Infof("destination file %q exists, purge it first", req.Output)
			tiny.span.AddEvent("purge exist output")
			os.Remove(req.Output)
		}
		dstFile, err := os.OpenFile(req.Output, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
		if err != nil {
			tiny.span.RecordError(err)
			tiny.span.SetAttributes(config.AttributePeerTaskSuccess.Bool(false))
			log.Errorf("open task destination file error: %s", err)
			return nil, nil, err
		}
		defer dstFile.Close()
		n, err := dstFile.Write(tiny.Content)
		if err != nil {
			tiny.span.RecordError(err)
			tiny.span.SetAttributes(config.AttributePeerTaskSuccess.Bool(false))
			return nil, nil, err
		}
		log.Debugf("copied tasks data %d bytes to %s", n, req.Output)
		tiny.span.SetAttributes(config.AttributePeerTaskSuccess.Bool(true))
		return nil, tiny, nil
	}
	pt.SetCallback(&filePeerTaskCallback{
		ctx:   ctx,
		ptm:   ptm,
		req:   req,
		start: start,
	})

	ptm.runningPeerTasks.Store(req.PeerId, pt)

	// FIXME when failed due to schedulerClient error, relocate schedulerClient and retry
	progress, err := pt.Start(ctx)
	return progress, nil, err
}

func (ptm *peerTaskManager) StartStreamPeerTask(ctx context.Context, req *scheduler.PeerTaskRequest) (io.ReadCloser, map[string]string, error) {
	if ptm.enableMultiplex {
		taskID := idgen.TaskID(req.Url, req.Filter, req.UrlMeta, req.BizId)
		reuse := ptm.storageManager.FindCompletedTask(taskID)
		if reuse != nil {
			r, attr, ok := ptm.tryReuseStreamPeerTask(ctx, req, reuse)
			if ok {
				return r, attr, nil
			}
		}
	}

	start := time.Now()
	ctx, pt, tiny, err := newStreamPeerTask(ctx, ptm.host, ptm.pieceManager,
		req, ptm.schedulerClient, ptm.schedulerOption, ptm.perPeerRateLimit)
	if err != nil {
		return nil, nil, err
	}
	// tiny file content is returned by scheduler, just write to output
	if tiny != nil {
		logger.Infof("copied tasks data %d bytes to buffer", len(tiny.Content))
		tiny.span.SetAttributes(config.AttributePeerTaskSuccess.Bool(true))
		return ioutil.NopCloser(bytes.NewBuffer(tiny.Content)), map[string]string{
			headers.ContentLength: fmt.Sprintf("%d", len(tiny.Content)),
		}, nil
	}

	pt.SetCallback(&streamPeerTaskCallback{
		ctx:   ctx,
		ptm:   ptm,
		req:   req,
		start: start,
	})

	ptm.runningPeerTasks.Store(req.PeerId, pt)

	// FIXME when failed due to schedulerClient error, relocate schedulerClient and retry
	reader, attribute, err := pt.Start(ctx)
	return ioutil.NopCloser(reader), attribute, err
}

func (ptm *peerTaskManager) Stop(ctx context.Context) error {
	// TODO
	return nil
}

func (ptm *peerTaskManager) PeerTaskDone(peerID string) {
	ptm.runningPeerTasks.Delete(peerID)
}

func (ptm *peerTaskManager) IsPeerTaskRunning(peerID string) bool {
	_, ok := ptm.runningPeerTasks.Load(peerID)
	return ok
}
