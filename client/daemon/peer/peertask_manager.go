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

	"github.com/go-http-utils/headers"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/time/rate"

	"d7y.io/dragonfly/v2/client/clientutil"
	"d7y.io/dragonfly/v2/client/config"
	"d7y.io/dragonfly/v2/client/daemon/storage"
	logger "d7y.io/dragonfly/v2/internal/dflog"
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
	ReportPieceResult(result *pieceTaskResult) error
	GetPeerID() string
	GetTaskID() string
	GetTotalPieces() int32
	SetTotalPieces(int32)
	GetContentLength() int64
	// SetContentLength will be called after download completed, when download from source without content length
	SetContentLength(int64) error
	SetCallback(TaskCallback)
	AddTraffic(uint64)
	GetTraffic() uint64
	SetPieceMd5Sign(string)
	GetPieceMd5Sign() string
}

// TaskCallback inserts some operations for peer task download lifecycle
type TaskCallback interface {
	Init(pt Task) error
	Done(pt Task) error
	Update(pt Task) error
	Fail(pt Task, code base.Code, reason string) error
	GetStartTime() time.Time
	ValidateDigest(pt Task) error
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

	calculateDigest bool
}

func NewPeerTaskManager(
	host *scheduler.PeerHost,
	pieceManager PieceManager,
	storageManager storage.Manager,
	schedulerClient schedulerclient.SchedulerClient,
	schedulerOption config.SchedulerOption,
	perPeerRateLimit rate.Limit,
	multiplex bool,
	calculateDigest bool) (TaskManager, error) {

	ptm := &peerTaskManager{
		host:             host,
		runningPeerTasks: sync.Map{},
		pieceManager:     pieceManager,
		storageManager:   storageManager,
		schedulerClient:  schedulerClient,
		schedulerOption:  schedulerOption,
		perPeerRateLimit: perPeerRateLimit,
		enableMultiplex:  multiplex,
		calculateDigest:  calculateDigest,
	}
	return ptm, nil
}

var _ TaskManager = (*peerTaskManager)(nil)

func (ptm *peerTaskManager) StartFilePeerTask(ctx context.Context, req *FilePeerTaskRequest) (chan *FilePeerTaskProgress, *TinyData, error) {
	if ptm.enableMultiplex {
		progress, ok := ptm.tryReuseFilePeerTask(ctx, req)
		if ok {
			return progress, nil, nil
		}
	}
	// TODO ensure scheduler is ok first
	start := time.Now()
	limit := ptm.perPeerRateLimit
	if req.Limit > 0 {
		limit = rate.Limit(req.Limit)
	}
	ctx, pt, tiny, err := newFilePeerTask(ctx, ptm.host, ptm.pieceManager,
		req, ptm.schedulerClient, ptm.schedulerOption, limit)
	if err != nil {
		return nil, nil, err
	}
	// tiny file content is returned by scheduler, just write to output
	if tiny != nil {
		ptm.storeTinyPeerTask(ctx, tiny)
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
		tiny.span.SetAttributes(config.AttributePeerTaskSize.Int(n))
		return nil, tiny, nil
	}
	pt.SetCallback(&filePeerTaskCallback{
		ptm:   ptm,
		pt:    pt,
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
		r, attr, ok := ptm.tryReuseStreamPeerTask(ctx, req)
		if ok {
			return r, attr, nil
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
		ptm.storeTinyPeerTask(ctx, tiny)
		logger.Infof("copied tasks data %d bytes to buffer", len(tiny.Content))
		tiny.span.SetAttributes(config.AttributePeerTaskSuccess.Bool(true))
		return ioutil.NopCloser(bytes.NewBuffer(tiny.Content)), map[string]string{
			headers.ContentLength: fmt.Sprintf("%d", len(tiny.Content)),
		}, nil
	}

	pt.SetCallback(
		&streamPeerTaskCallback{
			ptm:   ptm,
			pt:    pt,
			req:   req,
			start: start,
		})

	ptm.runningPeerTasks.Store(req.PeerId, pt)

	// FIXME when failed due to schedulerClient error, relocate schedulerClient and retry
	readCloser, attribute, err := pt.Start(ctx)
	return readCloser, attribute, err
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

func (ptm *peerTaskManager) storeTinyPeerTask(ctx context.Context, tiny *TinyData) {
	// TODO store tiny data asynchronous
	l := int64(len(tiny.Content))
	err := ptm.storageManager.RegisterTask(ctx,
		storage.RegisterTaskRequest{
			CommonTaskRequest: storage.CommonTaskRequest{
				PeerID: tiny.PeerID,
				TaskID: tiny.TaskID,
			},
			ContentLength: l,
			TotalPieces:   1,
			// TODO check md5 digest
		})
	if err != nil {
		logger.Errorf("register tiny data storage failed: %s", err)
		return
	}
	n, err := ptm.storageManager.WritePiece(ctx,
		&storage.WritePieceRequest{
			PeerTaskMetaData: storage.PeerTaskMetaData{
				PeerID: tiny.PeerID,
				TaskID: tiny.TaskID,
			},
			PieceMetaData: storage.PieceMetaData{
				Num:    0,
				Md5:    "",
				Offset: 0,
				Range: clientutil.Range{
					Start:  0,
					Length: l,
				},
				Style: 0,
			},
			UnknownLength: false,
			Reader:        bytes.NewBuffer(tiny.Content),
		})
	if err != nil {
		logger.Errorf("write tiny data storage failed: %s", err)
		return
	}
	if n != l {
		logger.Errorf("write tiny data storage failed", n, l)
		return
	}
	err = ptm.storageManager.Store(ctx,
		&storage.StoreRequest{
			CommonTaskRequest: storage.CommonTaskRequest{
				PeerID: tiny.PeerID,
				TaskID: tiny.TaskID,
			},
			MetadataOnly: true,
			TotalPieces:  1,
		})
	if err != nil {
		logger.Errorf("store tiny data failed: %s", err)
	} else {
		logger.Debugf("store tiny data, len: %d", l)
	}
}
