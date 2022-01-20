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

	"github.com/go-http-utils/headers"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/time/rate"

	"d7y.io/dragonfly/v2/client/config"
	"d7y.io/dragonfly/v2/client/daemon/metrics"
	"d7y.io/dragonfly/v2/client/daemon/storage"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/idgen"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	"d7y.io/dragonfly/v2/pkg/rpc/scheduler"
)

type StreamTaskRequest struct {
	// universal resource locator for different kind of storage
	URL string
	// url meta info
	URLMeta *base.UrlMeta
	// peer's id and must be global uniqueness
	PeerID string
}

// StreamTask represents a peer task with stream io for reading directly without once more disk io
type StreamTask interface {
	// Start starts the special peer task, return an io.Reader for stream io
	// when all data transferred, reader return an io.EOF
	// attribute stands some extra data, like HTTP response Header
	Start(ctx context.Context) (rc io.ReadCloser, attribute map[string]string, err error)
}

type streamTask struct {
	*logger.SugaredLoggerOnWith
	ctx               context.Context
	span              trace.Span
	peerTaskConductor *peerTaskConductor
	pieceCh           chan *pieceInfo
}

func (ptm *peerTaskManager) newStreamTask(
	ctx context.Context,
	request *scheduler.PeerTaskRequest) (*streamTask, error) {
	metrics.StreamTaskCount.Add(1)
	var limit = rate.Inf
	if ptm.perPeerRateLimit > 0 {
		limit = ptm.perPeerRateLimit
	}
	ptc, err := ptm.getPeerTaskConductor(ctx, idgen.TaskID(request.Url, request.UrlMeta), request, limit)
	if err != nil {
		return nil, err
	}
	ctx, span := tracer.Start(ctx, config.SpanStreamTask, trace.WithSpanKind(trace.SpanKindClient))
	pt := &streamTask{
		SugaredLoggerOnWith: ptc.SugaredLoggerOnWith,
		ctx:                 ctx,
		span:                span,
		peerTaskConductor:   ptc,
		pieceCh:             ptc.broker.Subscribe(),
	}
	return pt, nil
}

func (s *streamTask) Start(ctx context.Context) (io.ReadCloser, map[string]string, error) {
	// wait first piece to get content length and attribute (eg, response header for http/https)
	var firstPiece *pieceInfo

	attr := map[string]string{}
	attr[config.HeaderDragonflyTask] = s.peerTaskConductor.taskID
	attr[config.HeaderDragonflyPeer] = s.peerTaskConductor.peerID
	select {
	case <-ctx.Done():
		s.Errorf("%s", ctx.Err())
		s.span.RecordError(ctx.Err())
		s.span.End()
		return nil, attr, ctx.Err()
	case <-s.peerTaskConductor.failCh:
		return nil, attr, fmt.Errorf("peer task failed: %d/%s",
			s.peerTaskConductor.failedCode, s.peerTaskConductor.failedReason)
	case <-s.peerTaskConductor.successCh:
		rc, err := s.peerTaskConductor.peerTaskManager.storageManager.ReadAllPieces(
			ctx,
			&storage.PeerTaskMetadata{
				PeerID: s.peerTaskConductor.peerID,
				TaskID: s.peerTaskConductor.taskID,
			})
		return rc, attr, err
	case first := <-s.pieceCh:
		firstPiece = first
	}

	if s.peerTaskConductor.GetContentLength() != -1 {
		attr[headers.ContentLength] = fmt.Sprintf("%d", s.peerTaskConductor.GetContentLength())
	} else {
		attr[headers.TransferEncoding] = "chunked"
	}

	pr, pw := io.Pipe()
	var readCloser io.ReadCloser = pr
	go s.writeToPipe(firstPiece, pw)

	return readCloser, attr, nil
}

func (s *streamTask) writeOnePiece(w io.Writer, pieceNum int32) (int64, error) {
	pr, pc, err := s.peerTaskConductor.storage.ReadPiece(s.ctx, &storage.ReadPieceRequest{
		PeerTaskMetadata: storage.PeerTaskMetadata{
			PeerID: s.peerTaskConductor.peerID,
			TaskID: s.peerTaskConductor.taskID,
		},
		PieceMetadata: storage.PieceMetadata{
			Num: pieceNum,
		},
	})
	if err != nil {
		return 0, err
	}
	n, err := io.Copy(w, pr)
	if err != nil {
		pc.Close()
		return n, err
	}
	return n, pc.Close()
}

func (s *streamTask) writeToPipe(firstPiece *pieceInfo, pw *io.PipeWriter) {
	defer func() {
		s.span.End()
	}()
	var (
		desired int32
		cur     *pieceInfo
		wrote   int64
		err     error
		cache   = make(map[int32]bool)
	)
	// update first piece to cache and check cur with desired
	cache[firstPiece.num] = true
	cur = firstPiece
	for {
		if desired == cur.num {
			for {
				delete(cache, desired)
				_, span := tracer.Start(s.ctx, config.SpanWriteBackPiece)
				span.SetAttributes(config.AttributePiece.Int(int(desired)))
				wrote, err = s.writeOnePiece(pw, desired)
				if err != nil {
					span.RecordError(err)
					span.End()
					s.Errorf("write to pipe error: %s", err)
					_ = pw.CloseWithError(err)
					return
				}
				span.SetAttributes(config.AttributePieceSize.Int(int(wrote)))
				s.Debugf("wrote piece %d to pipe, size %d", desired, wrote)
				span.End()
				desired++
				cached := cache[desired]
				if !cached {
					break
				}
			}
		} else {
			// not desired piece, cache it
			cache[cur.num] = true
			if cur.num < desired {
				s.Warnf("piece number should be equal or greater than %d, received piece number: %d", desired, cur.num)
			}
		}

		select {
		case <-s.ctx.Done():
			s.Errorf("context done due to: %s", s.ctx.Err())
			s.span.RecordError(s.ctx.Err())
			if err = pw.CloseWithError(s.ctx.Err()); err != nil {
				s.Errorf("CloseWithError failed: %s", err)
			}
			return
		case cur = <-s.pieceCh:
			// FIXME check missing piece for non-block broker channel
			continue
		case <-s.peerTaskConductor.failCh:
			ptError := fmt.Errorf("context done due to peer task fail: %d/%s",
				s.peerTaskConductor.failedCode, s.peerTaskConductor.failedReason)
			s.Error(ptError.Error())
			s.span.RecordError(ptError)
			if err = pw.CloseWithError(ptError); err != nil {
				s.Errorf("CloseWithError failed: %s", err)
			}
			return
		case <-s.peerTaskConductor.successCh:
			for {
				// all data wrote to local storage, and all data wrote to pipe write
				if s.peerTaskConductor.readyPieces.Settled() == desired {
					s.Debugf("all %d pieces wrote to pipe", desired)
					pw.Close()
					return
				}
				_, span := tracer.Start(s.ctx, config.SpanWriteBackPiece)
				span.SetAttributes(config.AttributePiece.Int(int(desired)))
				wrote, err = s.writeOnePiece(pw, desired)
				if err != nil {
					span.RecordError(err)
					span.End()
					s.span.RecordError(err)
					s.Errorf("write to pipe error: %s", err)
					_ = pw.CloseWithError(err)
					return
				}
				span.SetAttributes(config.AttributePieceSize.Int(int(wrote)))
				span.End()
				s.Debugf("wrote piece %d to pipe, size %d", desired, wrote)
				desired++
			}
		}
	}
}
