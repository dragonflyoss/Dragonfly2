package peer

import (
	"context"
	"fmt"
	"io"
	"sync"
	"sync/atomic"

	"github.com/go-http-utils/headers"
	"github.com/pkg/errors"

	"d7y.io/dragonfly/v2/client/daemon/storage"
	"d7y.io/dragonfly/v2/pkg/dfcodes"
	logger "d7y.io/dragonfly/v2/pkg/dflog"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	"d7y.io/dragonfly/v2/pkg/rpc/scheduler"
	schedulerclient "d7y.io/dragonfly/v2/pkg/rpc/scheduler/client"
)

// StreamPeerTask represents a peer task with stream io for reading directly without once more disk io
type StreamPeerTask interface {
	PeerTask
	// Start start the special peer task, return a io.Reader for stream io
	// when all data transferred, reader return a io.EOF
	// attribute stands some extra data, like HTTP response Header
	Start(ctx context.Context) (reader io.Reader, attribute map[string]string, err error)
}

type streamPeerTask struct {
	base           filePeerTask
	successPieceCh chan int32
}

func NewStreamPeerTask(ctx context.Context,
	host *scheduler.PeerHost,
	schedulerClient schedulerclient.SchedulerClient,
	pieceManager PieceManager,
	request *scheduler.PeerTaskRequest) (StreamPeerTask, error) {
	result, err := schedulerClient.RegisterPeerTask(ctx, request)
	if err != nil {
		logger.Errorf("register peer task failed: %s, peer id: %s", err, request.PeerId)
		return nil, err
	}
	if !result.State.Success {
		return nil, fmt.Errorf("regist error: %s/%s", result.State.Code, result.State.Msg)
	}
	schedPieceResultCh, schedPeerPacketCh, err := schedulerClient.ReportPieceResult(ctx, result.TaskId, request)
	if err != nil {
		return nil, err
	}
	logger.Infof("register task success, task id: %s, peer id: %s, SizeScope: %s",
		result.TaskId, request.PeerId, base.SizeScope_name[int32(result.SizeScope)])
	return &streamPeerTask{
		base: filePeerTask{
			ctx:             ctx,
			host:            host,
			backSource:      result.State.Code == dfcodes.BackSource,
			request:         request,
			pieceResultCh:   schedPieceResultCh,
			peerPacketCh:    schedPeerPacketCh,
			pieceManager:    pieceManager,
			peerPacketReady: make(chan bool),
			peerId:          request.PeerId,
			taskId:          result.TaskId,
			done:            make(chan struct{}),
			once:            sync.Once{},
			bitmap:          NewBitmap(),
			lock:            &sync.Mutex{},
			failedPieceCh:   make(chan int32, 4),
			contentLength:   -1,

			SugaredLoggerOnWith: logger.With("peer", request.PeerId, "task", result.TaskId, "component", "streamPeerTask"),
		},
		successPieceCh: make(chan int32, 4),
	}, nil
}

func (s *streamPeerTask) GetPeerID() string {
	return s.base.GetPeerID()
}

func (s *streamPeerTask) GetTaskID() string {
	return s.base.GetTaskID()
}

func (s *streamPeerTask) GetContentLength() int64 {
	return s.base.GetContentLength()
}

func (s *streamPeerTask) SetCallback(callback PeerTaskCallback) {
	s.base.SetCallback(callback)
}

func (s *streamPeerTask) AddTraffic(n int64) {
	s.base.AddTraffic(n)
}

func (s *streamPeerTask) GetTraffic() int64 {
	return s.base.GetTraffic()
}

func (s *streamPeerTask) ReportPieceResult(piece *base.PieceInfo, pieceResult *scheduler.PieceResult) error {
	// FIXME goroutine safe for channel and send on closed channel
	defer func() {
		if r := recover(); r != nil {
			logger.Warnf("recover from %s", r)
		}
	}()
	// retry failed piece
	if !pieceResult.Success {
		s.base.pieceResultCh <- pieceResult
		s.base.failedPieceCh <- pieceResult.PieceNum
		return nil
	}
	pieceResult.FinishedCount = s.base.bitmap.Settled()
	s.base.pieceResultCh <- pieceResult
	s.successPieceCh <- piece.PieceNum
	s.base.Debugf("success piece %d sent", piece.PieceNum)
	select {
	case <-s.base.ctx.Done():
		s.base.Warnf("peer task context done due to %s", s.base.ctx.Err())
		return s.base.ctx.Err()
	default:
	}

	s.base.lock.Lock()
	defer s.base.lock.Unlock()
	if s.base.bitmap.IsSet(pieceResult.PieceNum) {
		s.base.Warnf("piece %d is already reported, skipped", pieceResult.PieceNum)
		return nil
	}
	// mark piece processed
	s.base.bitmap.Set(pieceResult.PieceNum)
	atomic.AddInt64(&s.base.completedLength, int64(piece.RangeSize))

	if !s.base.isCompleted() {
		return nil
	}

	return s.finish()
}

func (s *streamPeerTask) Start(ctx context.Context) (io.Reader, map[string]string, error) {
	s.base.ctx, s.base.cancel = context.WithCancel(ctx)
	if s.base.backSource {
		go func() {
			s.base.contentLength = -1
			_ = s.base.callback.Init(s)
			err := s.base.pieceManager.DownloadSource(ctx, s, s.base.request)
			if err != nil {
				s.base.cancel()
				s.base.Errorf("download from source error: %s", err)
				s.cleanUnfinished()
				return
			}
			s.base.Debugf("download from source ok")
			s.finish()
		}()
	} else {
		go s.base.receivePeerPacket()
		go s.base.pullPiecesFromPeers(s, s.cleanUnfinished)
	}

	// wait first piece to get content length and attribute (eg, response header for http/https)
	var firstPiece int32
	select {
	case <-s.base.ctx.Done():
		err := errors.Errorf("ctx.Done due to: %s", s.base.ctx.Err())
		s.base.Errorf("%s", err)
		return nil, nil, err
	case <-s.base.done:
		err := errors.New("early done")
		return nil, nil, err
	case num, ok := <-s.successPieceCh:
		if !ok {
			err := errors.New("early done")
			s.base.Warnf("successPieceCh closed")
			return nil, nil, err
		}
		firstPiece = num
	}

	pr, pw := io.Pipe()
	attr := map[string]string{}
	var reader io.Reader = pr
	var writer io.Writer = pw
	if s.base.contentLength != -1 {
		attr[headers.ContentLength] = fmt.Sprintf("%d", s.base.contentLength)
	} else {
		attr[headers.TransferEncoding] = "chunked"
	}

	go func(first int32) {
		var (
			desired int32
			cur     int32
			wrote   int64
			err     error
			ok      bool
			cache   = make(map[int32]bool)
		)
		// update first piece to cache and check cur with desired
		cache[first] = true
		cur = first
		for {
			if desired == cur {
				for {
					delete(cache, desired)
					wrote, err = s.writeTo(writer, desired)
					if err != nil {
						s.base.Errorf("write to pipe error: %s", err)
						_ = pw.CloseWithError(err)
						return
					}
					s.base.Debugf("wrote piece %d to pipe, size %d", desired, wrote)
					desired++
					cached := cache[desired]
					if !cached {
						break
					}
				}
			} else {
				// not desired piece, cache it
				cache[cur] = true
				if cur < desired {
					s.base.Warnf("piece number should be equal or greater than %d, received piece number: %d",
						desired, cur)
				}
			}

			select {
			case <-s.base.ctx.Done():
				s.base.Errorf("ctx.Done due to: %s", s.base.ctx.Err())
				if err := pw.CloseWithError(s.base.ctx.Err()); err != nil {
					s.base.Errorf("CloseWithError failed: %s", err)
				}
				return
			case <-s.base.done:
				for {
					// all data is wrote to local storage, and all data is wrote to pipe write
					if s.base.bitmap.Settled() == desired {
						pw.Close()
						return
					}
					wrote, err = s.writeTo(pw, desired)
					if err != nil {
						s.base.Errorf("write to pipe error: %s", err)
						_ = pw.CloseWithError(err)
						return
					}
					s.base.Debugf("wrote piece %d to pipe, size %d", desired, wrote)
					desired++
				}
			case cur, ok = <-s.successPieceCh:
				if !ok {
					s.base.Warnf("successPieceCh closed")
					continue
				}
			}
		}
	}(firstPiece)

	return reader, attr, nil
}

func (s *streamPeerTask) finish() error {
	// send last progress
	s.base.once.Do(func() {
		// send EOF piece result to scheduler
		s.base.pieceResultCh <- scheduler.NewEndPieceResult(s.base.taskId, s.base.peerId, s.base.bitmap.Settled())
		s.base.Debugf("end piece result sent")
		// async callback to store meta data
		go func() {
			if err := s.base.callback.Done(s); err != nil {
				s.base.Errorf("callback done error: %s", err)
			}
		}()
		close(s.base.done)
	})
	return nil
}

func (s *streamPeerTask) cleanUnfinished() {
	// send last progress
	s.base.once.Do(func() {
		// send EOF piece result to scheduler
		s.base.pieceResultCh <- scheduler.NewEndPieceResult(s.base.taskId, s.base.peerId, s.base.bitmap.Settled())
		s.base.Debugf("end piece result sent")
		close(s.base.done)
	})
}

func (s *streamPeerTask) SetContentLength(i int64) error {
	s.base.contentLength = i
	if !s.base.isCompleted() {
		return nil
	}
	return s.finish()
}

func (s *streamPeerTask) writeTo(w io.Writer, pieceNum int32) (int64, error) {
	pr, pc, err := s.base.pieceManager.ReadPiece(s.base.ctx, &storage.ReadPieceRequest{
		PeerTaskMetaData: storage.PeerTaskMetaData{
			PeerID: s.base.peerId,
			TaskID: s.base.taskId,
		},
		PieceMetaData: storage.PieceMetaData{
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
