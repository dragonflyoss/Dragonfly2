package peer

import (
	"context"
	"fmt"
	"io"
	"sync"
	"sync/atomic"

	"github.com/go-http-utils/headers"
	"github.com/pkg/errors"

	"d7y.io/dragonfly/v2/client/config"
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
	pieceManager PieceManager,
	request *scheduler.PeerTaskRequest,
	schedulerClient schedulerclient.SchedulerClient,
	schedulerOption config.SchedulerOption) (StreamPeerTask, error) {
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
			backSource:      result.State.Code == dfcodes.SchedNeedBackSource,
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
			failedReason:    "unknown",
			contentLength:   -1,
			totalPiece:      -1,
			schedulerOption: schedulerOption,

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

func (s *streamPeerTask) GetTotalPieces() int32 {
	return s.base.totalPiece
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

	s.base.lock.Lock()
	if s.base.bitmap.IsSet(pieceResult.PieceNum) {
		s.base.lock.Unlock()
		s.base.Warnf("piece %d is already reported, skipped", pieceResult.PieceNum)
		return nil
	}
	// mark piece processed
	s.base.bitmap.Set(pieceResult.PieceNum)
	atomic.AddInt64(&s.base.completedLength, int64(piece.RangeSize))
	s.base.lock.Unlock()

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

	if !s.base.isCompleted() {
		return nil
	}

	return s.finish()
}

func (s *streamPeerTask) Start(ctx context.Context) (io.Reader, map[string]string, error) {
	s.base.ctx, s.base.cancel = context.WithTimeout(ctx, s.base.schedulerOption.ScheduleTimeout.Duration)
	if s.base.backSource {
		go func() {
			s.base.contentLength = -1
			_ = s.base.callback.Init(s)
			err := s.base.pieceManager.DownloadSource(ctx, s, s.base.request)
			if err != nil {
				s.base.Errorf("download from source error: %s", err)
				s.cleanUnfinished()
				return
			}
			s.base.Debugf("download from source ok")
			_ = s.finish()
		}()
	} else {
		go s.base.receivePeerPacket()
		go s.base.pullPiecesFromPeers(s, s.cleanUnfinished)
	}

	// wait first piece to get content length and attribute (eg, response header for http/https)
	var firstPiece int32
	select {
	case <-s.base.ctx.Done():
		err := errors.Errorf("ctx.PeerTaskDone due to: %s", s.base.ctx.Err())
		s.base.Errorf("%s", err)
		return nil, nil, err
	case <-s.base.done:
		err := errors.New("stream peer task early done")
		return nil, nil, err
	case first := <-s.successPieceCh:
		//if !ok {
		//	s.base.Warnf("successPieceCh closed unexpect")
		//	return nil, nil, errors.New("early done")
		//}
		firstPiece = first
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
		defer s.base.cancel()
		var (
			desired int32
			cur     int32
			wrote   int64
			err     error
			//ok      bool
			cache = make(map[int32]bool)
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
				s.base.Errorf("ctx.PeerTaskDone due to: %s", s.base.ctx.Err())
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
			case cur = <-s.successPieceCh:
				continue
				//if !ok {
				//	s.base.Warnf("successPieceCh closed")
				//	continue
				//}
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
		close(s.base.done)
		//close(s.successPieceCh)
		if err := s.base.callback.Done(s); err != nil {
			s.base.Errorf("done callback error: %s", err)
		}
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
		//close(s.successPieceCh)
		if err := s.base.callback.Fail(s, s.base.failedReason); err != nil {
			s.base.Errorf("fail callback error: %s", err)
		}
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
