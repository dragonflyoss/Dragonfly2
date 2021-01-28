package peer

import (
	"context"
	"fmt"
	"io"
	"sync"
	"sync/atomic"

	"github.com/dragonflyoss/Dragonfly2/client/daemon/storage"
	logger "github.com/dragonflyoss/Dragonfly2/pkg/dflog"
	"github.com/dragonflyoss/Dragonfly2/pkg/rpc/base"
	"github.com/dragonflyoss/Dragonfly2/pkg/rpc/scheduler"
	schedulerclient "github.com/dragonflyoss/Dragonfly2/pkg/rpc/scheduler/client"
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
			ctx:                ctx,
			host:               host,
			schedPieceResultCh: schedPieceResultCh,
			schedPeerPacketCh:  schedPeerPacketCh,
			pieceManager:       pieceManager,
			peerClientReady:    make(chan bool),
			peerId:             request.PeerId,
			taskId:             result.TaskId,
			done:               make(chan struct{}),
			doneOnce:           sync.Once{},
			bitmap:             NewBitmap(),
			lock:               &sync.Mutex{},
			log:                logger.With("peer", request.PeerId, "task", result.TaskId, "component", "streamPeerTask"),
			failedPieceCh:      make(chan int32, 4),
			contentLength:      -1,
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
		s.base.schedPieceResultCh <- pieceResult
		s.base.failedPieceCh <- pieceResult.PieceNum
		return nil
	}
	pieceResult.FinishedCount = s.base.bitmap.Settled()
	s.base.schedPieceResultCh <- pieceResult
	s.successPieceCh <- piece.PieceNum
	s.base.log.Debugf("success piece %d sent", piece.PieceNum)
	select {
	case <-s.base.ctx.Done():
		s.base.log.Warnf("peer task context done due to %s", s.base.ctx.Err())
		return s.base.ctx.Err()
	default:
	}

	s.base.lock.Lock()
	defer s.base.lock.Unlock()
	if s.base.bitmap.IsSet(pieceResult.PieceNum) {
		s.base.log.Warnf("piece %d is already reported, skipped", pieceResult.PieceNum)
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
	go s.base.receivePeerPacket()
	go s.base.pullPiecesFromPeers(s, s.cleanUnfinished)
	r, w := io.Pipe()
	go func() {
		var (
			desired int32
			wrote   int64
			err     error
			cache   = make(map[int32]bool)
		)
		for {
			select {
			case <-s.base.done:
				for {
					// all data is wrote to local storage, and all data is wrote to pipe write
					if s.base.bitmap.Settled() == desired {
						w.Close()
						return
					}
					wrote, err = s.writeTo(w, desired)
					if err != nil {
						s.base.log.Errorf("write to pipe error: %s", err)
						_ = w.CloseWithError(err)
						return
					}
					s.base.log.Debugf("wrote piece %d to pipe, size %d", desired, wrote)
					desired++
				}
			case num, ok := <-s.successPieceCh:
				if !ok {
					s.base.log.Warnf("successPieceCh closed")
					continue
				}
				// not desired piece, cache it
				if desired != num {
					cache[num] = true
					if num < desired {
						s.base.log.Warnf("piece number should be equal or greater than %d, received piece number: %d",
							desired, num)
					}
					continue
				}
				for {
					wrote, err = s.writeTo(w, desired)
					if err != nil {
						s.base.log.Errorf("write to pipe error: %s", err)
						_ = w.CloseWithError(err)
						return
					}
					s.base.log.Debugf("wrote piece %d to pipe, size %d", desired, wrote)
					desired++
					cached := cache[desired]
					if !cached {
						break
					}
					delete(cache, desired)
				}
			}
		}
	}()
	// FIXME(jim) update attribute
	return r, nil, nil
}

func (s *streamPeerTask) finish() error {
	var err error
	// send last progress
	s.base.doneOnce.Do(func() {
		// send EOF piece result to scheduler
		s.base.schedPieceResultCh <- scheduler.NewEndPieceResult(s.base.bitmap.Settled(), s.base.taskId, s.base.peerId)
		s.base.log.Debugf("end piece result sent")
		// callback to store data to output
		err = s.base.callback.Done(s)
		close(s.base.done)
	})
	return err
}

func (s *streamPeerTask) cleanUnfinished() {
	// send last progress
	s.base.doneOnce.Do(func() {
		// send EOF piece result to scheduler
		s.base.schedPieceResultCh <- scheduler.NewEndPieceResult(s.base.bitmap.Settled(), s.base.taskId, s.base.peerId)
		s.base.log.Debugf("end piece result sent")
		close(s.base.done)
	})
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
