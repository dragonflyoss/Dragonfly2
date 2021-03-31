package peer

import (
	"context"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"d7y.io/dragonfly/v2/client/config"
	"d7y.io/dragonfly/v2/pkg/dfcodes"
	"d7y.io/dragonfly/v2/pkg/dferrors"
	logger "d7y.io/dragonfly/v2/pkg/dflog"
	"d7y.io/dragonfly/v2/pkg/rpc"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	dfclient "d7y.io/dragonfly/v2/pkg/rpc/dfdaemon/client"
	"d7y.io/dragonfly/v2/pkg/rpc/scheduler"
	schedulerclient "d7y.io/dragonfly/v2/pkg/rpc/scheduler/client"
)

const (
	reasonScheduleTimeout       = "wait first peer packet from scheduler timeout"
	reasonReScheduleTimeout     = "wait more available peers from scheduler timeout"
	reasonContextCanceled       = "context canceled"
	reasonPeerGoneFromScheduler = "scheduler says client should disconnect"

	failedCodeNotSet = 0
)

var errPeerPacketChanged = errors.New("peer packet changed")

type peerTask struct {
	*logger.SugaredLoggerOnWith
	ctx    context.Context
	cancel context.CancelFunc

	// backSource indicates downloading resource from instead of other peers
	backSource bool
	request    *scheduler.PeerTaskRequest

	// pieceManager will be used for downloading piece
	pieceManager PieceManager
	// host info about current host
	host *scheduler.PeerHost
	// callback holds some actions, like init, done, fail actions
	callback PeerTaskCallback

	// schedule options
	schedulerOption config.SchedulerOption

	// peer task meta info
	peerId          string
	taskId          string
	contentLength   int64
	totalPiece      int32
	completedLength int64
	usedTraffic     int64

	//sizeScope   base.SizeScope
	singlePiece *scheduler.SinglePiece

	// TODO peerPacketStream
	peerPacketStream schedulerclient.PeerPacketStream
	// peerPacket is the latest available peers from peerPacketCh
	peerPacket *scheduler.PeerPacket
	// peerPacketReady will receive a ready signal for peerPacket ready
	peerPacketReady chan bool
	// pieceParallelCount stands the piece parallel count from peerPacket
	pieceParallelCount int32

	// done channel will be close when peer task is finished
	done chan struct{}
	// peerTaskDone will be true after peer task done
	peerTaskDone bool

	// same actions must be done only once, like close done channel and so on
	once sync.Once

	// failedPieceCh will hold all pieces which download failed,
	// those pieces will be retry later
	failedPieceCh chan int32
	// failedReason will be set when peer task failed
	failedReason string
	// failedReason will be set when peer task failed
	failedCode base.Code

	// readyPieces stands all pieces download status
	readyPieces *Bitmap
	// requestedPieces stands all pieces requested from peers
	requestedPieces *Bitmap
	// lock used by piece result manage, when update readyPieces, lock first
	lock sync.Locker
}

func (pt *peerTask) ReportPieceResult(pieceTask *base.PieceInfo, pieceResult *scheduler.PieceResult) error {
	panic("implement me")
}

func (pt *peerTask) SetCallback(callback PeerTaskCallback) {
	pt.callback = callback
}

func (pt *peerTask) GetPeerID() string {
	return pt.peerId
}

func (pt *peerTask) GetTaskID() string {
	return pt.taskId
}

func (pt *peerTask) GetContentLength() int64 {
	return pt.contentLength
}

func (pt *peerTask) SetContentLength(i int64) error {
	panic("implement me")
}

func (pt *peerTask) AddTraffic(n int64) {
	atomic.AddInt64(&pt.usedTraffic, n)
}

func (pt *peerTask) GetTraffic() int64 {
	return pt.usedTraffic
}

func (pt *peerTask) GetTotalPieces() int32 {
	return pt.totalPiece
}

func (pt *peerTask) Context() context.Context {
	return pt.ctx
}

func (pt *peerTask) Log() *logger.SugaredLoggerOnWith {
	return pt.SugaredLoggerOnWith
}

func (pt *peerTask) pullPieces(pti PeerTask, cleanUnfinishedFunc func()) {
	// when there is a single piece, try to download first
	if pt.singlePiece != nil {
		go pt.pullSinglePiece(pti, cleanUnfinishedFunc)
	} else {
		go pt.receivePeerPacket()
		go pt.pullPiecesFromPeers(pti, cleanUnfinishedFunc)
	}
}

func (pt *peerTask) receivePeerPacket() {
	var (
		peerPacket *scheduler.PeerPacket
		err        error
	)
loop:
	for {
		select {
		case <-pt.ctx.Done():
			pt.Debugf("context done due to %s", pt.ctx.Err())
			break loop
		case <-pt.done:
			pt.Infof("peer task done, stop wait peer packet from scheduler")
			break loop
		default:
		}

		peerPacket, err = pt.peerPacketStream.Recv()
		if err == io.EOF {
			pt.Debugf("peerPacketStream closed")
			break loop
		}
		if err != nil {
			pt.failedCode = dfcodes.UnknownError
			if de, ok := err.(*dferrors.DfError); ok {
				pt.failedCode = de.Code
				pt.failedReason = de.Message
			}
			pt.cancel()
			pt.Errorf(pt.failedReason)
			break loop
		}

		if peerPacket.Code == dfcodes.SchedPeerGone {
			pt.failedReason = reasonPeerGoneFromScheduler
			pt.failedCode = dfcodes.SchedPeerGone
			pt.cancel()
			pt.Errorf(pt.failedReason)
			break
		}

		if peerPacket.Code != dfcodes.Success {
			pt.Errorf("receive peer packet with error: %d", peerPacket.Code)
			// TODO when receive error, cancel ?
			// pt.cancel()
			continue
		}

		if peerPacket.MainPeer == nil && peerPacket.StealPeers == nil {
			pt.Warnf("scheduler client send a peerPacket with empty peers")
			continue
		}
		pt.Debugf("receive new peer packet, main peer: %s, parallel count: %d",
			peerPacket.MainPeer.PeerId, peerPacket.ParallelCount)

		pt.peerPacket = peerPacket
		pt.pieceParallelCount = pt.peerPacket.ParallelCount
		select {
		case pt.peerPacketReady <- true:
		case <-pt.ctx.Done():
			pt.Debugf("context done due to %s", pt.ctx.Err())
			break loop
		case <-pt.done:
			pt.Infof("peer task done, stop wait peer packet from scheduler")
			break loop
		default:
		}
	}
	close(pt.peerPacketReady)
}

func (pt *peerTask) pullSinglePiece(pti PeerTask, cleanUnfinishedFunc func()) {
	pt.Infof("single piece, dest peer id: %s, piece num: %d, size: %d",
		pt.singlePiece.DstPid, pt.singlePiece.PieceInfo.PieceNum, pt.singlePiece.PieceInfo.RangeSize)

	pt.contentLength = int64(pt.singlePiece.PieceInfo.RangeSize)
	if err := pt.callback.Init(pti); err != nil {
		pt.failedReason = err.Error()
		pt.failedCode = dfcodes.ClientError
		cleanUnfinishedFunc()
		return
	}

	request := &DownloadPieceRequest{
		TaskID:  pt.GetTaskID(),
		DstPid:  pt.singlePiece.DstPid,
		DstAddr: pt.singlePiece.DstAddr,
		piece:   pt.singlePiece.PieceInfo,
	}
	if pt.pieceManager.DownloadPiece(pti, request) {
		pt.Infof("single piece download success")
	} else {
		// fallback to download from other peers
		pt.Warnf("single piece download failed, switch to download from other peers")
		go pt.receivePeerPacket()
		pt.pullPiecesFromPeers(pti, cleanUnfinishedFunc)
	}
}

// TODO when main peer is not available, switch to steel peers
// piece manager need peer task interface, pti make it compatibility for stream peer task
func (pt *peerTask) pullPiecesFromPeers(pti PeerTask, cleanUnfinishedFunc func()) {
	defer func() {
		close(pt.failedPieceCh)
		cleanUnfinishedFunc()
	}()
	// wait available peer daemon
	select {
	case <-pt.peerPacketReady:
		// preparePieceTasksByPeer func already send piece result with error
		pt.Infof("new peer client ready, scheduler time cost: %dus",
			time.Now().Sub(pt.callback.GetStartTime()).Microseconds())
	case <-time.After(pt.schedulerOption.ScheduleTimeout.Duration):
		pt.failedReason = reasonScheduleTimeout
		pt.failedCode = dfcodes.ClientScheduleTimeout
		pt.Errorf(pt.failedReason)
		return
	}
	var (
		num            int32
		limit          int32
		initialized    bool
		pieceRequestCh chan *DownloadPieceRequest
	)
loop:
	for {
		limit = pt.pieceParallelCount
		// check whether catch exit signal or get a failed piece
		// if nothing got, process normal pieces
		select {
		case <-pt.done:
			pt.Infof("peer task done, stop get pieces from peer")
			break loop
		case <-pt.ctx.Done():
			pt.Debugf("context done due to %s", pt.ctx.Err())
			if !pt.peerTaskDone {
				if pt.failedCode == failedCodeNotSet {
					pt.failedReason = reasonContextCanceled
					pt.failedCode = dfcodes.ClientContextCanceled
					pt.callback.Fail(pt, pt.failedCode, pt.ctx.Err().Error())
				} else {
					pt.callback.Fail(pt, pt.failedCode, pt.failedReason)
				}
			}
			break loop
		case failed := <-pt.failedPieceCh:
			pt.Warnf("download piece/%d failed, retry", failed)
			num = failed
			limit = 1
		default:
		}

		pt.Debugf("try to get pieces, number: %d, limit: %d", num, limit)
		piecePacket, err := pt.preparePieceTasks(
			&base.PieceTaskRequest{
				TaskId:   pt.taskId,
				SrcPid:   pt.peerId,
				StartNum: num,
				Limit:    limit,
			})

		if err != nil {
			pt.Warnf("get piece task error: %s, wait available peers from scheduler", err)
			select {
			// when peer task without content length or total pieces count, match here
			case <-pt.done:
				pt.Infof("peer task done, stop get pieces from peer")
			case <-pt.ctx.Done():
				pt.Debugf("context done due to %s", pt.ctx.Err())
				if !pt.peerTaskDone {
					if pt.failedCode == failedCodeNotSet {
						pt.failedReason = reasonContextCanceled
						pt.failedCode = dfcodes.ClientContextCanceled
					}
				}
			case <-pt.peerPacketReady:
				// preparePieceTasksByPeer func already send piece result with error
				pt.Infof("new peer client ready")
				continue loop
			case <-time.After(pt.schedulerOption.ScheduleTimeout.Duration):
				pt.failedReason = reasonReScheduleTimeout
				pt.failedCode = dfcodes.ClientScheduleTimeout
				pt.Errorf(pt.failedReason)
			}
			// only <-pt.peerPacketReady continue loop, others break
			break loop
		}

		if !initialized {
			pt.contentLength = piecePacket.ContentLength
			initialized = true
			if err = pt.callback.Init(pt); err != nil {
				pt.failedReason = err.Error()
				pt.failedCode = dfcodes.ClientError
				break loop
			}
			pc := pt.peerPacket.ParallelCount
			if pc <= 4 {
				pc = 4
			}
			pieceRequestCh = make(chan *DownloadPieceRequest, pc*2)
			for i := int32(0); i < pc; i++ {
				go pt.downloadPieceWorker(i, pti, pieceRequestCh)
			}
		}

		// update total piece
		if piecePacket.TotalPiece > pt.totalPiece {
			pt.totalPiece = piecePacket.TotalPiece
			_ = pt.callback.Update(pt)
		}

		// trigger DownloadPiece
		for _, piece := range piecePacket.PieceInfos {
			pt.Infof("get piece %d from %s", piece.PieceNum, piecePacket.DstPid)
			if !pt.requestedPieces.IsSet(piece.PieceNum) {
				pt.requestedPieces.Set(piece.PieceNum)
			}
			pieceRequestCh <- &DownloadPieceRequest{
				TaskID:  pt.GetTaskID(),
				DstPid:  piecePacket.DstPid,
				DstAddr: piecePacket.DstAddr,
				piece:   piece,
			}
		}

		num = pt.getNextPieceNum(num, limit)
		if num == -1 {
			pt.Infof("all pieces requests send, just wait failed pieces")
			if pt.isCompleted() {
				break loop
			}
			// use no default branch select to wait failed piece or exit
			select {
			case <-pt.done:
				pt.Infof("peer task done, stop get pieces from peer")
				break loop
			case <-pt.ctx.Done():
				if !pt.peerTaskDone {
					if pt.failedCode == failedCodeNotSet {
						pt.failedReason = reasonContextCanceled
						pt.failedCode = dfcodes.ClientContextCanceled
					}
					pt.Errorf("context done due to %s, progress is not done", pt.ctx.Err())
				} else {
					pt.Debugf("context done due to %s, progress is already done", pt.ctx.Err())
				}
				break loop
			case failed := <-pt.failedPieceCh:
				pt.Warnf("download piece/%d failed, retry", failed)
				num = failed
				limit = 1
			}
		}
	}
}

func (pt *peerTask) downloadPieceWorker(id int32, pti PeerTask, requests chan *DownloadPieceRequest) {
	for {
		select {
		case request := <-requests:
			pt.Debugf("peer download worker #%d receive piece task, "+
				"dest peer id: %s, piece num: %d, range start: %d, range size: %d",
				id, request.DstPid, request.piece.PieceNum, request.piece.RangeStart, request.piece.RangeSize)
			pt.pieceManager.DownloadPiece(pti, request)
		case <-pt.done:
			pt.Debugf("pt.done, peer download worker #%d exit", id)
			return
		case <-pt.ctx.Done():
			pt.Debugf("pt.ctx.Done(), peer download worker #%d exit", id)
			return
		}
	}
}

func (pt *peerTask) isCompleted() bool {
	return pt.completedLength == pt.contentLength
}

func (pt *peerTask) preparePieceTasks(request *base.PieceTaskRequest) (p *base.PiecePacket, err error) {
	defer func() {
		if rerr := recover(); rerr != nil {
			pt.Errorf("preparePieceTasks recover from: %s", rerr)
			err = fmt.Errorf("%v", rerr)
		}
	}()
prepare:
	pt.pieceParallelCount = pt.peerPacket.ParallelCount
	request.DstPid = pt.peerPacket.MainPeer.PeerId
	p, err = pt.preparePieceTasksByPeer(pt.peerPacket, pt.peerPacket.MainPeer, request)
	if err == nil {
		return
	}
	if err == errPeerPacketChanged {
		goto prepare
	}
	for _, peer := range pt.peerPacket.StealPeers {
		request.DstPid = peer.PeerId
		p, err = pt.preparePieceTasksByPeer(pt.peerPacket, peer, request)
		if err == nil {
			return
		}
		if err == errPeerPacketChanged {
			goto prepare
		}
	}
	err = fmt.Errorf("no peers available")
	return
}

func (pt *peerTask) preparePieceTasksByPeer(curPeerPacket *scheduler.PeerPacket, peer *scheduler.PeerPacket_DestPeer, request *base.PieceTaskRequest) (*base.PiecePacket, error) {
	if peer == nil {
		return nil, fmt.Errorf("empty peer")
	}
retry6404:
	pt.Debugf("get piece task from peer %s, piece num: %d, limit: %d\"", peer.PeerId, request.StartNum, request.Limit)
	p, err := pt.getPieceTasks(curPeerPacket, peer, request)
	if err == nil {
		pt.Infof("get piece task from peer %s ok, pieces length: %d", peer.PeerId, len(p.PieceInfos))
		return p, nil
	}
	if err == errPeerPacketChanged {
		return nil, err
	}

	// grpc error
	if se, ok := err.(interface{ GRPCStatus() *status.Status }); ok {
		pt.Debugf("get piece task with grpc error, code: %d", se.GRPCStatus().Code())
		// context canceled, just exit
		if se.GRPCStatus().Code() == codes.Canceled {
			pt.Warnf("get piece task from peer(%s) canceled: %s", peer.PeerId, err)
			return nil, err
		}
	}
	code := dfcodes.ClientPieceRequestFail
	// not grpc error
	if de, ok := err.(*dferrors.DfError); ok && uint32(de.Code) > uint32(codes.Unauthenticated) {
		pt.Debugf("get piece task from peer %s with df error, code: %d", peer.PeerId, de.Code)
		code = de.Code
	}
	pt.Errorf("get piece task from peer(%s) error: %s, code: %d", peer.PeerId, err, code)
	perr := pt.peerPacketStream.Send(&scheduler.PieceResult{
		TaskId:        pt.taskId,
		SrcPid:        pt.peerId,
		DstPid:        peer.PeerId,
		Success:       false,
		Code:          code,
		HostLoad:      nil,
		FinishedCount: -1,
	})
	if perr != nil {
		pt.Errorf("send piece result error: %s, code: %d", peer.PeerId, err)
	}

	if code == dfcodes.CdnTaskNotFound && curPeerPacket == pt.peerPacket {
		goto retry6404
	}
	return nil, err
}

func (pt *peerTask) getPieceTasks(curPeerPacket *scheduler.PeerPacket, peer *scheduler.PeerPacket_DestPeer, request *base.PieceTaskRequest) (*base.PiecePacket, error) {
	var peerPacketChanged bool
	p, err := rpc.ExecuteWithRetry(func() (interface{}, error) {
		pp, getErr := dfclient.GetPieceTasks(peer, pt.ctx, request)
		if getErr != nil {
			// fast way to exit retry
			if curPeerPacket != pt.peerPacket {
				pt.Warnf("get piece tasks with error: %s, but peer packet changed, switch to new peer packet", getErr)
				peerPacketChanged = true
				return nil, nil
			}
			return nil, getErr
		}
		// by santong: when peer return empty, retry later
		if len(pp.PieceInfos) == 0 {
			er := pt.peerPacketStream.Send(&scheduler.PieceResult{
				TaskId:        pt.taskId,
				SrcPid:        pt.peerId,
				DstPid:        peer.PeerId,
				Success:       false,
				Code:          dfcodes.ClientWaitPieceReady,
				HostLoad:      nil,
				FinishedCount: pt.readyPieces.Settled(),
			})
			if er != nil {
				pt.Errorf("send piece result error: %s, code: %d", peer.PeerId, er)
			}
			// fast way to exit retry
			if curPeerPacket != pt.peerPacket {
				pt.Warnf("get empty pieces and peer packet changed, switch to new peer packet")
				peerPacketChanged = true
				return nil, nil
			}
			pt.Warnf("peer %s returns success but with empty pieces, retry later", peer.PeerId)
			return nil, dferrors.ErrEmptyValue
		}
		return pp, nil
	}, 0.05, 0.2, 40, nil)
	if peerPacketChanged {
		return nil, errPeerPacketChanged
	}

	if err == nil {
		return p.(*base.PiecePacket), nil
	}
	return nil, err
}

func (pt *peerTask) getNextPieceNum(cur, limit int32) int32 {
	if pt.isCompleted() {
		return -1
	}
	i := cur + limit
	for ; pt.requestedPieces.IsSet(i); i++ {
	}
	if pt.totalPiece > 0 && i >= pt.totalPiece {
		// double check, re-search not success or not requested pieces
		for i = int32(0); pt.requestedPieces.IsSet(i); i++ {
		}
		if pt.totalPiece > 0 && i >= pt.totalPiece {
			return -1
		}
	}
	return i
}
