package scheduler

import (
	"fmt"
	dferror "github.com/dragonflyoss/Dragonfly2/pkg/error"
	logger "github.com/dragonflyoss/Dragonfly2/pkg/log"
	"github.com/dragonflyoss/Dragonfly2/scheduler/scheduler/basic"
	"math"

	"github.com/dragonflyoss/Dragonfly2/scheduler/types"
)

type Scheduler struct {
	evaluator IPeerTaskEvaluator
}

func CreateScheduler() *Scheduler {
	return &Scheduler{
		evaluator: basic.NewEvaluator(),
	}
}

func (s *Scheduler) Scheduler(task *types.PeerTask) (result []*types.PieceTask, waitingPieceNumList []int32, err error) {
	err = dferror.SchedulerWaitPiece
	for {
		// choose a piece to download
		piece, pieceNum, e := s.evaluator.GetNextPiece(task)
		if e != nil {
			logger.Debugf("[%s][%s]: scheduler get piece nil", task.Pid, task.Task.TaskId)
			break
		}

		if pieceNum >= task.Task.GetMaxPieceNum() && task.Task.PieceTotal > 0 {
			if len(result)+len(waitingPieceNumList) == 0 {
				err = dferror.SchedulerFinished
				return
			}
		}

		if piece == nil {
			if pieceNum >= 0 && (pieceNum <= task.Task.GetMaxPieceNum() ||
				(pieceNum > task.Task.GetMaxPieceNum() && task.Task.PieceTotal <= 0)) {
				logger.Debugf("[%s][%s]: wait for piece [%d]", task.Task.TaskId, task.Pid, pieceNum)
				waitingPieceNumList = append(waitingPieceNumList, pieceNum)
			}
			break
		}

		// scheduler piece to a host
		readyPeerTaskList := piece.GetReadyPeerTaskList()
		var dstPeerTask *types.PeerTask
		value := math.MaxFloat64
		for _, pt := range readyPeerTaskList {
			val, _ := s.evaluator.Evaluate(pt, task)
			if val < value {
				value = val
				dstPeerTask = pt
			}
		}
		if dstPeerTask != nil && value < s.evaluator.GetMaxUsableHostValue() {
			task.AddDownloadingPiece(piece.PieceNum)
			dstPeerTask.Host.AddLoad(1)
			logger.Debugf("[%s][%s]: host[%s] [%d] delete host load", task.Task.TaskId, task.Pid, dstPeerTask.Host.Uuid, piece.PieceNum)
			result = append(result, &types.PieceTask{
				Piece:   piece,
				SrcPid:  task.Pid,
				DstPid:  dstPeerTask.Pid,
				DstAddr: fmt.Sprintf("%s:%d", dstPeerTask.Host.Ip, dstPeerTask.Host.Port),
			})
		} else {
			// bad dstHost quality
			logger.Debugf("[%s][%s]: wait for ready host [%d][%.04f]", task.Task.TaskId, task.Pid, pieceNum, value)
			waitingPieceNumList = append(waitingPieceNumList, pieceNum)
			break
		}
	}
	if len(result) > 0 {
		err = nil
	}
	return
}
