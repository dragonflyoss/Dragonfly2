package scheduler

import (
	"fmt"
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

func (s *Scheduler) Scheduler(task *types.PeerTask) (result []*types.PieceTask, err error) {
	for {
		// choose a piece to download
		var piece *types.Piece
		piece, err = s.evaluator.GetNextPiece(task)
		if err != nil {
			logger.Debugf("scheduler get piece for taskID(%s) nil", task.Task.TaskId)
			break
		}

		if piece == nil {
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
			result = append(result, &types.PieceTask{
				Piece:   piece,
				SrcPid:  task.Pid,
				DstPid:  dstPeerTask.Pid,
				DstAddr: fmt.Sprintf("%s:%d", dstPeerTask.Host.Ip, dstPeerTask.Host.Port),
			})
		} else if value > s.evaluator.GetMaxUsableHostValue() {
			// bad dstHost quality
			return
		}
	}
	return
}

