package scheduler

import (
	"fmt"
	"github.com/dragonflyoss/Dragonfly2/scheduler/scheduler/basic"
	"math"

	"github.com/sirupsen/logrus"

	"github.com/dragonflyoss/Dragonfly2/scheduler/types"
)

type Scheduler struct {
	evaluator   IPeerTaskEvaluator
}

func CreateScheduler() *Scheduler {
	return &Scheduler{
		evaluator: &basic.Evaluator{},
	}
}

func (s *Scheduler) Scheduler(task *types.PeerTask) (result []*types.PieceTask, err error) {
	for {
		// choose a piece to download
		piece, err := s.evaluator.GetNextPiece(task)
		if err != nil {
			logrus.Debugf("scheduler get piece for taskID(%s) nil", task.Task.BizId)
			break
		}

		// scheduler piece to a host
		srcHost := task.Host
		readyPeerTaskList:= piece.GetReadPeerTaskList()
		var dstPeerTask *types.PeerTask
		value := math.MaxFloat64
		for _, pt := range readyPeerTaskList {
			val, _ := s.evaluator.Evaluate(pt.Host, srcHost)
			if val < value {
				value = val
				dstPeerTask = pt
			}
		}
		if dstPeerTask != nil && value < s.evaluator.GetMaxUsableHostValue() {
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

func (s *Scheduler) Start() {

}