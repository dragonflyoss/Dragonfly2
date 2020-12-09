package scheduler

import (
	"fmt"
	"math"

	"github.com/sirupsen/logrus"

	"github.com/dragonflyoss/Dragonfly2/scheduler/daemon/mgr"
	"github.com/dragonflyoss/Dragonfly2/scheduler/types"
)

type Scheduler struct {
	hostMgr     *mgr.HostManager
	taskMgr     *mgr.TaskManager
	peerTaskMgr *mgr.PeerTaskManager
	evaluator   IPeerTaskEvaluator
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
		readyPeerHostList := piece.GetReadPeerHostList()
		var dstPeerHost *types.PeerHost
		value := math.MaxFloat64
		for _, host := range readyPeerHostList {
			val, _ := s.evaluator.Evaluate(host.Host, srcHost)
			if val < value {
				value = val
				dstPeerHost = host
			}
		}
		if dstPeerHost != nil {
			result = append(result, &types.PieceTask{
				Piece:   piece,
				SrcPid:  task.Pid,
				DstPid:  dstPeerHost.Pid,
				DstAddr: fmt.Sprintf("%s:%d", dstPeerHost.Host.Ip, dstPeerHost.Host.Port),
			})
		}
	}
	return
}
