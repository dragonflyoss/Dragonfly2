package basic

import (
	"github.com/dragonflyoss/Dragonfly2/scheduler/daemon/mgr"
	"github.com/dragonflyoss/Dragonfly2/scheduler/scheduler"
	"github.com/dragonflyoss/Dragonfly2/scheduler/types"
)

var _ scheduler.IPeerTaskEvaluator= &Evaluator{}

type Evaluator struct {
	hostMgr *mgr.HostManager
	taskMgr *mgr.TaskManager
	peerTaskMgr *mgr.PeerTaskManager
}


func (e *Evaluator) GetNextPiece(peerTask *types.PeerTask) (*types.Piece, error) {
	// first piece need download
	pieceNum := peerTask.FirstPieceNum


	return
}

func (e *Evaluator) Evaluate(dst *types.Host, src *types.Host) (result float64, error error) {
	return
}


func (e *Evaluator) GetHostLoad(host *types.Host) (float64, error) {
	panic("implement me")
}

func (e *Evaluator) GetDistance(dst *types.Host, src *types.Host) (float64, error) {
	panic("implement me")
}


