package basic

import (
	"github.com/dragonflyoss/Dragonfly2/scheduler/config"
	"github.com/dragonflyoss/Dragonfly2/scheduler/types"
)

type Evaluator struct {
	maxUsableHostValue float64
}

func NewEvaluator() *Evaluator {
	e := &Evaluator{
		maxUsableHostValue: config.GetConfig().Scheduler.MaxUsableValue,
	}
	return e
}

func (e *Evaluator) GetMaxUsableHostValue() float64 {
	return e.maxUsableHostValue
}

func (e *Evaluator) GetNextPiece(peerTask *types.PeerTask) (p *types.Piece, err error) {
	// first piece need download
	pieceNum := peerTask.GetFirstPieceNum()
	for !peerTask.IsPieceDownloading(pieceNum) && peerTask.Task.GetPiece(pieceNum) != nil {
		pieceNum++
	}

	if pieceNum >= peerTask.Task.GetPieceTotal() {
		return
	}

	p = peerTask.Task.GetPiece(pieceNum)

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
