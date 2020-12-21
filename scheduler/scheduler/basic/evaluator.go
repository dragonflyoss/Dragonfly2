package basic

import (
	"github.com/dragonflyoss/Dragonfly2/scheduler/scheduler"
	"github.com/dragonflyoss/Dragonfly2/scheduler/types"
)

var _ scheduler.IPeerTaskEvaluator= &Evaluator{}

type Evaluator struct {
}

func (e *Evaluator) GetMaxUsableHostValue() float64 {
	return 100.0
}

func (e *Evaluator) GetNextPiece(peerTask *types.PeerTask) (p *types.Piece, err error) {
	// first piece need download
	pieceNum := peerTask.GetFirstPieceNum()
	for !peerTask.IsPieceDownloading(pieceNum) && pieceNum < peerTask.Task.GetPieceTotal() {
		pieceNum ++
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


