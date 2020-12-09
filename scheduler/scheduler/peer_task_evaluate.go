package scheduler

import (
	"github.com/dragonflyoss/Dragonfly2/scheduler/types"
)

type IPeerTaskEvaluator interface {
	GetNextPiece(peerTask *types.PeerTask) (*types.Piece, error)
	Evaluate(dst *types.Host, src *types.Host) (float64, error)
}
