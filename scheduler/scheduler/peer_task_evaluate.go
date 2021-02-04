package scheduler

import (
	"d7y.io/dragonfly/v2/scheduler/types"
)

type IPeerTaskEvaluator interface {
	NeedAdjustParent(peer *types.PeerTask) bool
	IsNodeBad(peer *types.PeerTask) bool
	Evaluate(dst *types.PeerTask, src *types.PeerTask) (float64, error)
	SelectChildCandidates(peer *types.PeerTask) []*types.PeerTask
	SelectParentCandidates(peer *types.PeerTask) []*types.PeerTask
}
