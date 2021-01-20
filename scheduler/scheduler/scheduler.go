package scheduler

import (
	"github.com/dragonflyoss/Dragonfly2/scheduler/scheduler/basic"
	"github.com/dragonflyoss/Dragonfly2/scheduler/types"
)

type Scheduler struct {
	factory *evaluatorFactory
}

func CreateScheduler() *Scheduler {
	RegisterEvaluator("default", basic.NewEvaluator())
	RegisterGetEvaluatorFunc(0, func(*types.Task)(string, bool){return "default", true})
	return &Scheduler{
		factory: factory,
	}
}

// scheduler children to a peer
func (s *Scheduler) SchedulerChildren(peer *types.PeerTask) (children []*types.PeerTask, err error) {
	if peer == nil {
		return
	}
	freeLoad := peer.GetFreeLoad()
	candidates := s.factory.getEvaluator(peer.Task).SelectChildCandidates(peer)
	schedulerResult := make(map[*types.PeerTask]int8)
	for freeLoad > 0 {
		var chosen *types.PeerTask
		value := 0.0
		for _, child := range candidates {
			val, _ := s.factory.getEvaluator(peer.Task).Evaluate(peer, child)
			if val > value {
				value = val
				chosen = child
			}
		}
		if schedulerResult[chosen] == 0 {
			children = append(children, chosen)
		}
		schedulerResult[chosen]++
		freeLoad--
	}
	for _, child := range children {
		concurrency := schedulerResult[child]
		child.AddParent(peer, concurrency)
	}
	return
}

// scheduler a parent to a peer
func (s *Scheduler) SchedulerParent(peer *types.PeerTask) ( primary *types.PeerTask, secondary []*types.PeerTask, err error) {
	if peer == nil {
		return
	}
	candidates := s.factory.getEvaluator(peer.Task).SelectParentCandidates(peer)
	value := 0.0
	for _, parent := range candidates {
		val, _ := s.factory.getEvaluator(peer.Task).Evaluate(parent, peer)
		if val > value {
			value = val
			primary = parent
		}
	}
	if primary != nil {
		peer.AddParent(primary, 1)
	}

	return
}

func (s *Scheduler) SchedulerBadNode(peer *types.PeerTask) (adjustNodes []*types.PeerTask, err error) {
	adjustNodes, err = s.SchedulerLeaveNode(peer)
	if err != nil {
		return
	}

	children, _ := s.SchedulerChildren(peer)
	for _, child := range children {
		adjustNodes = append(adjustNodes, child)
	}

	return
}

func (s *Scheduler) SchedulerLeaveNode(peer *types.PeerTask) (adjustNodes []*types.PeerTask, err error) {
	peer.DeleteParent()
	s.SchedulerParent(peer)
	adjustNodes = append(adjustNodes, peer)

	for _, child := range peer.GetChildren() {
		child.SrcPeerTask.DeleteParent()
		s.SchedulerParent(child.SrcPeerTask)
		adjustNodes = append(adjustNodes, child.SrcPeerTask)
	}

	return
}

func (s *Scheduler) SchedulerAdjustParentNode(peer *types.PeerTask) (primary *types.PeerTask, secondary []*types.PeerTask, err error) {
	peer.DeleteParent()
	return s.SchedulerParent(peer)
}

func (s *Scheduler) SchedulerDone(peer *types.PeerTask) (parent *types.PeerTask, err error) {
	if peer.GetParent() == nil {
		return
	}
	parent = peer.GetParent().DstPeerTask
	if parent == nil {
		return
	}
	peer.DeleteParent()

	return
}


func (s *Scheduler) NeedAdjustParent(peer *types.PeerTask) bool {
	return s.factory.getEvaluator(peer.Task).NeedAdjustParent(peer)
}

func (s *Scheduler) IsNodeBad(peer *types.PeerTask) bool {
	return s.factory.getEvaluator(peer.Task).IsNodeBad(peer)
}


