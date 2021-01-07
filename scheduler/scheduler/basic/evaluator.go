package basic

import (
	"github.com/dragonflyoss/Dragonfly2/scheduler/config"
	"github.com/dragonflyoss/Dragonfly2/scheduler/types"
	"sort"
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

func (e *Evaluator) NeedAdjustParent(peer *types.PeerTask) bool {
	return false
}

func (e *Evaluator) IsNodeBad(peer *types.PeerTask) bool {
	return true
}

func (e *Evaluator) GetMaxUsableHostValue() float64 {
	return e.maxUsableHostValue
}

func (e *Evaluator) SelectChildCandidates(peer *types.PeerTask) (list []*types.PeerTask) {
	return
}

func (e *Evaluator) SelectParentCandidates(peer *types.PeerTask) (list []*types.PeerTask) {
	return
}

func (e *Evaluator) Evaluate(dst *types.PeerTask, src *types.PeerTask) (result float64, error error) {
	profits := e.GetProfits(dst, src)

	load, err := e.GetHostLoad(dst.Host)
	if err != nil {
		return
	}

	dist, err := e.GetDistance(dst, src)
	if err != nil {
		return
	}

	result = (profits+1) * (load + dist)
	return
}

// GetProfits 0.0~unlimited larger and better
func (e *Evaluator) GetProfits(dst *types.PeerTask, src *types.PeerTask)  float64 {
	diff := src.GetDiffPieceNum(dst)
	if diff == 0 {
		return 0.0
	}
	return float64(diff * src.GetSubTreeNodesNum())
}

// GetHostLoad 0.0~1.0 larger and better
func (e *Evaluator) GetHostLoad(host *types.Host) (load float64, err error) {
	load = 1.0 - host.GetUploadLoadPercent()
	return
}

// GetDistance 0.0~1.0 larger and better
func (e *Evaluator) GetDistance(dst *types.PeerTask, src *types.PeerTask) (dist float64, err error) {
	hostDist := 40.0
	if dst.Host == src.Host {
		hostDist = 0.0
	} else if dst.Host.Switch == src.Host.Switch && src.Host.Switch != "" {
		hostDist = 10.0
	} else if dst.Host.Idc == src.Host.Idc && src.Host.Idc != "" {
		hostDist = 20.0
	}

	historyDist := 40.0

	var list []*types.PieceStatus
	historyList := src.GetPieceStatusList()
	if historyList != nil {
		historyList.Range(func(key, value interface{}) bool {
			ps := value.(*types.PieceStatus)
			if ps.DstPid == dst.Pid {
				list = append(list, ps)
			}
			return true
		})
	}

	sort.Slice(list, func(i int, j int) bool {
		return list[i].PieceNum > list[j].PieceNum
	})

	if len(list) > 0 {
		historyDist -= 10.0
		for i := 0; i < len(list) && i < 3; i++ {
			if !list[i].Success {
				historyDist += 20.0
			} else {
				cost := (float64(list[i].Cost) - 100.0) / 100.0 * 10
				if cost > 10.0 {
					cost = 10.0
				}
				historyDist += cost
			}
		}
	}

	dist = hostDist + historyDist
	return 1.0 - dist/80.0, nil
}
