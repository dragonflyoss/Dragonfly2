package basic

import (
	"github.com/dragonflyoss/Dragonfly2/scheduler/config"
	"github.com/dragonflyoss/Dragonfly2/scheduler/types"
	"math/rand"
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

func (e *Evaluator) GetMaxUsableHostValue() float64 {
	return e.maxUsableHostValue
}

func (e *Evaluator) GetNextPiece(peerTask *types.PeerTask) (p *types.Piece, err error) {
	count := peerTask.GetDownloadingPieceNum()
	if count >= config.GetConfig().Scheduler.MaxDownloadPieceNum {
		return
	}

	var pieceNum int32 = -1
	retryList := peerTask.GetRetryPieceList()
	if retryList != nil && len(retryList) > 0 && rand.Int31n(2) > 0 {
		for p := range retryList {
			if !peerTask.IsPieceDownloading(pieceNum) {
				pieceNum = p
			}
		}
	}

	// first piece need download
	if pieceNum < 0 {
		pieceNum = peerTask.GetFirstPieceNum()
		for peerTask.IsPieceDownloading(pieceNum) && peerTask.Task.GetPiece(pieceNum) != nil {
			pieceNum++
		}
	}

	if pieceNum > peerTask.Task.GetMaxPieceNum() {
		return
	}

	p = peerTask.Task.GetPiece(pieceNum)

	return
}

func (e *Evaluator) Evaluate(dst *types.PeerTask, src *types.PeerTask) (result float64, error error) {
	load, err := e.GetHostLoad(dst.Host)
	if err != nil {
		return
	}
	dist, err := e.GetDistance(dst, src)
	if err != nil {
		return
	}
	result = load + dist
	return
}

func (e *Evaluator) GetHostLoad(host *types.Host) (load float64, err error) {
	load = float64(host.ProducerLoad) / float64(config.GetConfig().Scheduler.MaxUploadPieceNum)
	return
}

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
		historyList.Range(func(key, value interface{}) bool{
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
		for i:=0; i<len(list) && i<3; i++ {
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
	return
}
