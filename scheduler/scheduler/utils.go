package scheduler

import (
	"sort"

	"d7y.io/dragonfly/v2/scheduler/resource"
	"d7y.io/dragonfly/v2/scheduler/scheduler/evaluator"
	"github.com/kevwan/mapreduce/v2"
)

func sortNodes(candidates []*resource.Peer, eval evaluator.Evaluator, peer *resource.Peer, total int32) ([]*resource.Peer, error) {
	switch eval.EvalType() {
	case evaluator.MLAlgorithm:
		peers, err := compute(candidates, peer, eval, total)
		if err != nil {
			return nil, err
		}
		return peers, nil

	default:
		baseCompute(candidates, peer, total)
		return candidates, nil
	}
}

type operator struct {
	peer  *resource.Peer
	value float64
}

func baseCompute(candidates []*resource.Peer, peer *resource.Peer, taskPieceCount int32) {
	sort.Slice(
		candidates,
		func(i, j int) bool {
			return evaluator.Evaluate(candidates[i], peer, taskPieceCount) > evaluator.Evaluate(candidates[j], peer, taskPieceCount)
		},
	)
}

func compute(candidates []*resource.Peer, peer *resource.Peer, eval evaluator.Evaluator, taskPieceCount int32) ([]*resource.Peer, error) {
	operators, err := mapreduce.MapReduce(func(source chan<- *resource.Peer) {
		for _, parent := range candidates {
			source <- parent
		}
	}, func(parent *resource.Peer, writer mapreduce.Writer[*operator], cancel func(error)) {
		// mapper
		writer.Write(&operator{peer: parent, value: eval.Evaluate(parent, peer, taskPieceCount)})
	}, func(pipe <-chan *operator, writer mapreduce.Writer[[]*operator], cancel func(error)) {
		// reducer
		var orderList []*operator
		for op := range pipe {
			orderList = append(orderList, op)
		}
		writer.Write(orderList)
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(operators, func(i, j int) bool {
		return operators[i].value > operators[j].value
	})
	var sortList []*resource.Peer
	for _, op := range operators {
		sortList = append(sortList, op.peer)
	}
	return sortList, nil
}
