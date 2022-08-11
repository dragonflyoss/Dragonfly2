package training

import (
	"time"

	"d7y.io/dragonfly/v2/scheduler/config"

	"d7y.io/dragonfly/v2/pkg/rpc/manager/client"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/dag"
	"d7y.io/dragonfly/v2/pkg/pipeline"
	"d7y.io/dragonfly/v2/scheduler/storage"
)

type LinearTraining struct {
	graph         dag.DAG[pipeline.StepConstruct]
	storage       storage.Storage
	done          chan bool
	interval      time.Duration
	managerClient client.Client
	cfg           config.DynconfigInterface
}

func NewLinearTraining(storage storage.Storage, cfg config.DynconfigInterface, mc client.Client, tc *config.TrainingConfig) *LinearTraining {
	g, err := LinearDag()
	if err != nil {
		return nil
	}
	return &LinearTraining{
		graph:         g,
		storage:       storage,
		done:          make(chan bool),
		interval:      tc.RefreshModelInterval,
		managerClient: mc,
		cfg:           cfg,
	}
}

var (
	mapStep map[string]pipeline.StepConstruct
	order   []string
)

func init() {
	mapStep = map[string]pipeline.StepConstruct{
		"LoadingData": NewLoadStep,
		"Training":    NewTrainStep,
		"LoadingTest": NewLoadStep,
		"Evaluating":  NewEvaStep,
		"Saving":      NewSavingStep,
	}
	order = []string{"LoadingData", "Training", "LoadingTest", "Evaluating", "Saving"}
}

func LinearDag() (dag.DAG[pipeline.StepConstruct], error) {
	graph := dag.NewDAG[pipeline.StepConstruct]()

	for k, v := range mapStep {
		err := graph.AddVertex(k, v)
		if err != nil {
			return nil, err
		}
	}

	for i := 0; i < len(order)-1; i++ {
		err := graph.AddEdge(order[i], order[i+1])
		if err != nil {
			return nil, err
		}
	}
	return graph, nil
}

func (lr *LinearTraining) Process() (interface{}, error) {
	p := pipeline.NewPipeline()
	req := &pipeline.Request{
		KeyVal: make(map[string]interface{}),
		Data:   lr.storage,
	}
	dynconfigData, err := lr.cfg.Get()
	if err != nil {
		return nil, err
	}
	req.KeyVal[LoadType] = LoadData
	req.KeyVal[ManagerClient] = lr.managerClient
	req.KeyVal[DynConfigData] = dynconfigData
	req, err = p.Exec(req, lr.graph)
	if err != nil {
		return nil, err
	}
	return req.Data, nil
}

func (lr *LinearTraining) Serve() {
	go func() {
		ticker := time.NewTicker(lr.interval)
		for {
			select {
			case <-lr.done:
				logger.Infof("stop linear training")
				ticker.Stop()
				return
			case <-ticker.C:
				_, err := lr.Process()
				if err != nil {
					logger.Fatalf("linear regression error: %s", err.Error())
				}
			}
		}
	}()
}

func (lr *LinearTraining) Stop() {
	close(lr.done)
}
