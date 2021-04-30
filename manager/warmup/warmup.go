package warmup

import (
	"sync"

	"d7y.io/dragonfly/v2/manager/apis/v2/types"
	"d7y.io/dragonfly/v2/manager/hostidentifier"
	"d7y.io/dragonfly/v2/manager/store"
)

type warmupTasks struct {
	tasks map[string]*types.WarmupTask
}

type WarmupSvc struct {
	mu         sync.Mutex
	store      store.Store
	identifier hostidentifier.Identifier
	tasks      map[string]*warmupTasks
}

func NewWarmupSvc(store store.Store, identifier hostidentifier.Identifier) (*WarmupSvc, error) {
	return &WarmupSvc{
		store:      store,
		identifier: identifier,
		tasks:      make(map[string]*warmupTasks),
	}, nil
}
