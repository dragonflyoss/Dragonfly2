package warmup

import (
	"sync"

	"d7y.io/dragonfly/v2/manager/apis/v2/types"
	"d7y.io/dragonfly/v2/manager/hostidentifier"
	"d7y.io/dragonfly/v2/manager/store"
)

type tasks struct {
	tasks map[string]*types.WarmupTask
}

type Server struct {
	mu         sync.Mutex
	store      store.Store
	identifier hostidentifier.Identifier
	tasks      map[string]*tasks
}

func NewServer(store store.Store, identifier hostidentifier.Identifier) (*Server, error) {
	return &Server{
		store:      store,
		identifier: identifier,
		tasks:      make(map[string]*tasks),
	}, nil
}
