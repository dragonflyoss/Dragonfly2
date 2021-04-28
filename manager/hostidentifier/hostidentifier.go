package hostidentifier

import (
	"sync"
)

type Identifier interface {
	Get(hostName string) (instanceId string, ok bool)
	Put(hostName, instanceId string)
	Delete(hostName string)
	Exist(hostName string) bool
}

type identifier struct {
	mu    sync.Mutex
	hosts map[string]string
}

func NewIdentifier() Identifier {
	return &identifier{
		mu:    sync.Mutex{},
		hosts: make(map[string]string),
	}
}

func (id *identifier) Get(hostName string) (instanceId string, ok bool) {
	id.mu.Lock()
	defer id.mu.Unlock()

	instanceId, ok = id.hosts[hostName]
	return
}

func (id *identifier) Put(hostName, instanceId string) {
	id.mu.Lock()
	defer id.mu.Unlock()

	id.hosts[hostName] = instanceId
}

func (id *identifier) Delete(hostName string) {
	id.mu.Lock()
	defer id.mu.Unlock()

	if _, ok := id.hosts[hostName]; ok {
		delete(id.hosts, hostName)
	}
}

func (id *identifier) Exist(hostName string) bool {
	id.mu.Lock()
	defer id.mu.Unlock()

	_, ok := id.hosts[hostName]
	return ok
}
