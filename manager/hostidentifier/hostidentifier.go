package hostidentifier

import (
	"sync"
)

type Identifier interface {
	Get(hostName string) (instanceID string, ok bool)
	Put(hostName, instanceID string)
	Delete(hostName string)
	Exist(hostName string) bool
}

type identifier struct {
	mu    sync.Mutex
	hosts map[string]string
}

func NewIdentifier() Identifier {
	return &identifier{
		hosts: make(map[string]string),
	}
}

func (id *identifier) Get(hostName string) (instanceID string, ok bool) {
	id.mu.Lock()
	defer id.mu.Unlock()

	instanceID, ok = id.hosts[hostName]
	return
}

func (id *identifier) Put(hostName, instanceID string) {
	id.mu.Lock()
	defer id.mu.Unlock()

	id.hosts[hostName] = instanceID
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
