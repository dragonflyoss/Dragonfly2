package mgr

import (
	"sync"

	"github.com/dragonflyoss/Dragonfly2/scheduler/types"
)

type HostManager struct {
	data *sync.Map
}

func (mgr *HostManager) GetHost(uuid string) (host *types.Host, err error) {
	value, ok := mgr.data.Load(uuid)
	if !ok {
		return
	}
	host = value.(*types.Host)
	return
}
