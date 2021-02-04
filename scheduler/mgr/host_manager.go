package mgr

import (
	"github.com/dragonflyoss/Dragonfly/v2/scheduler/types"
	"sync"
)

type HostManager struct {
	data *sync.Map
}

func createHostManager() *HostManager {
	return &HostManager{
		data: new(sync.Map),
	}
}

func (m *HostManager) AddHost(host *types.Host) *types.Host {
	v, ok := m.data.Load(host.Uuid)
	if ok {
		return v.(*types.Host)
	}

	copyHost := types.CopyHost(host)
	m.CalculateLoad(copyHost)
	m.data.Store(host.Uuid, copyHost)
	return copyHost
}

func (m *HostManager) DeleteHost(uuid string) {
	m.data.Delete(uuid)
	return
}

func (m *HostManager) GetHost(uuid string) (h *types.Host, ok bool) {
	data, ok := m.data.Load(uuid)
	if !ok {
		return
	}
	h = data.(*types.Host)
	return
}

func (m *HostManager) CalculateLoad(host *types.Host) {
	if host.Type == types.HostTypePeer {
		host.SetTotalUploadLoad(3)
		host.SetTotalDownloadLoad(3)
	} else {
		host.SetTotalUploadLoad(4)
		host.SetTotalDownloadLoad(4)
	}
	return
}
