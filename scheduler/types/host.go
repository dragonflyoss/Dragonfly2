package types

import (
	"sync"
	"sync/atomic"
)

type HostType int

const (
	HostTypePeer = 1
	HostTypeCdn  = 2
)

type Host struct {
	Uuid           string
	Ip             string
	Port           int32 // peer server http port
	HostName       string
	SecurityDomain string // security isolation domain for network
	Location       string // area|country|province|city|...
	Idc            string
	Switch         string // network device construct, xx|yy|zz

	Type        HostType  // peer / cdn
	peerTaskMap *sync.Map // Pid => PeerTask
	// ProducerLoad is the load of download services provided by the current node.
	ProducerLoad int32
	// ServiceDownTime the down time of the peer service.
	ServiceDownTime int64
}

func CopyHost(h *Host) *Host {
	copyHost := *h
	copyHost.peerTaskMap = new(sync.Map)
	return &copyHost
}

func (h *Host) AddPeerTask(peerTask *PeerTask) {
	h.peerTaskMap.Store(peerTask.Pid, peerTask)
}

func (h *Host) DeletePeerTask(peerTaskId string) {
	h.peerTaskMap.Delete(peerTaskId)
}

func (h *Host) GetPeerTaskNum() int32 {
	count := 0
	if h.peerTaskMap != nil {
		h.peerTaskMap.Range(func(key interface{}, value interface{})bool {
			count++
			return true
		})
	}
	return int32(count)
}

func (h *Host) GetPeerTask(peerTaskId string) (peerTask *PeerTask) {
	v, _ := h.peerTaskMap.Load(peerTaskId)
	peerTask, _ = v.(*PeerTask)
	return
}

func (h *Host) AddLoad(delta int32) {
	atomic.AddInt32(&h.ProducerLoad, delta)
}

func (h *Host) GetLoad() int32 {
	return atomic.LoadInt32(&h.ProducerLoad)
}
