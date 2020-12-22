package types

import "sync"

type HostType int

const (
	HostTypePeer = 1
	HostTypeCdn  = 2
)

type Host struct {
	Uuid           string   `json:"uuid,omitempty"`
	Ip             string   `json:"ip,omitempty"`
	Port           int32    `json:"port,omitempty"` // peer server http port
	HostName       string   `json:"host_name,omitempty"`
	SecurityDomain string   `json:"security_domain,omitempty"` // security isolation domain for network
	Location       string   `json:"location,omitempty"`        // area|country|province|city|...
	Idc            string   `json:"idc,omitempty"`
	Switch         string   `json:"switch,omitempty"` // network device construct, xx|yy|zz
	Type           HostType // peer / cdn

	peerTaskMap *sync.Map // Pid => PeerTask

	// ProducerLoad is the load of download services provided by the current node.
	ProducerLoad int16
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
