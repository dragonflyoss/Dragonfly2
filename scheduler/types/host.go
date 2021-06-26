/*
 *     Copyright 2020 The Dragonfly Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package types

import (
	"sync"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	"go.uber.org/atomic"
)

type HostType int32

const (
	HostTypePeer = iota + 1
	HostTypeCdn
)

type Host struct {
	// each time the daemon starts, it will generate a different uuid
	Uuid string `protobuf:"bytes,1,opt,name=uuid,proto3" json:"uuid,omitempty"`
	// peer host ip
	IP string `protobuf:"bytes,2,opt,name=ip,proto3" json:"ip,omitempty"`
	// rpc service port for peer
	RpcPort int32 `protobuf:"varint,3,opt,name=rpc_port,json=rpcPort,proto3" json:"rpc_port,omitempty"`
	// piece downloading port for peer
	DownloadPort int32 `protobuf:"varint,4,opt,name=down_port,json=downPort,proto3" json:"down_port,omitempty"`
	// peer host name
	HostName string `protobuf:"bytes,5,opt,name=host_name,json=hostName,proto3" json:"host_name,omitempty"`
	// security isolation domain for network
	SecurityDomain string `protobuf:"bytes,6,opt,name=security_domain,json=securityDomain,proto3" json:"security_domain,omitempty"`
	// location path: area|country|province|city|...
	Location string `protobuf:"bytes,7,opt,name=location,proto3" json:"location,omitempty"`
	// idc where the peer host is located
	Idc string `protobuf:"bytes,8,opt,name=idc,proto3" json:"idc,omitempty"`
	// network device path: switch|router|...
	NetTopology string `protobuf:"bytes,9,opt,name=net_topology,json=netTopology,proto3" json:"net_topology,omitempty"`

	Type        HostType // peer / cdn
	peerTaskMap sync.Map // Pid => PeerTask
	// ProducerLoad is the load of download services provided by the current node.
	totalUploadLoad     int32
	currentUploadLoad   atomic.Int32
	totalDownloadLoad   int32
	currentDownloadLoad atomic.Int32
}

func (h *Host) AddPeerTask(peerTask *PeerTask) {
	h.peerTaskMap.Store(peerTask.Pid, peerTask)
}

func (h *Host) DeletePeerTask(peerTaskID string) {
	h.peerTaskMap.Delete(peerTaskID)
}

func (h *Host) GetPeerTaskNum() int32 {
	count := 0
	h.peerTaskMap.Range(func(key, value interface{}) bool {
		count++
		return true
	})
	return int32(count)
}

func (h *Host) GetPeerTask(peerID string) (peerTask *PeerTask) {
	v, _ := h.peerTaskMap.Load(peerID)
	peerTask, _ = v.(*PeerTask)
	return
}

func (h *Host) SetTotalUploadLoad(load int32) {
	h.totalUploadLoad = load
}

func (h *Host) AddUploadLoad(delta int32) {
	h.loadLock.Lock()
	defer h.loadLock.Unlock()
	logger.Infof("host[%s] type[%d] add UploadLoad [%d]", h.Uuid, h.Type, delta)
	h.currentUploadLoad += delta
}

func (h *Host) GetUploadLoad() int32 {
	h.loadLock.Lock()
	defer h.loadLock.Unlock()
	return h.currentUploadLoad
}

func (h *Host) GetUploadLoadPercent() float64 {
	h.loadLock.Lock()
	defer h.loadLock.Unlock()
	if h.totalUploadLoad <= 0 {
		return 1.0
	}
	return float64(h.currentUploadLoad) / float64(h.totalUploadLoad)
}

func (h *Host) GetFreeUploadLoad() int32 {
	h.loadLock.Lock()
	defer h.loadLock.Unlock()
	return h.totalUploadLoad - h.currentUploadLoad
}

func (h *Host) SetTotalDownloadLoad(load int32) {
	h.loadLock.Lock()
	defer h.loadLock.Unlock()
	h.totalDownloadLoad = load
}

func (h *Host) AddDownloadLoad(delta int32) {
	h.loadLock.Lock()
	defer h.loadLock.Unlock()
	h.currentDownloadLoad += delta
}

func (h *Host) GetDownloadLoad() int32 {
	h.loadLock.Lock()
	defer h.loadLock.Unlock()
	return h.currentDownloadLoad
}

func (h *Host) GetDownloadLoadPercent() float64 {
	h.loadLock.Lock()
	defer h.loadLock.Unlock()
	if h.totalDownloadLoad <= 0 {
		return 1.0
	}
	return float64(h.currentDownloadLoad) / float64(h.totalDownloadLoad)
}

func (h *Host) GetFreeDownloadLoad() int32 {
	h.loadLock.Lock()
	defer h.loadLock.Unlock()
	return h.totalDownloadLoad - h.currentDownloadLoad
}
