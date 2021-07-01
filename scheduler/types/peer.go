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
	"errors"
	"sync/atomic"
	"time"

	"d7y.io/dragonfly/v2/internal/dfcodes"
	"d7y.io/dragonfly/v2/internal/dferrors"
	"d7y.io/dragonfly/v2/internal/rpc/base"
	"d7y.io/dragonfly/v2/internal/rpc/scheduler"
	"d7y.io/dragonfly/v2/scheduler/core/worker"
)

const (
	PeerStatusHealth = iota + 1
	PeerStatusNeedParent
	PeerStatusNeedChildren
	PeerStatusBadNode
	PeerStatusNeedAdjustNode
	PeerStatusNeedCheckNode
	PeerStatusDone
	PeerStatusLeaveNode
	PeerStatusAddParent
	PeerStatusNodeGone
)

type PeerNode struct {
	peerID            string    // peer id
	task              *Task     // task info
	host              *NodeHost // host info
	finishedNum       int       // downloaded finished piece number
	startTime         time.Time
	LastAccessTime    time.Time
	parent            *PeerNode
	children          []*PeerNode
	client            *worker.Client // the client of peer task, which used for send and receive msg
	Traffic           int64
	Cost              time.Duration
	Success           bool
	Code              base.Code
	Status            int
	jobData           interface{}
	downloadStatistic *downloadStatistic
}

type downloadStatistic struct {
}

func NewPeerNode(peerID string, task *Task, host *NodeHost, client *worker.Client) (*PeerNode, error) {
	if task == nil {
		return nil, errors.New("task is nil")
	}
	if host == nil {
		return nil, errors.New("host is nil")
	}
	return &PeerNode{
		peerID:         peerID,
		task:           task,
		host:           host,
		client:         client,
		startTime:      time.Now(),
		LastAccessTime: time.Now(),
	}, nil
	//if host != nil {
	//	host.AddPeerTask(pt)
	//}
	//pt.Touch()
	//if task != nil {
	//	task.Statistic.AddPeerTaskStart()
	//}
}

func (peer *PeerNode) GetPeerID() string {
	return peer.peerID
}

func (peer *PeerNode) GetHost() *NodeHost {
	return peer.host
}

func (peer *PeerNode) GetTask() *Task {
	return peer.task
}

func (peer *PeerNode) GetWholeTreeNode() int {
	node := peer
	// 获取以当前节点为根的整棵树的节点个数
	for node != nil {
		if node.parent == nil || IsCDN(node.host) {
			break
		}
		node = node.parent
	}
	return 9
}

func (peer *PeerNode) SetParent(parent *PeerNode, concurrency int) error {
	if parent == nil {
		return errors.New("parent node is nil")
	}

	pe := &PeerEdge{
		SrcPeerTask: pt,     // child
		DstPeerTask: parent, // parent
		Concurrency: concurrency,
	}
	peer.Parent = parent
	parent.children.Store(pt, pe)

	// modify subTreeNodesNum of all ancestor
	p := parent
	for p != nil {
		atomic.AddInt32(&p.subTreeNodesNum, peer.subTreeNodesNum)
		if p.parent == nil || p.parent.DstPeerTask == nil {
			break
		}
		p = p.parent
	}
	if peer.GetHost() != nil {
		peer.GetHost().AddDownloadLoad(concurrency)
	}
	if parent.GetHost() != nil {
		parent.GetHost().AddUploadLoad(concurrency)
	}
}

func (peer *PeerNode) GetStartTime() time.Time {
	return peer.startTime
}

func (peer *PeerNode) GetParent() *PeerNode {
	return peer.parent
}

func (peer *PeerNode) GetCost() int64 {
	if len(peer.parent.CostHistory) < 1 {
		return int64(time.Second / time.Millisecond)
	}
	totalCost := int64(0)
	for _, cost := range peer.parent.CostHistory {
		totalCost += cost
	}
	return totalCost / int64(len(peer.parent.CostHistory))
}

func (peer *PeerNode) GetChildren() (children []*PeerNode) {
	return peer.children
}

func (peer *PeerNode) AddConcurrency(parent *PeerNode, delta int8) {
	if parent == nil {
		return
	}

	if pt.parent == nil && pt.parent.DstPeerTask != parent {
		return
	}

	pt.parent.Concurrency += delta

	if pt.Host != nil {
		pt.Host.AddDownloadLoad(int32(delta))
	}
	if parent.Host != nil {
		parent.Host.AddUploadLoad(int32(delta))
	}
}

func (peer *PeerNode) DeleteParent() {
	if peer.parent == nil {
		return
	}

	parent := peer.parent
	if peer.parent != nil && peer.parent.children != nil {
		peer.parent.children.Delete(pt)
	}
	concurency := int32(peer.parent.Concurrency)
	peer.parent = nil

	p := parent
	for p != nil {
		atomic.AddInt32(&p.subTreeNodesNum, -pt.subTreeNodesNum)
		if p.parent == nil || p.parent.DstPeerTask == nil ||
			(p.Host != nil && p.Host.Type == HostTypeCdn) {
			break
		}
		p = p.parent
	}

	if peer.host != nil {
		peer.host.AddDownloadLoad(-concurency)
	}
	if parent.host != nil {
		parent.Host.AddUploadLoad(-concurency)
	}
}

func (peer *PeerNode) GetFreeLoad() int32 {
	if pt.Host == nil {
		return 0
	}
	return pt.Host.GetFreeUploadLoad()
}

func (peer *PeerNode) AddPieceStatus(ps *scheduler.PieceResult) {
	pt.lock.Lock()
	defer pt.lock.Unlock()

	if !ps.Success {
		return
	}

	// peer as cdn set up
	if pt.Host != nil && pt.Host.Type == HostTypeCdn && pt.isDown {
		pt.isDown = false
	}

	pt.finishedNum = ps.FinishedCount

	if pt.parent != nil {
		pt.parent.AddCost(int64(ps.EndTime - ps.BeginTime))
	}

	pt.Touch()
}

func (peer *PeerNode) SetUp() {
	pt.isDown = false
	pt.Touch()
}

func (peer *PeerNode) SetStatus(traffic int64, cost uint32, success bool, code base.Code) {
	peer.Traffic = traffic
	peer.Cost = cost
	peer.Success = success
	peer.Code = code
	peer.Touch()
	if peer.Success && peer.Task != nil {
		pt.Task.Statistic.AddPeerTaskDown(int32((time.Now().UnixNano() - pt.startTime) / int64(time.Millisecond)))
	}
}

func (peer *PeerNode) SetClient(client *worker.Client) {
	peer.client = client
}

func (peer *PeerNode) GetSendPkg() (pkg *scheduler.PeerPacket) {
	// if pt != nil && pt.client != nil {
	pkg = &scheduler.PeerPacket{
		Code:   dfcodes.Success,
		TaskId: pt.Task.TaskID,
		// source peer id
		SrcPid: pt.Pid,
		// concurrent downloading count from main peer
	}
	pt.lock.Lock()
	defer pt.lock.Unlock()
	if pt.parent != nil && pt.parent.DstPeerTask != nil && pt.parent.DstPeerTask.Host != nil {
		pkg.ParallelCount = int32(pt.parent.Concurrency)
		pkg.MainPeer = &scheduler.PeerPacket_DestPeer{
			Ip:      pt.parent.DstPeerTask.Host.PeerHost.Ip,
			RpcPort: pt.parent.DstPeerTask.Host.PeerHost.RpcPort,
			PeerId:  pt.parent.DstPeerTask.Pid,
		}
	}
	// TODO select StealPeers

	return
}

func (peer *PeerNode) Send() error {
	if pt == nil {
		return nil
	}
	if pt.client != nil {
		if pt.client.IsClosed() {
			pt.client = nil
			return errors.New("client closed")
		}
		return pt.client.Send(pt.GetSendPkg())
	}
	return errors.New("empty client")
}

func (peer *PeerNode) SendError(dfError *dferrors.DfError) error {
	if pt.client != nil {
		if pt.client.IsClosed() {
			pt.client = nil
			return errors.New("client closed")
		}
		pkg := &scheduler.PeerPacket{
			Code: dfError.Code,
		}
		if dfError.Code == dfcodes.SchedPeerGone ||
			pt.Task.CDNError != nil {
			defer pt.client.Close()
		}
		return pt.client.Send(pkg)
	}
	return errors.New("empty client")
}

func (peer *PeerNode) GetDepth() int {
	var deep int
	node := peer
	for node != nil {
		deep++
		if node.parent == nil || IsCDN(node.host) {
			break
		}
		node = node.parent
	}
	return deep
}

func (peer *PeerNode) GetTreeRoot() *PeerNode {
	node := peer
	for node != nil {
		if node.parent == nil || IsCDN(node.host) {
			break
		}
		node = node.parent
	}
	return node
}

// if ancestor is ancestor of peer
func (peer *PeerNode) IsAncestor(ancestor *PeerNode) bool {
	if ancestor == nil {
		return false
	}
	node := peer
	for node != nil {
		if node.parent == nil || IsCDN(node.host) {
			return false
		} else if node.pid == ancestor.pid {
			return true
		}
		node = node.parent
	}
	return false
}

func (peer *PeerNode) IsWaiting() bool {
	if peer.parent == nil {
		return false
	}

	return peer.finishedNum >= peer.parent.finishedNum
}

func (peer *PeerNode) GetSortKeys() (key1, key2 int) {
	key1 = int(peer.finishedNum)
	key2 = int(peer.GetFreeLoad())
	return
}

func (peer *PeerNode) HasParent() bool {
	return peer.parent != nil
}

func (peer *PeerNode) HistoryCost() time.Duration {
	return time.Second
}

func GetDiffPieceNum(src *PeerNode, dst *PeerNode) int {
	diff := src.finishedNum - dst.finishedNum
	if diff > 0 {
		return diff
	}
	return -diff
}
