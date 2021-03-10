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
	"d7y.io/dragonfly/v2/pkg/dfcodes"
	"d7y.io/dragonfly/v2/pkg/dferrors"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	"d7y.io/dragonfly/v2/pkg/rpc/scheduler"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

type PeerTaskStatus int8

const (
	PeerTaskStatusHealth         PeerTaskStatus = 0
	PeerTaskStatusNeedParent     PeerTaskStatus = 1
	PeerTaskStatusNeedChildren   PeerTaskStatus = 2
	PeerTaskStatusBadNode        PeerTaskStatus = 3
	PeerTaskStatusNeedAdjustNode PeerTaskStatus = 4
	PeerTaskStatusNeedCheckNode  PeerTaskStatus = 5
	PeerTaskStatusDone           PeerTaskStatus = 6
	PeerTaskStatusLeaveNode      PeerTaskStatus = 7
	PeerTaskStatusAddParent      PeerTaskStatus = 8
	PeerTaskStatusNodeGone       PeerTaskStatus = 9
)

type PeerTask struct {
	Pid  string // peer id
	Task *Task  // task info
	Host *Host  // host info

	isDown          bool // is leave scheduler
	pieceStatusList *sync.Map
	lock            *sync.Mutex
	finishedNum     int32 // download finished piece number
	startTime       int64
	lastActiveTime  int64
	touch           func(*PeerTask)

	parent          *PeerEdge // primary download provider
	children        *sync.Map // all primary download consumers
	subTreeNodesNum int32     // node number of subtree and current node is root of the subtree

	// the client of peer task, which used for send and receive msg
	client scheduler.Scheduler_ReportPieceResultServer

	Traffic int64
	Cost    uint32
	Success bool
	Code    base.Code

	status  PeerTaskStatus
	jobData interface{}
}

type PeerEdge struct {
	SrcPeerTask *PeerTask // child, consumer
	DstPeerTask *PeerTask // parent, provider
	Concurrency int8      // number of thread download from the provider
	CostHistory []int32   // history of downloading one piece cost from the provider
}

func (pe *PeerEdge) AddCost(cost int32) {
	if pe == nil {
		return
	}
	pe.CostHistory = append(pe.CostHistory, cost)
	if len(pe.CostHistory) > 20 {
		pe.CostHistory = pe.CostHistory[1:]
	}
}

func NewPeerTask(pid string, task *Task, host *Host, touch func(*PeerTask)) *PeerTask {
	pt := &PeerTask{
		Pid:             pid,
		Task:            task,
		Host:            host,
		pieceStatusList: new(sync.Map),
		isDown:          false,
		lock:            new(sync.Mutex),
		startTime:       time.Now().UnixNano(),
		lastActiveTime:  time.Now().UnixNano(),
		touch:           touch,
		children:        new(sync.Map),
		subTreeNodesNum: 1,
	}
	if host != nil {
		host.AddPeerTask(pt)
	}
	pt.Touch()
	if task != nil {
		task.Statistic.AddPeerTaskStart()
	}
	return pt
}

func (pt *PeerTask) AddParent(parent *PeerTask, concurrency int8) {
	if pt == nil || parent == nil {
		return
	}
	pt.lock.Lock()
	defer pt.lock.Unlock()

	pe := &PeerEdge{
		SrcPeerTask: pt,     // child
		DstPeerTask: parent, // parent
		Concurrency: concurrency,
	}
	pt.parent = pe
	parent.children.Store(pt, pe)

	// modify subTreeNodesNum of all ancestor
	p := parent
	for p != nil {
		atomic.AddInt32(&p.subTreeNodesNum, pt.subTreeNodesNum)
		if p.parent == nil || p.parent.DstPeerTask == nil {
			break
		}
		p = p.parent.DstPeerTask
	}
	if pt.Host != nil {
		pt.Host.AddDownloadLoad(int32(concurrency))
	}
	if parent.Host != nil {
		parent.Host.AddUploadLoad(int32(concurrency))
	}
}

func (pt *PeerTask) GetParent() *PeerEdge {
	pt.lock.Lock()
	defer pt.lock.Unlock()
	return pt.parent
}

func (pt *PeerTask) GetCost() int32 {
	if pt.parent == nil || len(pt.parent.CostHistory) < 1 {
		return int32(time.Second / time.Millisecond)
	}
	totalCost := int32(0)
	for _, cost := range pt.parent.CostHistory {
		totalCost += cost
	}
	return totalCost / int32(len(pt.parent.CostHistory))
}

func (pt *PeerTask) GetChildren() (children []*PeerEdge) {
	if pt.children != nil {
		pt.children.Range(func(k, v interface{}) bool {
			children = append(children, v.(*PeerEdge))
			return true
		})
	}
	return children
}

func (pt *PeerTask) AddConcurrency(parent *PeerTask, delta int8) {
	if pt == nil || parent == nil {
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

func (pt *PeerTask) DeleteParent() {
	if pt == nil || pt.parent == nil {
		return
	}

	parent := pt.parent.DstPeerTask
	if pt.parent.DstPeerTask != nil && pt.parent.DstPeerTask.children != nil {
		pt.parent.DstPeerTask.children.Delete(pt)
	}
	pt.parent = nil

	p := parent
	for p != nil {
		atomic.AddInt32(&p.subTreeNodesNum, -pt.subTreeNodesNum)
		if p.parent == nil || p.parent.DstPeerTask == nil ||
			(p.Host != nil && p.Host.Type == HostTypeCdn) {
			break
		}
		p = p.parent.DstPeerTask
	}

	if pt.Host != nil {
		pt.Host.AddDownloadLoad(-1)
	}
	if parent.Host != nil {
		parent.Host.AddUploadLoad(-1)
	}
}

func (pt *PeerTask) GetFreeLoad() int32 {
	if pt.Host == nil {
		return 0
	}
	return pt.Host.GetFreeUploadLoad()
}

func (pt *PeerTask) Touch() {
	pt.lastActiveTime = time.Now().UnixNano()
	pt.touch(pt)
}

func (pt *PeerTask) GetFinishedNum() int32 {
	pt.lock.Lock()
	defer pt.lock.Unlock()
	return pt.finishedNum
}

func (pt *PeerTask) GetPieceStatusList() *sync.Map {
	return pt.pieceStatusList
}

func (pt *PeerTask) AddPieceStatus(ps *scheduler.PieceResult) {
	pt.lock.Lock()
	defer pt.lock.Unlock()

	if pt.pieceStatusList == nil {
		pt.pieceStatusList = new(sync.Map)
	}
	old, loaded := pt.pieceStatusList.LoadOrStore(ps.PieceNum, ps)
	if loaded {
		oldPs, _ := old.(*scheduler.PieceResult)
		if oldPs != nil && oldPs.Success {
			return
		}
		if ps.Success {
			pt.pieceStatusList.Store(ps.PieceNum, ps)
		}
	}
	if !ps.Success {
		return
	}

	// peer as cdn set up
	if pt.Host.Type == HostTypeCdn && pt.isDown {
		pt.isDown = false
	}

	pt.finishedNum = ps.FinishedCount

	if pt.parent != nil {
		pt.parent.AddCost(int32(ps.EndTime - ps.BeginTime))
	}

	pt.Touch()
}

func (pt *PeerTask) IsDown() (ok bool) {
	return pt.isDown
}

func (pt *PeerTask) SetDown() {
	pt.isDown = true
	pt.Touch()
}

func (pt *PeerTask) SetUp() {
	pt.isDown = false
	pt.Touch()
}

func (pt *PeerTask) SetStatus(traffic int64, cost uint32, success bool, code base.Code) {
	if pt == nil {
		return
	}
	pt.Traffic = traffic
	pt.Cost = cost
	pt.Success = success
	pt.Code = code
	pt.Touch()
	if pt.Success && pt.Task != nil {
		pt.Task.Statistic.AddPeerTaskDown(int32((time.Now().UnixNano() - pt.startTime) / int64(time.Millisecond)))
	}
}

func (pt *PeerTask) SetClient(client scheduler.Scheduler_ReportPieceResultServer) {
	pt.client = client
}

func (pt *PeerTask) GetSendPkg() (pkg *scheduler.PeerPacket) {
	// if pt != nil && pt.client != nil {
	pkg = &scheduler.PeerPacket{
		State: &base.ResponseState{
			Success: true,
			Code:    dfcodes.Success,
		},
		TaskId: pt.Task.TaskId,
		// source peer id
		SrcPid: pt.Pid,
		// concurrent downloading count from main peer
	}
	if pt.parent != nil && pt.parent.DstPeerTask != nil && pt.parent.DstPeerTask.Host != nil {
		pkg.ParallelCount = int32(pt.parent.Concurrency)
		peerHost := pt.parent.DstPeerTask.Host.PeerHost
		pkg.MainPeer = &scheduler.PeerPacket_DestPeer{
			Ip:      peerHost.Ip,
			RpcPort: peerHost.RpcPort,
			PeerId:  pt.parent.DstPeerTask.Pid,
		}
	}
	// TODO select StealPeers

	return
}

func (pt *PeerTask) Send() error {
	if pt == nil {
		return nil
	}
	if pt.client != nil {
		err := pt.client.Context().Err()
		if err != nil {
			pt.client = nil
			return err
		}
		return pt.client.Send(pt.GetSendPkg())
	}
	return errors.New("empty client")
}

func (pt *PeerTask) SendError(dfError *dferrors.DfError) error {
	if pt == nil {
		return nil
	}
	if pt.client != nil {
		err := pt.client.Context().Err()
		if err != nil {
			pt.client = nil
			return err
		}
		pkg := &scheduler.PeerPacket{
			State: &base.ResponseState{
				Success: false,
				Code:    dfError.Code,
				Msg: dfError.Message,
			},
		}
		return pt.client.Send(pkg)
	}
	return errors.New("empty client")
}

func (pt *PeerTask) GetDiffPieceNum(dst *PeerTask) int32 {
	pt.lock.Lock()
	defer pt.lock.Unlock()
	diff := dst.finishedNum - pt.finishedNum
	if diff > 0 {
		return diff
	}
	return 0
}

func (pt *PeerTask) GetDeep() int32 {
	pt.lock.Lock()
	defer pt.lock.Unlock()
	deep := int32(0)
	node := pt
	for node != nil {
		deep++
		if node.parent == nil || node.parent.DstPeerTask == nil ||
			(node.Host != nil && node.Host.Type == HostTypeCdn) {
			break
		}
		node = node.parent.DstPeerTask
	}
	return deep
}

func (pt *PeerTask) GetRoot() *PeerTask {
	pt.lock.Lock()
	defer pt.lock.Unlock()
	node := pt
	for node != nil {
		if node.parent == nil || node.parent.DstPeerTask == nil ||
			(node.Host != nil && node.Host.Type == HostTypeCdn) {
			break
		}
		node = node.parent.DstPeerTask
	}
	return node
}

func (pt *PeerTask) IsAncestor(a *PeerTask) bool {
	pt.lock.Lock()
	defer pt.lock.Unlock()
	if a == nil {
		return false
	}
	node := pt
	for node != nil {
		if node.parent == nil || node.parent.DstPeerTask == nil ||
			(node.Host != nil && node.Host.Type == HostTypeCdn) {
			return false
		} else if node.Pid == a.Pid {
			return true
		}
		node = node.parent.DstPeerTask
	}
	return false
}

func (pt *PeerTask) GetSubTreeNodesNum() int32 {
	pt.lock.Lock()
	defer pt.lock.Unlock()
	return pt.subTreeNodesNum
}

func (pt *PeerTask) GetNodeStatus() PeerTaskStatus {
	pt.lock.Lock()
	defer pt.lock.Unlock()
	return pt.status
}

func (pt *PeerTask) GetJobData() interface{} {
	pt.lock.Lock()
	defer pt.lock.Unlock()
	return pt.jobData
}

func (pt *PeerTask) SetNodeStatus(status PeerTaskStatus, data ...interface{}) {
	pt.lock.Lock()
	defer pt.lock.Unlock()
	pt.status = status
	if len(data) > 0 {
		pt.jobData = data[0]
	} else {
		pt.jobData = nil
	}
}

func (pt *PeerTask) IsWaiting() bool {
	pt.lock.Lock()
	defer pt.lock.Unlock()
	if pt == nil || pt.parent == nil || pt.parent.DstPeerTask == nil {
		return false
	}
	return pt.finishedNum >= pt.parent.DstPeerTask.finishedNum
}

func (pt *PeerTask) GetLastActiveTime() int64 {
	pt.lock.Lock()
	defer pt.lock.Unlock()
	return pt.lastActiveTime
}
