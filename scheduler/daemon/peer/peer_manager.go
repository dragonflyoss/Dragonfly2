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

package peer

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"d7y.io/dragonfly/v2/internal/dfcodes"
	"d7y.io/dragonfly/v2/internal/dferrors"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/structure/sortedlist"
	"d7y.io/dragonfly/v2/scheduler/config"
	"d7y.io/dragonfly/v2/scheduler/daemon"
	"d7y.io/dragonfly/v2/scheduler/types"
	"github.com/olekukonko/tablewriter"
	"k8s.io/client-go/util/workqueue"
)

const (
	PeerGoneTimeout      = 10 * time.Second
	PeerForceGoneTimeout = 2 * time.Minute
)

type manager struct {
	peerMap              sync.Map
	dataRanger           sync.Map
	gcQueue              workqueue.DelayingInterface
	gcDelayTime          time.Duration
	downloadMonitorQueue workqueue.DelayingInterface
	//downloadMonitorCallBack func(node *types.PeerNode)
	taskManager daemon.TaskMgr
	hostManager daemon.HostMgr
	verbose     bool
}

func newPeerManager(cfg *config.Config, taskManager daemon.TaskMgr, hostManager daemon.HostMgr) daemon.PeerMgr {
	delay := time.Hour
	if cfg.GC.PeerTaskDelay > delay {
		delay = cfg.GC.PeerTaskDelay
	}

	peerManager := &manager{
		gcQueue:              workqueue.NewDelayingQueue(),
		gcDelayTime:          delay,
		downloadMonitorQueue: workqueue.NewDelayingQueue(),
		taskManager:          taskManager,
		hostManager:          hostManager,
		verbose:              cfg.Verbose,
	}

	go peerManager.downloadMonitorWorkingLoop()

	go peerManager.gcWorkingLoop()

	go peerManager.printDebugInfoLoop()

	return peerManager
}

func (m *manager) Add(peerID string, task *types.Task, host *types.NodeHost) *types.PeerNode {
	v, ok := m.peerMap.Load(peerID)
	if ok {
		return v.(*types.PeerNode)
	}

	peer, err := types.NewPeerNode(peerID, task, host, m.addToGCQueue)
	m.peerMap.Store(peerID, peer)

	m.taskManager.Touch(task.GetTaskID())

	r, ok := m.dataRanger.Load(task.GetTaskID())
	if ok {
		ranger, ok := r.(*sortedlist.SortedList)
		if ok {
			ranger.Add(peer)
		}
	}

	return peer
}

func (m *manager) AddFake(pid string, task *types.Task) *types.PeerNode {
	v, ok := m.peerMap.Load(pid)
	if ok {
		return v.(*types.PeerNode)
	}

	peer, _ := types.NewPeerNode(pid, task, nil, m.addToGCQueue)
	m.peerMap.Store(pid, peer)
	peer.SetDown()
	return pt
}

func (m *manager) Delete(pid string) {
	data, ok := m.peerMap.Load(pid)
	if ok {
		if peer, ok := data.(*types.PeerNode); ok {
			v, ok := m.dataRanger.Load(peer.GetTask())
			if ok {
				ranger, ok := v.(*sortedlist.SortedList)
				if ok {
					ranger.Delete(peer)
				}
			}

		}
		m.peerMap.Delete(pid)
	}
	return
}

func (m *manager) Get(pid string) (h *types.PeerNode, ok bool) {
	data, ok := m.peerMap.Load(pid)
	if !ok {
		return
	}
	h = data.(*types.PeerNode)
	return
}

func (m *manager) AddTask(task *types.Task) {
	m.dataRanger.LoadOrStore(task, sortedlist.NewSortedList())
}

func (m *manager) DeleteTask(task *types.Task) {
	// notify client cnd error
	m.peerMap.Range(func(key, value interface{}) bool {
		peer, _ := value.(*types.PeerNode)
		if peer == nil {
			return true
		}
		if peer.GetTask() != task {
			return true
		}
		if task.CDNError != nil {
			peer.SendError(task.CDNError)
		}
		m.peerMap.Delete(key)
		return true
	})

	m.dataRanger.Delete(task)
}

func (m *manager) Update(pt *types.PeerNode) {
	if pt == nil {
		return
	}
	v, ok := m.dataRanger.Load(pt.GetTask())
	if !ok {
		return
	}
	ranger, ok := v.(*sortedlist.SortedList)
	if !ok {
		return
	}

	if pt.GetFreeLoad() == 0 || pt.IsDown() {
		ranger.Delete(pt)
		return
	}

	status := pt.GetNodeStatus()
	switch status {
	case types.PeerStatusLeaveNode, types.PeerStatusNodeGone:
		ranger.Delete(pt)
	default:
		ranger.UpdateOrAdd(pt)
	}
}

func (m *manager) Walker(task *types.Task, limit int, walker func(pt *types.PeerNode) bool) {
	if walker == nil {
		return
	}
	v, ok := m.dataRanger.Load(task)
	if !ok {
		return
	}
	ranger, ok := v.(*sortedlist.SortedList)
	if !ok {
		return
	}
	ranger.RangeLimit(limit, func(data sortedlist.Item) bool {
		pt, _ := data.(*types.PeerNode)
		return walker(pt)
	})
}

func (m *manager) WalkerReverse(task *types.Task, limit int, walker func(pt *types.PeerNode) bool) {
	if walker == nil {
		return
	}
	v, ok := m.dataRanger.Load(task)
	if !ok {
		return
	}
	ranger, ok := v.(*sortedlist.SortedList)
	if !ok {
		return
	}
	ranger.RangeReverseLimit(limit, func(data sortedlist.Item) bool {
		pt, _ := data.(*types.PeerNode)
		return walker(pt)
	})
}

func (m *manager) ClearPeerTask() {
	m.peerMap.Range(func(key interface{}, value interface{}) bool {
		peer, _ := value.(*types.PeerNode)
		if peer != nil && peer.GetTask() != nil && peer.GetTask().Removed {
			m.peerMap.Delete(peer.GetPeerID())
		}
		return true
	})
}

func (m *manager) GetGCDelayTime() time.Duration {
	return m.gcDelayTime
}

func (m *manager) SetGCDelayTime(delay time.Duration) {
	m.gcDelayTime = delay
}

func (m *manager) addToGCQueue(pt *types.PeerNode) {
	m.gcQueue.AddAfter(pt, m.gcDelayTime)
}

func (m *manager) cleanPeerTask(pt *types.PeerNode) {
	defer m.gcQueue.Done(pt)
	if pt == nil {
		return
	}
	m.peerMap.Delete(pt.GetPeerID())
	if pt.GetHost() != nil {
		host, _ := m.hostManager.Get(pt.GetHost().GetUUID())
		if host != nil {
			host.DeletePeerTask(pt.GetPeerID())
			if host.GetPeerTaskNum() <= 0 {
				m.hostManager.Delete(pt.GetHost().GetUUID())
			}
		}
	}
}

func (m *manager) gcWorkingLoop() {
	for {
		v, shutdown := m.gcQueue.Get()
		if shutdown {
			break
		}
		pt, _ := v.(*types.PeerNode)
		if pt != nil {
			m.cleanPeerTask(pt)
		}
		m.gcQueue.Done(v)
	}
}

func (m *manager) printDebugInfoLoop() {
	for {
		time.Sleep(time.Second * 10)
		if m.verbose {
			logger.Debugf(m.printDebugInfo())
		}
	}
}

func (m *manager) printDebugInfo() string {
	var task *types.Task
	var roots []*types.PeerNode

	buffer := bytes.NewBuffer([]byte{})
	table := tablewriter.NewWriter(buffer)
	table.SetHeader([]string{"PeerId", "Finished Piece Num", "Download Finished", "Free Load", "Peer Down"})

	m.peerMap.Range(func(key interface{}, value interface{}) (ok bool) {
		ok = true
		peerTask, _ := value.(*types.PeerNode)
		if peerTask == nil {
			return
		}
		if task == nil {
			task = peerTask.GetTask()
		}
		if peerTask.GetParent() == nil {
			roots = append(roots, peerTask)
		}
		// do not print finished node witch do not has child
		if !(peerTask.Success && peerTask.Host != nil && peerTask.Host.GetUploadLoadPercent() < 0.001) {
			table.Append([]string{peerTask.Pid, strconv.Itoa(int(peerTask.GetFinishedNum())),
				strconv.FormatBool(peerTask.Success), strconv.Itoa(int(peerTask.GetFreeLoad())), strconv.FormatBool(peerTask.IsDown())})
		}
		return
	})
	table.Render()

	var msgs []string
	msgs = append(msgs, buffer.String())

	var printTree func(node *types.PeerNode, path []string)
	printTree = func(node *types.PeerNode, path []string) {
		if node == nil {
			return
		}
		nPath := append(path, fmt.Sprintf("%s(%d)", node.GetPeerID(), node.GetWholeTreeNode()))
		if len(path) > 1 {
			msgs = append(msgs, node.GetPeerID()+" || "+strings.Join(nPath, "-"))
		}
		for _, child := range node.GetChildren() {
			if child == nil || child.SrcPeerTask == nil {
				continue
			}
			printTree(child.SrcPeerTask, nPath)
		}
	}

	for _, root := range roots {
		printTree(root, nil)
	}

	msg := "============\n" + strings.Join(msgs, "\n") + "\n==============="
	return msg
}

func (m *manager) RefreshDownloadMonitor(pt *types.PeerNode) {
	logger.Debugf("[%s][%s] downloadMonitorWorkingLoop refresh ", pt.TaskID, pt.Pid)
	status := pt.GetNodeStatus()
	if status != types.PeerStatusHealth {
		m.downloadMonitorQueue.AddAfter(pt, time.Second*2)
	} else if pt.IsWaiting() {
		m.downloadMonitorQueue.AddAfter(pt, time.Second*2)
	} else {
		delay := time.Millisecond * time.Duration(pt.GetCost()*10)
		if delay < time.Millisecond*20 {
			delay = time.Millisecond * 20
		}
		m.downloadMonitorQueue.AddAfter(pt, delay)
	}
}

func (m *manager) CDNCallback(pt *types.PeerNode, err *dferrors.DfError) {
	if err != nil {
		pt.SendError(err)
	}
	m.downloadMonitorQueue.Add(pt)
}

func (m *manager) SetDownloadingMonitorCallBack(callback func(*types.PeerNode)) {
	m.downloadMonitorCallBack = callback
}

// monitor peer download
func (m *manager) downloadMonitorWorkingLoop() {
	for {
		v, shutdown := m.downloadMonitorQueue.Get()
		if shutdown {
			break
		}
		if m.downloadMonitorCallBack != nil {
			pt, _ := v.(*types.PeerNode)
			if pt != nil {
				logger.Debugf("[%s][%s] downloadMonitorWorkingLoop status[%d]", pt.GetTask().GetTaskID(), pt.GetPeerID(), pt.Status)
				if pt.Success || (pt.GetHost() != nil && types.IsCDN(pt.GetHost())) {
					// clear from monitor
				} else {
					if pt.Status != types.PeerStatusHealth {
						// peer do not report for a long time, peer gone
						if time.Now().After(pt.LastAccessTime.Add(PeerGoneTimeout)) {
							pt.Status = types.PeerStatusNodeGone
							pt.SendError(dferrors.New(dfcodes.SchedPeerGone, "report time out"))
						}
						m.downloadMonitorCallBack(pt)
					} else if !pt.IsWaiting() {
						m.downloadMonitorCallBack(pt)
					} else {
						if time.Now().After(pt.LastAccessTime.Add(PeerForceGoneTimeout)) {
							pt.Status = types.PeerStatusNodeGone
							pt.SendError(dferrors.New(dfcodes.SchedPeerGone, "report fource time out"))
						}
						m.downloadMonitorCallBack(pt)
					}
					_, ok := m.Get(pt.GetPeerID())
					status := pt.Status
					if ok && !pt.Success && status != types.PeerStatusNodeGone && status != types.PeerStatusLeaveNode {
						m.RefreshDownloadMonitor(pt)
					}
				}
			}
		}

		m.downloadMonitorQueue.Done(v)
	}
}
