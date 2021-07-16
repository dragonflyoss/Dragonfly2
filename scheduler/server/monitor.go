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

package server

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
	"time"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/scheduler/daemon"
	"d7y.io/dragonfly/v2/scheduler/types"
	"github.com/olekukonko/tablewriter"
	"k8s.io/client-go/util/workqueue"
)

const (
	PeerGoneTimeout      = time.Second * 10
	PeerForceGoneTimeout = time.Minute * 2
)

type monitor struct {
	downloadMonitorQueue    workqueue.DelayingInterface
	downloadMonitorCallBack func(node *types.PeerNode)
	peerManager             daemon.PeerMgr
	verbose                 bool
}

func NewMonitor() {
	monitor := &monitor{
		downloadMonitorQueue:    nil,
		downloadMonitorCallBack: nil,
		peerManager:             nil,
		verbose:                 false,
	}
	go monitor.downloadMonitorWorkingLoop()
	go monitor.printDebugInfoLoop()
}

func (m *monitor) printDebugInfoLoop() {
	if m.verbose {
		ticker := time.NewTicker(time.Second * 10)
		for {
			<-ticker.C
			logger.Debugf(m.printDebugInfo())
		}

	}
}

func (m *monitor) printDebugInfo() string {
	var task *types.Task
	var roots []*types.PeerNode

	buffer := bytes.NewBuffer([]byte{})
	table := tablewriter.NewWriter(buffer)
	table.SetHeader([]string{"PeerID", "Finished Piece Num", "Finished", "Free Load"})

	m.peerManager.ListPeers().Range(func(key interface{}, value interface{}) (ok bool) {
		ok = true
		peer := value.(*types.PeerNode)
		if peer == nil {
			return
		}
		if task == nil {
			task = peer.Task
		}
		if peer.GetParent() == nil {
			roots = append(roots, peer)
		}
		// do not print finished node witch do not has child
		if !(peer.IsSuccess() && peer.Host != nil && peer.Host.GetUploadLoadPercent() < 0.001) {
			table.Append([]string{peer.PeerID, strconv.Itoa(int(peer.GetFinishedNum())),
				strconv.FormatBool(peer.IsSuccess()), strconv.Itoa(peer.Host.GetFreeUploadLoad())})
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
		nPath := append(path, fmt.Sprintf("%s(%d)", node.PeerID, node.GetWholeTreeNode()))
		if len(path) > 1 {
			msgs = append(msgs, node.PeerID+" || "+strings.Join(nPath, "-"))
		}
		for _, child := range node.GetChildren() {
			if child == nil {
				continue
			}
			printTree(child, nPath)
		}
	}

	for _, root := range roots {
		printTree(root, nil)
	}

	msg := "============\n" + strings.Join(msgs, "\n") + "\n==============="
	return msg
}

func (m *monitor) RefreshDownloadMonitor(peer *types.PeerNode) {
	logger.Debugf("[%s][%s] downloadMonitorWorkingLoop refresh ", peer.Task.TaskID, peer.PeerID)
	if !peer.IsRunning() {
		m.downloadMonitorQueue.AddAfter(peer, time.Second*2)
	} else if peer.IsWaiting() {
		m.downloadMonitorQueue.AddAfter(peer, time.Second*2)
	} else {
		delay := time.Millisecond * time.Duration(peer.GetCost()*10)
		if delay < time.Millisecond*20 {
			delay = time.Millisecond * 20
		}
		m.downloadMonitorQueue.AddAfter(peer, delay)
	}
}

func (m *monitor) SetDownloadingMonitorCallBack(callback func(*types.PeerNode)) {
	m.downloadMonitorCallBack = callback
}

// downloadMonitorWorkingLoop monitor peers download
func (m *monitor) downloadMonitorWorkingLoop() {
	for {
		v, shutdown := m.downloadMonitorQueue.Get()
		if shutdown {
			logger.Infof("download monitor working loop closed")
			break
		}
		if m.downloadMonitorCallBack != nil {
			peer := v.(*types.PeerNode)
			if peer != nil {
				logger.Debugf("[%s][%s] downloadMonitorWorkingLoop status[%d]", peer.Task.TaskID, peer.PeerID, peer.GetStatus())
				if peer.IsSuccess() || (peer.Host.IsCDNHost()) {
					// clear from monitor
				} else {
					if !peer.IsRunning() {
						// peer do not report for a long time, peer gone
						if time.Now().After(peer.GetLastAccessTime().Add(PeerGoneTimeout)) {
							peer.SetStatus(types.PeerStatusNodeGone)
							//pt.SendError(dferrors.New(dfcodes.SchedPeerGone, "report time out"))
						}
						m.downloadMonitorCallBack(peer)
					} else if !peer.IsWaiting() {
						m.downloadMonitorCallBack(peer)
					} else {
						if time.Now().After(peer.GetLastAccessTime().Add(PeerForceGoneTimeout)) {
							peer.SetStatus(types.PeerStatusNodeGone)
							//pt.SendError(dferrors.New(dfcodes.SchedPeerGone, "report fource time out"))
						}
						m.downloadMonitorCallBack(peer)
					}
					//_, ok := m.Get(pt.GetPeerID())
					//status := pt.Status
					//if ok && !pt.Success && status != types.PeerStatusNodeGone && status != types.PeerStatusLeaveNode {
					//	m.RefreshDownloadMonitor(pt)
					//}
				}
			}
		}

		m.downloadMonitorQueue.Done(v)
	}
}
