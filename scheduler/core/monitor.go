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

package core

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
	downloadMonitorQueue workqueue.DelayingInterface
	peerManager          daemon.PeerMgr
	verbose              bool
	done                 chan struct{}
}

func newMonitor(peerManager daemon.PeerMgr) *monitor {
	return &monitor{
		downloadMonitorQueue: workqueue.NewDelayingQueue(),
		peerManager:          peerManager,
	}
}

func (m *monitor) start() {
	ticker := time.NewTicker(time.Second * 10)
	for {
		select {
		case <-ticker.C:
			logger.Debugf(m.printDebugInfo())
		case <-m.done:
			return
		}
	}
}

func (m *monitor) stop() {
	close(m.done)
}

func (m *monitor) printDebugInfo() string {
	var roots []*types.Peer

	buffer := bytes.NewBuffer([]byte{})
	table := tablewriter.NewWriter(buffer)
	table.SetHeader([]string{"PeerID", "URL", "parent node", "status", "start time", "Finished Piece Num", "Finished", "Free Load"})

	m.peerManager.ListPeers().Range(func(key interface{}, value interface{}) (ok bool) {
		ok = true
		peer := value.(*types.Peer)
		if peer == nil {
			return
		}
		parentNode := ""
		if peer.GetParent() == nil {
			roots = append(roots, peer)
		} else {
			parentNode = peer.GetParent().PeerID
		}

		table.Append([]string{peer.PeerID, peer.Task.URL, parentNode, peer.GetStatus().String(), peer.CreateTime.String(), strconv.Itoa(int(peer.GetFinishedNum())),
			strconv.FormatBool(peer.IsSuccess()), strconv.Itoa(peer.Host.GetFreeUploadLoad())})
		return
	})
	table.Render()

	var msgs []string
	msgs = append(msgs, buffer.String())

	var printTree func(node *types.Peer, path []string)
	printTree = func(node *types.Peer, path []string) {
		if node == nil {
			return
		}
		nPath := append(path, fmt.Sprintf("%s(%d)", node.PeerID, node.GetWholeTreeNode()))
		if len(path) >= 1 {
			msgs = append(msgs, node.PeerID+" || "+strings.Join(nPath, "-"))
		}
		node.GetChildren().Range(func(key, value interface{}) bool {
			child := (value).(*types.Peer)
			if child == nil {
				return true
			}
			printTree(child, nPath)
			return true
		})
	}

	for _, root := range roots {
		printTree(root, nil)
	}

	msg := "============\n" + strings.Join(msgs, "\n") + "\n==============="
	return msg
}

func (m *monitor) RefreshDownloadMonitor(peer *types.Peer) {
	logger.WithTaskAndPeerID(peer.Task.TaskID, peer.PeerID).Debugf("downloadMonitorWorkingLoop refresh ")
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

// downloadMonitorWorkingLoop monitor peers download
func (m *monitor) downloadMonitorWorkingLoop() {
	for {
		v, shutdown := m.downloadMonitorQueue.Get()
		if shutdown {
			logger.Infof("download monitor working loop closed")
			break
		}
		//if m.downloadMonitorCallBack != nil {
		peer := v.(*types.Peer)
		if peer != nil {
			logger.WithTaskAndPeerID(peer.Task.TaskID, peer.PeerID).Debugf("downloadMonitorWorkingLoop status[%d]", peer.GetStatus())
			if peer.IsSuccess() || peer.Host.CDN {
				// clear from monitor
			} else {
				if !peer.IsRunning() {
					// peer do not report for a long time, peer gone
					if time.Now().After(peer.GetLastAccessTime().Add(PeerGoneTimeout)) {
						peer.MarkLeave()
						//pt.SendError(dferrors.New(dfcodes.SchedPeerGone, "report time out"))
					}
					//m.downloadMonitorCallBack(peer)
				} else if !peer.IsWaiting() {
					//m.downloadMonitorCallBack(peer)
				} else {
					if time.Now().After(peer.GetLastAccessTime().Add(PeerForceGoneTimeout)) {
						peer.MarkLeave()
						//pt.SendError(dferrors.New(dfcodes.SchedPeerGone, "report fource time out"))
					}
					//m.downloadMonitorCallBack(peer)
				}
				//_, ok := m.Get(pt.GetPeerID())
				//status := pt.Status
				//if ok && !pt.Success && status != types.PeerStatusNodeGone && status != types.PeerStatusLeaveNode {
				//	m.RefreshDownloadMonitor(pt)
				//}
			}
		}
		//}

		m.downloadMonitorQueue.Done(v)
	}
}
