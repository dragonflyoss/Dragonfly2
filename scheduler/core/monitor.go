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
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/olekukonko/tablewriter"
	"go.uber.org/zap"
	"k8s.io/client-go/util/workqueue"

	"d7y.io/dragonfly/v2/scheduler/supervisor"
)

type monitor struct {
	downloadMonitorQueue workqueue.DelayingInterface
	peerManager          supervisor.PeerManager
	log                  *zap.SugaredLogger
}

func newMonitor(openMonitor bool, peerManager supervisor.PeerManager) *monitor {
	if !openMonitor {
		return nil
	}
	config := zap.NewDevelopmentConfig()
	logger, _ := config.Build()
	return &monitor{
		downloadMonitorQueue: workqueue.NewDelayingQueue(),
		peerManager:          peerManager,
		log:                  logger.Sugar(),
	}
}

func (m *monitor) start(done <-chan struct{}) {
	ticker := time.NewTicker(time.Second * 10)
	for {
		select {
		case <-ticker.C:
			m.log.Info(m.printDebugInfo())
		case <-done:
			return
		}
	}
}

func (m *monitor) printDebugInfo() string {
	var peers, roots []*supervisor.Peer
	m.peerManager.GetPeers().Range(func(key interface{}, value interface{}) (ok bool) {
		ok = true
		peer := value.(*supervisor.Peer)
		if peer == nil {
			m.log.Error("encounter a nil peer")
			return
		}

		if _, ok := peer.GetParent(); !ok {
			roots = append(roots, peer)
		}
		peers = append(peers, peer)
		return
	})

	sort.Slice(peers, func(i, j int) bool {
		return peers[i].GetStatus() > peers[j].GetStatus()
	})
	buffer := bytes.NewBuffer([]byte{})
	table := tablewriter.NewWriter(buffer)
	table.SetHeader([]string{"PeerID", "TaskID", "URL", "Parent", "Status", "start time", "Finished Piece Num", "Finished", "Free Load"})

	for _, peer := range peers {
		parentID := ""
		if parent, ok := peer.GetParent(); ok {
			parentID = parent.ID
		}

		table.Append([]string{peer.ID, peer.Task.ID, peer.Task.URL[len(peer.Task.URL)-15 : len(peer.Task.URL)], parentID, peer.GetStatus().String(),
			peer.CreateAt.Load().String(), strconv.Itoa(int(peer.TotalPieceCount.Load())),
			strconv.FormatBool(peer.IsSuccess()), strconv.Itoa(int(peer.Host.GetFreeUploadLoad()))})
	}
	table.Render()

	var msgs []string
	msgs = append(msgs, buffer.String())

	var printTree func(node *supervisor.Peer, path []string)
	printTree = func(node *supervisor.Peer, path []string) {
		if node == nil {
			return
		}
		nPath := append(path, fmt.Sprintf("%s(%d)(%s)", node.ID, node.GetTreeNodeCount(), node.GetStatus()))
		if len(path) >= 1 {
			msgs = append(msgs, node.ID+" || "+strings.Join(nPath, "-"))
		}
		node.GetChildren().Range(func(key, value interface{}) bool {
			child := (value).(*supervisor.Peer)
			printTree(child, nPath)
			return true
		})
	}

	for _, root := range roots {
		printTree(root, nil)
	}

	msg := "============\n" + strings.Join(append(msgs, "peer count: "+strconv.Itoa(table.NumLines())), "\n") + "\n==============="
	return msg
}
