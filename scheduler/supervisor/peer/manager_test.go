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
	"d7y.io/dragonfly/v2/scheduler/supervisor/mock"
	"fmt"
	"github.com/golang/mock/gomock"
	"testing"
)

func TestNewManager(t *testing.T) {
	ctl := gomock.NewController(t)
	defer ctl.Finish()
	peerMgr := mock.NewMockPeerMgr(gomock.NewController(t))
	//peerMgr.Add("123456",)

	if peer, isH := peerMgr.Get("952323"); isH == true {
		fmt.Print(peer)
	} else {
		t.Errorf("no this peer!")
	}

	if ListPeers := peerMgr.ListPeers(); ListPeers == nil {
		t.Errorf("no this peer!")
	}

	if peer := peerMgr.ListPeersByTask("123456"); peer == nil {
		t.Failed()
	}

	peerMgr.Delete("123456")

	//peerMgr := mock.MockPeerMgr{gomock.Controller{T: t}, recover()}
	//types.NewPeer(20210824,types.NewTask(00001,"fafadf","55656",
	//	types.PieceInfo{10,0,10,"abcd",10,5},sortedlist,))
	//var peer = []struct{
	//	lock sync.RWMutex
	//	// PeerID specifies ID of peer
	//	PeerID string
	//	// Task specifies
	//	Task *Task
	//	// Host specifies
	//	Host *PeerHost
	//	// bindPacketChan
	//	bindPacketChan bool
	//	// PacketChan send schedulerPacket to peer client
	//	packetChan chan *scheduler.PeerPacket
	//	// createTime
	//	CreateTime time.Time
	//	// finishedNum specifies downloaded finished piece number
	//	finishedNum    atomic.Int32
	//	lastAccessTime time.Time
	//	parent         *Peer
	//	children       sync.Map
	//	status         PeerStatus
	//	costHistory    []int
	//	leave          atomic.Bool
	//}

}
