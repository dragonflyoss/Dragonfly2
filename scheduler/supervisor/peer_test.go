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

package supervisor_test

import (
	"strconv"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"

	"d7y.io/dragonfly/v2/scheduler/config"
	"d7y.io/dragonfly/v2/scheduler/supervisor"
	"d7y.io/dragonfly/v2/scheduler/supervisor/mocks"
)

const (
	HostMaxLoad = 10 * 1000
)

func TestPeer_New(t *testing.T) {
	tests := []struct {
		name   string
		id     string
		expect func(t *testing.T, peer *supervisor.Peer)
	}{
		{
			name: "create by normal config",
			id:   "normal",
			expect: func(t *testing.T, peer *supervisor.Peer) {
				assert := assert.New(t)
				assert.Equal("normal", peer.ID)
			},
		},
		{
			name: "create by special symbols",
			id:   "#@+:\b\t\\\"☹ ☺ ☻ (✿◠‿◠)",
			expect: func(t *testing.T, peer *supervisor.Peer) {
				assert := assert.New(t)
				assert.Equal("#@+:\b\t\\\"☹ ☺ ☻ (✿◠‿◠)", peer.ID)
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			task := mockATask("task")
			host := mockAHost("host")
			peer := supervisor.NewPeer(tc.id, task, host)
			tc.expect(t, peer)
		})
	}
}

func TestPeer_Tree(t *testing.T) {
	tests := []struct {
		name   string
		number int
		tree   map[int]int
		answer []int
		expect func(t *testing.T, peers []*supervisor.Peer, number int, answer []int)
	}{
		{
			name:   "test ID of tree structure",
			number: 6,
			tree:   map[int]int{1: 0, 2: 0, 3: 1, 4: 1, 5: 2},
			answer: []int{0, 1, 2, 3, 4, 5},
			expect: func(t *testing.T, peers []*supervisor.Peer, number int, answer []int) {
				assert := assert.New(t)
				for i := 0; i < number; i++ {
					assert.Equal(strconv.Itoa(answer[i]), peers[i].ID)
				}
			},
		},
		{
			name:   "test TreeNodeCount of tree structure",
			number: 6,
			tree:   map[int]int{1: 0, 2: 0, 3: 1, 4: 1, 5: 2},
			answer: []int{6, 3, 2, 1, 1, 1},
			expect: func(t *testing.T, peers []*supervisor.Peer, number int, answer []int) {
				assert := assert.New(t)
				for i := 0; i < number; i++ {
					assert.Equal(answer[i], peers[i].GetTreeNodeCount())
				}
			},
		},
		{
			name:   "test TreeDepth of tree structure",
			number: 6,
			tree:   map[int]int{1: 0, 2: 0, 3: 1, 4: 1, 5: 2},
			answer: []int{1, 2, 2, 3, 3, 3},
			expect: func(t *testing.T, peers []*supervisor.Peer, number int, answer []int) {
				assert := assert.New(t)
				for i := 0; i < number; i++ {
					assert.Equal(answer[i], peers[i].GetTreeDepth())
				}
			},
		},
		{
			name:   "test Root of tree structure",
			number: 6,
			tree:   map[int]int{1: 0, 2: 0, 3: 1, 4: 1, 5: 2},
			answer: []int{0, 0, 0, 0, 0, 0},
			expect: func(t *testing.T, peers []*supervisor.Peer, number int, answer []int) {
				assert := assert.New(t)
				for i := 0; i < number; i++ {
					assert.Equal(strconv.Itoa(answer[i]), peers[i].GetRoot().ID)
				}
			},
		},
		{
			name:   "test Parent of tree structure",
			number: 6,
			tree:   map[int]int{1: 0, 2: 0, 3: 1, 4: 1, 5: 2},
			answer: []int{-1, 0, 0, 1, 1, 2},
			expect: func(t *testing.T, peers []*supervisor.Peer, number int, answer []int) {
				assert := assert.New(t)
				for i := 0; i < number; i++ {
					parent, success := peers[i].GetParent()
					if answer[i] < 0 {
						assert.Equal((*supervisor.Peer)(nil), parent)
						assert.False(success)
					} else {
						assert.Equal(strconv.Itoa(answer[i]), parent.ID)
						assert.True(success)
					}
				}
			},
		},
		{
			name:   "test Ancestor of tree structure",
			number: 6,
			tree:   map[int]int{1: 0, 2: 0, 3: 1, 4: 1, 5: 2},
			answer: []int{},
			expect: func(t *testing.T, peers []*supervisor.Peer, number int, answer []int) {
				assert := assert.New(t)
				assert.False(peers[0].IsAncestor(peers[0]))
				assert.False(peers[0].IsAncestor(nil))

				assert.True(peers[0].IsAncestor(peers[5]))
				assert.False(peers[5].IsAncestor(peers[0]))

				assert.True(peers[1].IsAncestor(peers[4]))
				assert.False(peers[4].IsAncestor(peers[1]))
			},
		},
		{
			name:   "test Descendant of tree structure",
			number: 6,
			tree:   map[int]int{1: 0, 2: 0, 3: 1, 4: 1, 5: 2},
			answer: []int{},
			expect: func(t *testing.T, peers []*supervisor.Peer, number int, answer []int) {
				assert := assert.New(t)
				assert.False(peers[0].IsDescendant(peers[0]))
				assert.False(peers[0].IsDescendant(nil))

				assert.False(peers[0].IsDescendant(peers[5]))
				assert.True(peers[5].IsDescendant(peers[0]))

				assert.False(peers[1].IsDescendant(peers[4]))
				assert.True(peers[4].IsDescendant(peers[1]))
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var peers []*supervisor.Peer
			task := mockATask("task")
			for i := 0; i < tc.number; i++ {
				index := strconv.Itoa(i)
				peer := mockAPeer(index, task)
				if i > 0 {
					peer.ReplaceParent(peers[tc.tree[i]])
				}
				peers = append(peers, peer)
			}
			tc.expect(t, peers, tc.number, tc.answer)
		})
	}
}

func TestPeer_Cost(t *testing.T) {
	tests := []struct {
		name          string
		finishedCount []int32
		cost          []int
		expect        func(t *testing.T, peer *supervisor.Peer, cost []int)
	}{
		{
			name:          "normal workflow",
			finishedCount: []int32{2, 3, 4},
			cost:          []int{3, 4, 5},
			expect: func(t *testing.T, peer *supervisor.Peer, cost []int) {
				assert := assert.New(t)

				costFetch := peer.GetPieceCosts()
				assert.ElementsMatch(costFetch, cost)

				average, success := peer.GetPieceAverageCost()
				assert.True(success)
				assert.Equal(4, average)
				assert.Equal(peer.SortedValue(), 4*HostMaxLoad+100)
			},
		},
		{
			name:          "no workflow will be neglected",
			finishedCount: []int32{},
			cost:          []int{},
			expect: func(t *testing.T, peer *supervisor.Peer, cost []int) {
				assert := assert.New(t)

				average, success := peer.GetPieceAverageCost()
				assert.False(success)
				assert.Equal(0, average)
			},
		},
		{
			name: "long workflow will be clipped",
			finishedCount: []int32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
				11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22},
			cost: []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
				11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22},
			expect: func(t *testing.T, peer *supervisor.Peer, cost []int) {
				assert := assert.New(t)

				costFetch := peer.GetPieceCosts()
				assert.ElementsMatch(costFetch, cost[2:])

				average, success := peer.GetPieceAverageCost()
				assert.True(success)
				assert.Equal(12, average)
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			task := mockATask("task")
			peer := mockAPeer("peer", task)
			for i := 0; i < len(tc.finishedCount); i++ {
				peer.UpdateProgress(tc.finishedCount[i], tc.cost[i])
			}
			tc.expect(t, peer, tc.cost)
		})
	}
}

func TestPeer_Status(t *testing.T) {
	tests := []struct {
		name       string
		status     supervisor.PeerStatus
		statusName string
		judgeArray []bool
		expect     func(t *testing.T, peer *supervisor.Peer, status supervisor.PeerStatus, statusName string, judgeArray []bool)
	}{
		{
			name:       "status Waiting",
			status:     supervisor.PeerStatusWaiting,
			statusName: "Waiting",
			judgeArray: []bool{false, true, false, false, false, false},
			expect: func(t *testing.T, peer *supervisor.Peer, status supervisor.PeerStatus, statusName string, judgeArray []bool) {
				assert := assert.New(t)
				assert.Equal(supervisor.PeerStatus.String(status), statusName)
				assert.Equal(peer.GetStatus(), status)

				statutusJudgeArray := []bool{
					peer.IsRunning(), peer.IsWaiting(), peer.IsSuccess(),
					peer.IsDone(), peer.IsBad(), peer.IsFail(),
				}
				assert.Equal(statutusJudgeArray, judgeArray)
			},
		},
		{
			name:       "status Running",
			status:     supervisor.PeerStatusRunning,
			statusName: "Running",
			judgeArray: []bool{true, false, false, false, false, false},
			expect: func(t *testing.T, peer *supervisor.Peer, status supervisor.PeerStatus, statusName string, judgeArray []bool) {
				assert := assert.New(t)
				assert.Equal(supervisor.PeerStatus.String(status), statusName)
				assert.Equal(peer.GetStatus(), status)

				statutusJudgeArray := []bool{
					peer.IsRunning(), peer.IsWaiting(), peer.IsSuccess(),
					peer.IsDone(), peer.IsBad(), peer.IsFail(),
				}
				assert.Equal(statutusJudgeArray, judgeArray)
			},
		},
		{
			name:       "status Zombie",
			status:     supervisor.PeerStatusZombie,
			statusName: "Zombie",
			judgeArray: []bool{false, false, false, false, true, false},
			expect: func(t *testing.T, peer *supervisor.Peer, status supervisor.PeerStatus, statusName string, judgeArray []bool) {
				assert := assert.New(t)
				assert.Equal(supervisor.PeerStatus.String(status), statusName)
				assert.Equal(peer.GetStatus(), status)

				statutusJudgeArray := []bool{
					peer.IsRunning(), peer.IsWaiting(), peer.IsSuccess(),
					peer.IsDone(), peer.IsBad(), peer.IsFail(),
				}
				assert.Equal(statutusJudgeArray, judgeArray)
			},
		},
		{
			name:       "status Fail",
			status:     supervisor.PeerStatusFail,
			statusName: "Fail",
			judgeArray: []bool{false, false, false, true, true, true},
			expect: func(t *testing.T, peer *supervisor.Peer, status supervisor.PeerStatus, statusName string, judgeArray []bool) {
				assert := assert.New(t)
				assert.Equal(supervisor.PeerStatus.String(status), statusName)
				assert.Equal(peer.GetStatus(), status)

				statutusJudgeArray := []bool{
					peer.IsRunning(), peer.IsWaiting(), peer.IsSuccess(),
					peer.IsDone(), peer.IsBad(), peer.IsFail(),
				}
				assert.Equal(statutusJudgeArray, judgeArray)
			},
		},
		{
			name:       "status Success",
			status:     supervisor.PeerStatusSuccess,
			statusName: "Success",
			judgeArray: []bool{false, false, true, true, false, false},
			expect: func(t *testing.T, peer *supervisor.Peer, status supervisor.PeerStatus, statusName string, judgeArray []bool) {
				assert := assert.New(t)
				assert.Equal(supervisor.PeerStatus.String(status), statusName)
				assert.Equal(peer.GetStatus(), status)

				statutusJudgeArray := []bool{
					peer.IsRunning(), peer.IsWaiting(), peer.IsSuccess(),
					peer.IsDone(), peer.IsBad(), peer.IsFail(),
				}
				assert.Equal(statutusJudgeArray, judgeArray)
			},
		},
		{
			name:       "unknown",
			status:     100,
			statusName: "unknown",
			judgeArray: []bool{false, false, false, false, false, false},
			expect: func(t *testing.T, peer *supervisor.Peer, status supervisor.PeerStatus, statusName string, judgeArray []bool) {
				assert := assert.New(t)
				assert.Equal(supervisor.PeerStatus.String(status), statusName)
				assert.Equal(peer.GetStatus(), status)

				statutusJudgeArray := []bool{
					peer.IsRunning(), peer.IsWaiting(), peer.IsSuccess(),
					peer.IsDone(), peer.IsBad(), peer.IsFail(),
				}
				assert.Equal(statutusJudgeArray, judgeArray)
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			task := mockATask("task")
			peer := mockAPeer("peer", task)
			peer.SetStatus(tc.status)

			tc.expect(t, peer, tc.status, tc.statusName, tc.judgeArray)
		})
	}
}

func TestPeerManager_New(t *testing.T) {
	tests := []struct {
		name   string
		config *config.GCConfig
		mock   func(m *mocks.MockGCMockRecorder)
		expect func(t *testing.T, peerManager supervisor.PeerManager, err error)
	}{
		{
			name:   "create with default config",
			config: config.New().Scheduler.GC,
			mock: func(m *mocks.MockGCMockRecorder) {
				m.Add(gomock.Any()).Return(nil).AnyTimes()
			},
			expect: func(t *testing.T, peerManager supervisor.PeerManager, err error) {
				assert := assert.New(t)
				assert.NotNil(peerManager)
				assert.Nil(err)
			},
		},
		{
			name: "create with strange int",
			config: &config.GCConfig{
				PeerGCInterval: 1,
				TaskGCInterval: 1 >> 69,
				PeerTTL:        1 << 62,
				PeerTTI:        1,
				TaskTTL:        1,
				TaskTTI:        1,
			},
			mock: func(m *mocks.MockGCMockRecorder) {
				m.Add(gomock.Any()).Return(nil).AnyTimes()
			},
			expect: func(t *testing.T, peerManager supervisor.PeerManager, err error) {
				assert := assert.New(t)
				assert.NotNil(peerManager)
				assert.Nil(err)
			},
		},
		{
			name:   "gc failed",
			config: config.New().Scheduler.GC,
			mock: func(m *mocks.MockGCMockRecorder) {
				m.Add(gomock.Any()).Return(errors.New("mockError")).AnyTimes()
			},
			expect: func(t *testing.T, peerManager supervisor.PeerManager, err error) {
				assert := assert.New(t)
				assert.Nil(peerManager)
				assert.Error(err)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			mockHostManager := mocks.NewMockHostManager(ctl)
			mockGC := mocks.NewMockGC(ctl)
			tc.mock(mockGC.EXPECT())

			peerManager, err := supervisor.NewPeerManager(tc.config, mockGC, mockHostManager)
			tc.expect(t, peerManager, err)
		})
	}
}

func TestPeerManager_GetPeer(t *testing.T) {
	tests := []struct {
		name   string
		number int
		fetch  int
		expect func(t *testing.T, peer *supervisor.Peer, success bool, err error)
	}{
		{
			name:   "fetch first peer",
			number: 3,
			fetch:  0,
			expect: func(t *testing.T, peer *supervisor.Peer, success bool, err error) {
				assert := assert.New(t)
				assert.Equal("0", peer.ID)
				assert.True(success)
				assert.Nil(err)
			},
		},
		{
			name:   "fetch last peer",
			number: 3,
			fetch:  2,
			expect: func(t *testing.T, peer *supervisor.Peer, success bool, err error) {
				assert := assert.New(t)
				assert.Equal("2", peer.ID)
				assert.True(success)
				assert.Nil(err)
			},
		},
		{
			name:   "fetch not exist peer",
			number: 3,
			fetch:  -1,
			expect: func(t *testing.T, peer *supervisor.Peer, success bool, err error) {
				assert := assert.New(t)
				assert.Equal((*supervisor.Peer)(nil), peer)
				assert.False(success)
				assert.Nil(err)
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			mockHostManager := mocks.NewMockHostManager(ctl)
			mockGC := mocks.NewMockGC(ctl)
			mockGC.EXPECT().Add(gomock.Any()).Return(nil).AnyTimes()

			cfg := config.New()
			peerManager, err := supervisor.NewPeerManager(cfg.Scheduler.GC, mockGC, mockHostManager)
			task := mockATask("123")
			for i := 0; i < tc.number; i++ {
				index := strconv.Itoa(i)
				peer := mockAPeer(index, task)
				peerManager.Add(peer)
			}
			peer, success := peerManager.Get(strconv.Itoa(tc.fetch))
			tc.expect(t, peer, success, err)
		})
	}
}

func TestPeerManager_Add(t *testing.T) {
	tests := []struct {
		name   string
		ID     []int
		expect func(t *testing.T, peer []*supervisor.Peer)
	}{
		{
			name: "add seperative peers",
			ID:   []int{1, 2, 3},
			expect: func(t *testing.T, peers []*supervisor.Peer) {
				assert := assert.New(t)
				assert.Len(peers, 3)
			},
		},
		{
			name: "add duplicate peers",
			ID:   []int{1, 1, 1},
			expect: func(t *testing.T, peers []*supervisor.Peer) {
				assert := assert.New(t)
				assert.Len(peers, 1)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			mockHostManager := mocks.NewMockHostManager(ctl)
			mockGC := mocks.NewMockGC(ctl)
			mockGC.EXPECT().Add(gomock.Any()).Return(nil).AnyTimes()

			cfg := config.New()
			peerManager, _ := supervisor.NewPeerManager(cfg.Scheduler.GC, mockGC, mockHostManager)
			task := mockATask("123")
			for _, i := range tc.ID {
				index := strconv.Itoa(i)
				peer := mockAPeer(index, task)
				peerManager.Add(peer)
			}
			peers := peerManager.GetPeersByTask("123")
			tc.expect(t, peers)
		})
	}
}

func TestPeerManager_GetPeersByTask(t *testing.T) {
	tests := []struct {
		name   string
		tasks  map[*supervisor.Task]int
		fetch  string
		expect func(t *testing.T, peer []*supervisor.Peer)
	}{
		{
			name:  "peer for a task",
			tasks: map[*supervisor.Task]int{mockATask("123"): 3},
			fetch: "123",
			expect: func(t *testing.T, peers []*supervisor.Peer) {
				assert := assert.New(t)
				assert.Len(peers, 3)
			},
		},
		{
			name:  "one from two task",
			tasks: map[*supervisor.Task]int{mockATask("123"): 2, mockATask("456"): 3},
			fetch: "123",
			expect: func(t *testing.T, peers []*supervisor.Peer) {
				assert := assert.New(t)
				assert.Len(peers, 2)
			},
		},
		{
			name:  "no peer for a task",
			tasks: map[*supervisor.Task]int{mockATask("123"): 1},
			fetch: "456",
			expect: func(t *testing.T, peers []*supervisor.Peer) {
				assert := assert.New(t)
				assert.Len(peers, 0)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			mockHostManager := mocks.NewMockHostManager(ctl)
			mockGC := mocks.NewMockGC(ctl)
			mockGC.EXPECT().Add(gomock.Any()).Return(nil).AnyTimes()

			cfg := config.New()
			peerManager, _ := supervisor.NewPeerManager(cfg.Scheduler.GC, mockGC, mockHostManager)
			nowAt := 0
			for task, num := range tc.tasks {
				for i := nowAt; i < nowAt+num; i++ {
					index := strconv.Itoa(i)
					peer := mockAPeer(index, task)
					t.Log(i, index, nowAt, peer.ID, num)
					peerManager.Add(peer)
				}
				nowAt += num
			}
			peers := peerManager.GetPeersByTask(tc.fetch)
			tc.expect(t, peers)
		})
	}
}

func TestPeerManager_Delete(t *testing.T) {
	tests := []struct {
		name   string
		number int
		delete int
		fetch  int
		expect func(t *testing.T, peer *supervisor.Peer, success bool)
	}{
		{
			name:   "delete exist peer",
			number: 1,
			delete: 0,
			fetch:  0,
			expect: func(t *testing.T, peer *supervisor.Peer, success bool) {
				assert := assert.New(t)
				assert.Nil(peer)
				assert.False(success)
			},
		},
		{
			name:   "delete not exist peer",
			number: 1,
			delete: 100,
			fetch:  0,
			expect: func(t *testing.T, peer *supervisor.Peer, success bool) {
				assert := assert.New(t)
				assert.NotNil(peer)
				assert.True(success)
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			mockHostManager := mocks.NewMockHostManager(ctl)
			mockGC := mocks.NewMockGC(ctl)
			mockGC.EXPECT().Add(gomock.Any()).Return(nil).AnyTimes()

			cfg := config.New()
			peerManager, _ := supervisor.NewPeerManager(cfg.Scheduler.GC, mockGC, mockHostManager)
			task := mockATask("123")
			for i := 0; i < tc.number; i++ {
				index := strconv.Itoa(i)
				peer := mockAPeer(index, task)
				peerManager.Add(peer)
			}
			peerManager.Delete(strconv.Itoa(tc.delete))
			peer, success := peerManager.Get(strconv.Itoa(tc.fetch))

			tc.expect(t, peer, success)
		})
	}
}

func mockAPeer(ID string, task *supervisor.Task) *supervisor.Peer {
	host := supervisor.NewClientHost(ID, "127.0.0.1", "Client", 8080, 8081, "", "", "")
	return supervisor.NewPeer(ID, task, host)
}
