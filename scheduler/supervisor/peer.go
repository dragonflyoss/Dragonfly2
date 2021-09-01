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

package supervisor

import (
	"io"
	"sync"
	"time"

	"d7y.io/dragonfly/v2/pkg/rpc/scheduler"
	"github.com/pkg/errors"
	"go.uber.org/atomic"
)

type PeerStatus uint8

func (status PeerStatus) String() string {
	switch status {
	case PeerStatusWaiting:
		return "Waiting"
	case PeerStatusRunning:
		return "Running"
	case PeerStatusZombie:
		return "Zombie"
	case PeerStatusFail:
		return "Fail"
	case PeerStatusSuccess:
		return "Success"
	default:
		return "unknown"
	}
}

const (
	PeerStatusWaiting PeerStatus = iota
	PeerStatusRunning
	PeerStatusZombie
	PeerStatusFail
	PeerStatusSuccess
)

type Peer struct {
	lock sync.RWMutex
	// PeerID specifies ID of peer
	PeerID string
	// Task specifies
	Task *Task
	// Host specifies
	Host *PeerHost
	conn *Channel
	// createTime
	CreateTime time.Time
	// finishedNum specifies downloaded finished piece number
	finishedNum    atomic.Int32
	lastAccessTime time.Time
	parent         *Peer
	children       sync.Map
	status         PeerStatus
	costHistory    []int
	leave          atomic.Bool
}

func NewPeer(peerID string, task *Task, host *PeerHost) *Peer {
	return &Peer{
		PeerID:         peerID,
		Task:           task,
		Host:           host,
		CreateTime:     time.Now(),
		lastAccessTime: time.Now(),
		status:         PeerStatusWaiting,
	}
}

// TODO: remove
func (peer *Peer) GetWholeTreeNode() int {
	count := 1
	peer.children.Range(func(key, value interface{}) bool {
		peerNode := value.(*Peer)
		count += peerNode.GetWholeTreeNode()
		return true
	})
	return count
}

func (peer *Peer) GetLastAccessTime() time.Time {
	peer.lock.RLock()
	defer peer.lock.RUnlock()
	return peer.lastAccessTime
}

func (peer *Peer) Touch() {
	peer.lock.Lock()
	defer peer.lock.Unlock()
	peer.lastAccessTime = time.Now()
	if peer.status == PeerStatusZombie && !peer.leave.Load() {
		peer.status = PeerStatusRunning
	}
	peer.Task.Touch()
}

func (peer *Peer) associateChild(child *Peer) {
	peer.children.Store(child.PeerID, child)
	peer.Host.IncUploadLoad()
	peer.Task.UpdatePeer(peer)
}

func (peer *Peer) disassociateChild(child *Peer) {
	peer.children.Delete(child.PeerID)
	peer.Host.DecUploadLoad()
	peer.Task.UpdatePeer(peer)
}

func (peer *Peer) ReplaceParent(parent *Peer) {
	oldParent := peer.parent
	if oldParent != nil {
		oldParent.disassociateChild(peer)
	}
	peer.parent = parent
	if parent != nil {
		parent.associateChild(peer)
	}
}

func (peer *Peer) GetCostHistory() []int {
	peer.lock.RLock()
	defer peer.lock.RUnlock()
	return peer.costHistory
}

func (peer *Peer) GetCost() int {
	peer.lock.RLock()
	defer peer.lock.RUnlock()
	if len(peer.costHistory) < 1 {
		return int(time.Second / time.Millisecond)
	}
	totalCost := 0
	for _, cost := range peer.costHistory {
		totalCost += cost
	}
	return totalCost / len(peer.costHistory)
}

func (peer *Peer) UpdateProgress(finishedCount int32, cost int) {
	peer.lock.Lock()
	if finishedCount > peer.finishedNum.Load() {
		peer.finishedNum.Store(finishedCount)
		peer.costHistory = append(peer.costHistory, cost)
		if len(peer.costHistory) > 20 {
			peer.costHistory = peer.costHistory[len(peer.costHistory)-20:]
		}
		peer.lock.Unlock()
		peer.Task.UpdatePeer(peer)
		return
	}
	peer.lock.Unlock()
}

func (peer *Peer) GetDepth() int {
	peer.lock.RLock()
	defer peer.lock.RUnlock()
	var deep int
	node := peer
	for node != nil {
		deep++
		if node.parent == nil || node.Host.CDN {
			break
		}
		node = node.parent
	}
	return deep
}

func (peer *Peer) GetTreeRoot() *Peer {
	node := peer
	for node != nil {
		if node.parent == nil || node.Host.CDN {
			break
		}
		node = node.parent
	}
	return node
}

// IsDescendantOf if peer is offspring of ancestor
func (peer *Peer) IsDescendantOf(ancestor *Peer) bool {
	return isDescendant(ancestor, peer)
}

func isDescendant(ancestor, offspring *Peer) bool {
	if ancestor == nil || offspring == nil {
		return false
	}
	// TODO avoid circulation
	node := offspring
	for node != nil {
		if node.parent == nil {
			return false
		} else if node.PeerID == ancestor.PeerID {
			return true
		}
		node = node.parent
	}
	return false
}

// IsAncestorOf if offspring is offspring of peer
func (peer *Peer) IsAncestorOf(offspring *Peer) bool {
	return isDescendant(peer, offspring)
}

func (peer *Peer) GetSortKeys() (key1, key2 int) {
	peer.lock.RLock()
	defer peer.lock.RUnlock()
	key1 = int(peer.finishedNum.Load())
	key2 = peer.getFreeLoad()
	return
}

func (peer *Peer) getFreeLoad() int {
	if peer.Host == nil {
		return 0
	}
	return peer.Host.GetFreeUploadLoad()
}

func GetDiffPieceNum(dst *Peer, src *Peer) int32 {
	return dst.finishedNum.Load() - src.finishedNum.Load()
}

func (peer *Peer) GetParent() *Peer {
	peer.lock.RLock()
	defer peer.lock.RUnlock()
	return peer.parent
}

func (peer *Peer) GetChildren() *sync.Map {
	peer.lock.RLock()
	defer peer.lock.RUnlock()
	return &peer.children
}

func (peer *Peer) SetStatus(status PeerStatus) {
	peer.lock.Lock()
	defer peer.lock.Unlock()
	peer.status = status
}

func (peer *Peer) GetStatus() PeerStatus {
	peer.lock.RLock()
	defer peer.lock.RUnlock()
	return peer.status
}

func (peer *Peer) GetFinishedNum() int32 {
	return peer.finishedNum.Load()
}

func (peer *Peer) MarkLeave() {
	peer.leave.Store(true)
}

func (peer *Peer) IsLeave() bool {
	return peer.leave.Load()
}

func (peer *Peer) IsRunning() bool {
	return peer.status == PeerStatusRunning
}

func (peer *Peer) IsWaiting() bool {
	return peer.status == PeerStatusWaiting
}

func (peer *Peer) IsSuccess() bool {
	return peer.status == PeerStatusSuccess
}

func (peer *Peer) IsDone() bool {
	return peer.status == PeerStatusSuccess || peer.status == PeerStatusFail
}

func (peer *Peer) IsBad() bool {
	return peer.status == PeerStatusFail || peer.status == PeerStatusZombie
}

func (peer *Peer) IsFail() bool {
	return peer.status == PeerStatusFail
}

func (peer *Peer) BindNewConn(stream scheduler.Scheduler_ReportPieceResultServer) *Channel {
	peer.lock.Lock()
	defer peer.lock.Unlock()
	if peer.status == PeerStatusWaiting {
		peer.status = PeerStatusRunning
	}
	peer.conn = newChannel(stream)
	return peer.conn
}

func (peer *Peer) IsConnected() bool {
	peer.lock.RLock()
	defer peer.lock.RUnlock()
	if peer.conn == nil {
		return false
	}
	return !peer.conn.IsClosed()
}

func (peer *Peer) SendSchedulePacket(packet *scheduler.PeerPacket) error {
	peer.lock.Lock()
	defer peer.lock.Unlock()
	if peer.conn != nil {
		return peer.conn.Send(packet)
	}
	return errors.New("client peer is not connected")
}

func (peer *Peer) CloseChannel(err error) error {
	peer.lock.Lock()
	defer peer.lock.Unlock()
	if peer.conn != nil {
		peer.conn.CloseWithError(err)
		return nil
	}
	return errors.New("client peer is not connected")
}

type Channel struct {
	startOnce sync.Once
	sender    chan *scheduler.PeerPacket
	receiver  chan *scheduler.PieceResult
	stream    scheduler.Scheduler_ReportPieceResultServer
	closed    *atomic.Bool
	done      chan struct{}
	wg        sync.WaitGroup
	err       error
}

func newChannel(stream scheduler.Scheduler_ReportPieceResultServer) *Channel {
	c := &Channel{
		sender:   make(chan *scheduler.PeerPacket, 10),
		receiver: make(chan *scheduler.PieceResult, 10),
		stream:   stream,
		closed:   atomic.NewBool(false),
		done:     make(chan struct{}),
	}
	c.start()
	return c
}

func (c *Channel) start() {
	c.startOnce.Do(func() {
		c.wg.Add(2)
		go c.receiveLoop()
		go c.sendLoop()
	})
}

func (c *Channel) Send(packet *scheduler.PeerPacket) error {
	select {
	case <-c.done:
		return errors.New("conn has closed")
	case c.sender <- packet:
		return nil
	default:
		return errors.New("send channel already full")
	}
}

func (c *Channel) Receiver() <-chan *scheduler.PieceResult {
	return c.receiver
}

func (c *Channel) Close() {
	if !c.closed.CAS(false, true) {
		return
	}
	go func() {
		close(c.done)
		c.wg.Wait()
	}()
}

func (c *Channel) CloseWithError(err error) {
	c.err = err
	c.Close()
}

func (c *Channel) Err() error {
	err := c.err
	return err
}

func (c *Channel) Done() <-chan struct{} {
	if c.done == nil {
		c.done = make(chan struct{})
	}
	d := c.done
	return d
}

func (c *Channel) IsClosed() bool {
	return c.closed.Load()
}

func (c *Channel) receiveLoop() {
	defer func() {
		close(c.receiver)
		c.wg.Done()
		c.Close()
	}()

	for {
		select {
		case <-c.done:
			return
		default:
			pieceResult, err := c.stream.Recv()
			if err == io.EOF {
				return
			}
			if err != nil {
				c.err = err
				return
			}
			c.receiver <- pieceResult
		}
	}
}

func (c *Channel) sendLoop() {
	defer func() {
		c.wg.Done()
		c.Close()
	}()

	for {
		select {
		case <-c.done:
			return
		case packet := <-c.sender:
			if err := c.stream.Send(packet); err != nil {
				c.err = err
				return
			}
		}
	}
}
