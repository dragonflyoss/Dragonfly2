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

	logger "d7y.io/dragonfly/v2/internal/dflog"
	gc "d7y.io/dragonfly/v2/pkg/gc"
	"d7y.io/dragonfly/v2/pkg/rpc/scheduler"
	"d7y.io/dragonfly/v2/scheduler/config"
	"github.com/pkg/errors"
	"go.uber.org/atomic"
)

const (
	PeerGCID = "peer"
)

type PeerManager interface {
	Add(*Peer)

	Get(string) (*Peer, bool)

	Delete(string)

	GetPeersByTask(string) []*Peer

	GetPeers() *sync.Map
}

type peerManager struct {
	hostManager HostManager
	gcTicker    *time.Ticker
	peerTTL     time.Duration
	peerTTI     time.Duration
	peers       *sync.Map
	lock        sync.RWMutex
}

func NewPeerManager(cfg *config.GCConfig, gcManager gc.GC, hostManager HostManager) (PeerManager, error) {
	m := &peerManager{
		hostManager: hostManager,
		gcTicker:    time.NewTicker(cfg.PeerGCInterval),
		peerTTL:     cfg.PeerTTL,
		peerTTI:     cfg.PeerTTI,
		peers:       &sync.Map{},
	}

	// Add GC task
	if err := gcManager.Add(gc.Task{
		ID:       PeerGCID,
		Interval: cfg.PeerGCInterval,
		Timeout:  cfg.PeerGCInterval,
		Runner:   m,
	}); err != nil {
		return nil, err
	}

	return m, nil
}

func (m *peerManager) Add(peer *Peer) {
	m.lock.Lock()
	defer m.lock.Unlock()

	peer.Host.AddPeer(peer)
	peer.Task.AddPeer(peer)
	m.peers.Store(peer.ID, peer)
}

func (m *peerManager) Get(id string) (*Peer, bool) {
	peer, ok := m.peers.Load(id)
	if !ok {
		return nil, false
	}

	return peer.(*Peer), ok
}

func (m *peerManager) Delete(id string) {
	m.lock.Lock()
	defer m.lock.Unlock()

	if peer, ok := m.Get(id); ok {
		peer.Host.DeletePeer(id)
		peer.Task.DeletePeer(peer)
		peer.ReplaceParent(nil)
		m.peers.Delete(id)
	}
}

func (m *peerManager) GetPeersByTask(taskID string) []*Peer {
	var peers []*Peer
	m.peers.Range(func(key, value interface{}) bool {
		peer := value.(*Peer)
		if peer.Task.ID == taskID {
			peers = append(peers, peer)
		}
		return true
	})
	return peers
}

func (m *peerManager) GetPeers() *sync.Map {
	return m.peers
}

func (m *peerManager) RunGC() error {
	m.peers.Range(func(key, value interface{}) bool {
		id := key.(string)
		peer := value.(*Peer)
		elapsed := time.Since(peer.GetLastAccessAt())

		if elapsed > m.peerTTI && !peer.IsDone() && !peer.Host.IsCDN {
			if !peer.IsConnected() {
				peer.Leave()
			}
			peer.Log().Infof("peer has been more than %s since last access, it's status changes from %s to zombie", m.peerTTI, peer.GetStatus().String())
			peer.SetStatus(PeerStatusZombie)
		}

		if peer.IsLeave() || peer.IsFail() || elapsed > m.peerTTL {
			if elapsed > m.peerTTL {
				peer.Log().Infof("delete peer because %s have passed since last access", m.peerTTL)
			}
			m.Delete(id)
			if peer.Host.GetPeersLen() == 0 {
				m.hostManager.Delete(peer.Host.UUID)
			}
			if peer.Task.GetPeers().Size() == 0 {
				peer.Task.Log().Info("peers is empty, task status become waiting")
				peer.Task.SetStatus(TaskStatusWaiting)
			}
		}

		return true
	})

	return nil
}

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
	// TODO add Seeding status
	PeerStatusZombie
	PeerStatusFail
	PeerStatusSuccess
)

type Peer struct {
	lock sync.RWMutex
	// ID specifies ID of peer
	ID string
	// Task specifies
	Task *Task
	// Host specifies
	Host *Host
	conn *Channel
	// peer create time
	CreateAt time.Time
	// peer last access time
	lastAccessAt time.Time
	// finishedNum specifies downloaded finished piece number
	finishedNum atomic.Int32
	parent      *Peer
	children    *sync.Map
	status      PeerStatus
	costs       []int
	leave       atomic.Bool
	logger      *logger.SugaredLoggerOnWith
}

func NewPeer(id string, task *Task, host *Host) *Peer {
	return &Peer{
		ID:           id,
		Task:         task,
		Host:         host,
		CreateAt:     time.Now(),
		lastAccessAt: time.Now(),
		children:     &sync.Map{},
		status:       PeerStatusWaiting,
		logger:       logger.WithTaskAndPeerID(task.ID, id),
	}
}

// TODO: remove
func (peer *Peer) GetTreeLen() int {
	count := 1
	peer.children.Range(func(key, value interface{}) bool {
		node := value.(*Peer)
		count += node.GetTreeLen()
		return true
	})

	return count
}

func (peer *Peer) GetLastAccessAt() time.Time {
	peer.lock.RLock()
	defer peer.lock.RUnlock()

	return peer.lastAccessAt
}

func (peer *Peer) Touch() {
	peer.lock.Lock()
	defer peer.lock.Unlock()

	peer.lastAccessAt = time.Now()
	if peer.status == PeerStatusZombie && !peer.leave.Load() {
		peer.status = PeerStatusRunning
	}
	peer.Task.Touch()
}

func (peer *Peer) insertChild(child *Peer) {
	peer.children.Store(child.ID, child)
	peer.Host.CurrentUploadLoad.Inc()
	peer.Task.UpdatePeer(peer)
}

func (peer *Peer) deleteChild(child *Peer) {
	peer.children.Delete(child.ID)
	peer.Host.CurrentUploadLoad.Dec()
	peer.Task.UpdatePeer(peer)
}

func (peer *Peer) ReplaceParent(parent *Peer) {
	oldParent := peer.parent
	if oldParent != nil {
		oldParent.deleteChild(peer)
	}

	peer.parent = parent
	if parent != nil {
		parent.insertChild(peer)
	}
}

func (peer *Peer) GetCosts() []int {
	peer.lock.RLock()
	defer peer.lock.RUnlock()
	return peer.costs
}

func (peer *Peer) GetPieceAverageCost() (int, bool) {
	costs := peer.GetCosts()
	if len(costs) < 1 {
		return 0, false
	}

	totalCost := 0
	for _, cost := range costs {
		totalCost += cost
	}

	return totalCost / len(costs), true
}

func (peer *Peer) UpdateProgress(finishedCount int32, cost int) {
	if finishedCount > peer.finishedNum.Load() {
		peer.finishedNum.Store(finishedCount)

		peer.lock.Lock()
		peer.costs = append(peer.costs, cost)
		if len(peer.costs) > 20 {
			peer.costs = peer.costs[len(peer.costs)-20:]
		}
		peer.lock.Unlock()

		peer.Task.UpdatePeer(peer)
		return
	}
}

func (peer *Peer) GetTreeDepth() int {
	peer.lock.RLock()
	defer peer.lock.RUnlock()

	var deep int
	node := peer
	for node != nil {
		deep++
		if node.parent == nil || node.Host.IsCDN {
			break
		}
		node = node.parent
	}
	return deep
}

func (peer *Peer) GetRoot() *Peer {
	node := peer
	for node != nil {
		if node.parent == nil || node.Host.IsCDN {
			break
		}
		node = node.parent
	}

	return node
}

// IsDescendant if peer is offspring of ancestor
func (peer *Peer) IsDescendant(ancestor *Peer) bool {
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
		} else if node.ID == ancestor.ID {
			return true
		}
		node = node.parent
	}
	return false
}

// IsAncestorOf if offspring is offspring of peer
func (peer *Peer) IsAncestor(offspring *Peer) bool {
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
	return int(peer.Host.GetFreeUploadLoad())
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
	return peer.children
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

func (peer *Peer) Leave() {
	peer.leave.Store(true)
}

func (peer *Peer) IsLeave() bool {
	return peer.leave.Load()
}

func (peer *Peer) IsRunning() bool {
	peer.lock.RLock()
	defer peer.lock.RUnlock()
	return peer.status == PeerStatusRunning
}

func (peer *Peer) IsWaiting() bool {
	peer.lock.RLock()
	defer peer.lock.RUnlock()
	return peer.status == PeerStatusWaiting
}

func (peer *Peer) IsSuccess() bool {
	peer.lock.RLock()
	defer peer.lock.RUnlock()
	return peer.status == PeerStatusSuccess
}

func (peer *Peer) IsDone() bool {
	peer.lock.RLock()
	defer peer.lock.RUnlock()
	return peer.status == PeerStatusSuccess || peer.status == PeerStatusFail
}

func (peer *Peer) IsBad() bool {
	peer.lock.RLock()
	defer peer.lock.RUnlock()
	return peer.status == PeerStatusFail || peer.status == PeerStatusZombie
}

func (peer *Peer) IsFail() bool {
	peer.lock.RLock()
	defer peer.lock.RUnlock()
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

func (peer *Peer) Log() *logger.SugaredLoggerOnWith {
	return peer.logger
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
		sender:   make(chan *scheduler.PeerPacket),
		receiver: make(chan *scheduler.PieceResult),
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
		return errors.New("send channel is blocking")
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

func (c *Channel) Error() error {
	return c.err
}

func (c *Channel) Done() <-chan struct{} {
	if c.done == nil {
		c.done = make(chan struct{})
	}
	return c.done
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
