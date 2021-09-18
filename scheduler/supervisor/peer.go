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
	peerTTL     time.Duration
	peerTTI     time.Duration
	peers       *sync.Map
	lock        sync.RWMutex
}

func NewPeerManager(cfg *config.GCConfig, gcManager gc.GC, hostManager HostManager) (PeerManager, error) {
	m := &peerManager{
		hostManager: hostManager,
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
		elapsed := time.Since(peer.lastAccessAt.Load())

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
	// ID specifies ID of peer
	ID string
	// Task specifies
	Task *Task
	// Host specifies
	Host *Host
	// TotalPieceCount specifies downloaded finished piece count
	TotalPieceCount atomic.Int32
	// peer create time
	CreateAt *atomic.Time
	// peer last access time
	lastAccessAt *atomic.Time
	// parent is peer parent and type is *Peer
	parent atomic.Value
	// children is peer children map
	children *sync.Map
	// status is peer status and type is PeerStatus
	status atomic.Value
	// pieceCosts is piece historical download time
	pieceCosts []int
	// conn is channel instance and type is *Channel
	conn atomic.Value
	// leave is whether the peer leaves
	leave atomic.Bool
	// logger is peer log instance
	logger *logger.SugaredLoggerOnWith
	// peer lock
	lock sync.RWMutex
}

func NewPeer(id string, task *Task, host *Host) *Peer {
	peer := &Peer{
		ID:           id,
		Task:         task,
		Host:         host,
		CreateAt:     atomic.NewTime(time.Now()),
		lastAccessAt: atomic.NewTime(time.Now()),
		children:     &sync.Map{},
		logger:       logger.WithTaskAndPeerID(task.ID, id),
	}

	peer.status.Store(PeerStatusWaiting)
	return peer
}

func (peer *Peer) GetTreeLen() int {
	count := 1
	peer.children.Range(func(key, value interface{}) bool {
		node := value.(*Peer)
		count += node.GetTreeLen()
		return true
	})

	return count
}

func (peer *Peer) GetTreeDepth() int {
	var deep int
	node := peer
	for node != nil {
		deep++
		parent, ok := node.GetParent()
		if !ok || node.Host.IsCDN {
			break
		}
		node = parent
	}
	return deep
}

func (peer *Peer) GetRoot() *Peer {
	node := peer
	for node != nil {
		parent, ok := node.GetParent()
		if !ok || node.Host.IsCDN {
			break
		}
		node = parent
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
		parent, ok := node.GetParent()
		if !ok {
			return false
		} else if node.ID == ancestor.ID {
			return true
		}
		node = parent
	}
	return false
}

// IsAncestorOf if offspring is offspring of peer
func (peer *Peer) IsAncestor(offspring *Peer) bool {
	return isDescendant(peer, offspring)
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
	oldParent, ok := peer.GetParent()
	if ok {
		oldParent.deleteChild(peer)
	}

	peer.SetParent(parent)
	if parent != nil {
		parent.insertChild(peer)
	}
}

func (peer *Peer) GetChildren() *sync.Map {
	return peer.children
}

func (peer *Peer) SetParent(parent *Peer) {
	peer.status.Store(parent)
}

func (peer *Peer) GetParent() (*Peer, bool) {
	parent := peer.parent.Load()
	if parent != nil {
		return nil, false
	}

	return parent.(*Peer), true
}

func (peer *Peer) Touch() {
	peer.lock.Lock()
	defer peer.lock.Unlock()

	peer.lastAccessAt.Store(time.Now())
	if peer.GetStatus() == PeerStatusZombie && !peer.leave.Load() {
		peer.SetStatus(PeerStatusRunning)
	}
	peer.Task.Touch()
}

func (peer *Peer) GetPieceCosts() []int {
	peer.lock.RLock()
	defer peer.lock.RUnlock()
	return peer.pieceCosts
}

func (peer *Peer) GetPieceAverageCost() (int, bool) {
	costs := peer.GetPieceCosts()
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
	if finishedCount > peer.TotalPieceCount.Load() {
		peer.TotalPieceCount.Store(finishedCount)

		peer.lock.Lock()
		peer.pieceCosts = append(peer.pieceCosts, cost)
		if len(peer.pieceCosts) > 20 {
			peer.pieceCosts = peer.pieceCosts[len(peer.pieceCosts)-20:]
		}
		peer.lock.Unlock()

		peer.Task.UpdatePeer(peer)
		return
	}
}

func (peer *Peer) GetSortKeys() (key1, key2 int) {
	peer.lock.RLock()
	defer peer.lock.RUnlock()

	key1 = int(peer.TotalPieceCount.Load())
	key2 = peer.getFreeLoad()
	return
}

func (peer *Peer) getFreeLoad() int {
	if peer.Host == nil {
		return 0
	}
	return int(peer.Host.GetFreeUploadLoad())
}

func (peer *Peer) SetStatus(status PeerStatus) {
	peer.status.Store(status)
}

func (peer *Peer) GetStatus() PeerStatus {
	return peer.status.Load().(PeerStatus)
}

func (peer *Peer) Leave() {
	peer.leave.Store(true)
}

func (peer *Peer) IsLeave() bool {
	return peer.leave.Load()
}

func (peer *Peer) IsRunning() bool {
	return peer.GetStatus() == PeerStatusRunning
}

func (peer *Peer) IsWaiting() bool {
	return peer.GetStatus() == PeerStatusWaiting
}

func (peer *Peer) IsSuccess() bool {
	return peer.GetStatus() == PeerStatusSuccess
}

func (peer *Peer) IsDone() bool {
	return peer.GetStatus() == PeerStatusSuccess || peer.GetStatus() == PeerStatusFail
}

func (peer *Peer) IsBad() bool {
	return peer.GetStatus() == PeerStatusFail || peer.GetStatus() == PeerStatusZombie
}

func (peer *Peer) IsFail() bool {
	return peer.GetStatus() == PeerStatusFail
}

func (peer *Peer) BindNewConn(stream scheduler.Scheduler_ReportPieceResultServer) (*Channel, bool) {
	peer.lock.Lock()
	defer peer.lock.Unlock()
	if peer.GetStatus() == PeerStatusWaiting {
		peer.SetStatus(PeerStatusRunning)
	}
	peer.SetConn(newChannel(stream))
	return peer.GetConn()
}

func (peer *Peer) SetConn(conn *Channel) {
	peer.conn.Store(conn)
}

func (peer *Peer) GetConn() (*Channel, bool) {
	conn := peer.conn.Load()
	if conn != nil {
		return nil, false
	}

	return conn.(*Channel), true
}

func (peer *Peer) IsConnected() bool {
	conn, ok := peer.GetConn()
	if !ok {
		return false
	}

	return !conn.IsClosed()
}

func (peer *Peer) SendSchedulePacket(packet *scheduler.PeerPacket) error {
	conn, ok := peer.GetConn()
	if !ok {
		return errors.New("client peer is not connected")
	}
	return conn.Send(packet)
}

func (peer *Peer) CloseChannel(err error) error {
	conn, ok := peer.GetConn()
	if !ok {
		return errors.New("client peer is not connected")
	}

	conn.CloseWithError(err)
	return nil
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
