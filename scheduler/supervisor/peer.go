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
//go:generate mockgen -destination ./mocks/peer_mock.go -package mocks d7y.io/dragonfly/v2/scheduler/supervisor PeerManager

package supervisor

import (
	"io"
	"sync"
	"time"

	"github.com/bits-and-blooms/bitset"
	"github.com/pkg/errors"
	"go.uber.org/atomic"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/rpc/scheduler"
)

const (
	PeerGCID = "peer"
)

var ErrChannelBusy = errors.New("channel busy")

type PeerManager interface {
	// Add peer
	Add(*Peer)
	// Get peer
	Get(string) (*Peer, bool)
	// Delete peer
	Delete(string)
	// Get peer by task id
	GetPeersByTask(string) []*Peer
	// Get peers
	GetPeers() *sync.Map
}

type peerManager struct {
	// hostManager is host manager
	hostManager HostManager
	// peers is peer map
	peers *sync.Map
	// peerManager lock
	lock sync.RWMutex
}

func NewPeerManager(hostManager HostManager) PeerManager {
	return &peerManager{
		hostManager: hostManager,
		peers:       &sync.Map{},
	}
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
		peer.DeleteParent()
		m.peers.Delete(id)
	}
}

func (m *peerManager) GetPeersByTask(taskID string) []*Peer {
	var peers []*Peer
	m.peers.Range(func(_, value interface{}) bool {
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

type PeerStatus uint8

func (status PeerStatus) String() string {
	switch status {
	case PeerStatusWaiting:
		return "Waiting"
	case PeerStatusRunning:
		return "Running"
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
	PeerStatusFail
	PeerStatusSuccess
)

type Peer struct {
	// ID is ID of peer
	ID string
	// Task is peer task
	Task *Task
	// Host is peer host
	Host *Host
	// CreateAt is peer create time
	CreateAt *atomic.Time
	// LastAccessAt is peer last access time
	LastAccessAt *atomic.Time
	// parent is peer parent and type is *Peer
	parent atomic.Value
	// children is peer children map
	children *sync.Map
	// status is peer status and type is PeerStatus
	status atomic.Value
	// pieces is piece bitset
	Pieces bitset.BitSet
	// pieceCosts is piece historical download time
	pieceCosts []int
	// conn is channel instance and type is *Channel
	conn atomic.Value
	// leave is whether the peer leaves
	leave atomic.Bool
	// peer logger
	logger *logger.SugaredLoggerOnWith
	// peer lock
	lock sync.RWMutex
}

func NewPeer(id string, task *Task, host *Host) *Peer {
	peer := &Peer{
		ID:           id,
		Task:         task,
		Host:         host,
		Pieces:       bitset.BitSet{},
		CreateAt:     atomic.NewTime(time.Now()),
		LastAccessAt: atomic.NewTime(time.Now()),
		children:     &sync.Map{},
		logger:       logger.WithTaskAndPeerID(task.ID, id),
	}

	peer.status.Store(PeerStatusWaiting)
	return peer
}

func (peer *Peer) GetTreeNodeCount() int {
	count := 1
	peer.children.Range(func(key, value interface{}) bool {
		node := value.(*Peer)
		count += node.GetTreeNodeCount()
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
		} else if parent.ID == ancestor.ID {
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
	peer.Task.AddPeer(peer)
}

func (peer *Peer) deleteChild(child *Peer) {
	peer.children.Delete(child.ID)
	peer.Host.CurrentUploadLoad.Dec()
	peer.Task.AddPeer(peer)
}

func (peer *Peer) ReplaceParent(parent *Peer) {
	oldParent, ok := peer.GetParent()
	if ok {
		oldParent.deleteChild(peer)
	}

	peer.SetParent(parent)
	parent.insertChild(peer)
}

func (peer *Peer) DeleteParent() {
	oldParent, ok := peer.GetParent()
	if ok {
		oldParent.deleteChild(peer)
	}

	peer.SetParent(nil)
}

func (peer *Peer) GetChildren() *sync.Map {
	return peer.children
}

func (peer *Peer) SetParent(parent *Peer) {
	peer.parent.Store(parent)
}

func (peer *Peer) GetParent() (*Peer, bool) {
	parent := peer.parent.Load()
	if parent == nil {
		return nil, false
	}

	p, ok := parent.(*Peer)
	if p == nil || !ok {
		return nil, false
	}

	return p, true
}

func (peer *Peer) Touch() {
	peer.lock.Lock()
	defer peer.lock.Unlock()

}

func (peer *Peer) GetPieceCosts() []int {
	peer.lock.RLock()
	defer peer.lock.RUnlock()

	return peer.pieceCosts
}

func (peer *Peer) SetPieceCosts(costs ...int) {
	peer.lock.Lock()
	defer peer.lock.Unlock()

	peer.pieceCosts = append(peer.pieceCosts, costs...)
}

func (peer *Peer) AddPiece(num int32, cost int) {
	peer.Pieces.Set(uint(num))
	peer.SetPieceCosts(cost)
	peer.Task.AddPeer(peer)
}

func (peer *Peer) getFreeLoad() int32 {
	if peer.Host == nil {
		return 0
	}
	return peer.Host.GetFreeUploadLoad()
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

func (peer *Peer) IsFail() bool {
	return peer.GetStatus() == PeerStatusFail
}

func (peer *Peer) BindNewConn(stream scheduler.Scheduler_ReportPieceResultServer) (*Channel, bool) {
	peer.lock.Lock()
	defer peer.lock.Unlock()

	if peer.GetStatus() == PeerStatusWaiting {
		peer.SetStatus(PeerStatusRunning)
	}
	peer.setConn(newChannel(stream))
	return peer.getConn()
}

func (peer *Peer) setConn(conn *Channel) {
	peer.conn.Store(conn)
}

func (peer *Peer) getConn() (*Channel, bool) {
	conn := peer.conn.Load()
	if conn == nil {
		return nil, false
	}

	c, ok := conn.(*Channel)
	if c == nil || !ok {
		return nil, false
	}

	return c, true
}

func (peer *Peer) IsConnected() bool {
	conn, ok := peer.getConn()
	if !ok {
		return false
	}

	return !conn.IsClosed()
}

func (peer *Peer) SendSchedulePacket(packet *scheduler.PeerPacket) error {
	conn, ok := peer.getConn()
	if !ok {
		return errors.New("client peer is not connected")
	}

	return conn.Send(packet)
}

func (peer *Peer) CloseChannelWithError(err error) error {
	conn, ok := peer.getConn()
	if !ok {
		return errors.New("client peer is not connected")
	}

	conn.err = err
	conn.Close()
	return nil
}

func (peer *Peer) Log() *logger.SugaredLoggerOnWith {
	return peer.logger
}

type Channel struct {
	sender   chan *scheduler.PeerPacket
	receiver chan *scheduler.PieceResult
	stream   scheduler.Scheduler_ReportPieceResultServer
	closed   *atomic.Bool
	done     chan struct{}
	wg       sync.WaitGroup
	err      error
}

func newChannel(stream scheduler.Scheduler_ReportPieceResultServer) *Channel {
	c := &Channel{
		sender:   make(chan *scheduler.PeerPacket),
		receiver: make(chan *scheduler.PieceResult),
		stream:   stream,
		closed:   atomic.NewBool(false),
		done:     make(chan struct{}),
	}

	c.wg.Add(2)
	c.start()
	return c
}

func (c *Channel) start() {
	startWG := &sync.WaitGroup{}
	startWG.Add(2)

	go c.receiveLoop(startWG)
	go c.sendLoop(startWG)
	startWG.Wait()
}

func (c *Channel) Send(packet *scheduler.PeerPacket) error {
	select {
	case <-c.done:
		return errors.New("conn has closed")
	case c.sender <- packet:
		return nil
	default:
		return ErrChannelBusy
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

func (c *Channel) receiveLoop(startWG *sync.WaitGroup) {
	defer func() {
		close(c.receiver)
		c.wg.Done()
		c.Close()
	}()

	startWG.Done()

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

func (c *Channel) sendLoop(startWG *sync.WaitGroup) {
	defer func() {
		c.wg.Done()
		c.Close()
	}()

	startWG.Done()

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
