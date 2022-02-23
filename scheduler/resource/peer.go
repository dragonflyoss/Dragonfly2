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

package resource

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/bits-and-blooms/bitset"
	"github.com/go-http-utils/headers"
	"github.com/looplab/fsm"
	"go.uber.org/atomic"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/rpc/scheduler"
)

const (
	// Download tiny file timeout
	downloadTinyFileContextTimeout = 2 * time.Minute
)

const (
	// Peer has been created but did not start running
	PeerStatePending = "Pending"

	// Peer successfully registered as tiny scope size
	PeerStateReceivedTiny = "ReceivedTiny"

	// Peer successfully registered as small scope size
	PeerStateReceivedSmall = "ReceivedSmall"

	// Peer successfully registered as normal scope size
	PeerStateReceivedNormal = "ReceivedNormal"

	// Peer is downloading resources from peer
	PeerStateRunning = "Running"

	// Peer is downloading resources from back-to-source
	PeerStateBackToSource = "BackToSource"

	// Peer has been downloaded successfully
	PeerStateSucceeded = "Succeeded"

	// Peer has been downloaded failed
	PeerStateFailed = "Failed"

	// Peer has been left
	PeerStateLeave = "Leave"
)

const (
	// Peer is registered as tiny scope size
	PeerEventRegisterTiny = "RegisterTiny"

	// Peer is registered as small scope size
	PeerEventRegisterSmall = "RegisterSmall"

	// Peer is registered as normal scope size
	PeerEventRegisterNormal = "RegisterNormal"

	// Peer is downloading
	PeerEventDownload = "Download"

	// Peer is downloading from back-to-source
	PeerEventDownloadFromBackToSource = "DownloadFromBackToSource"

	// Peer downloaded successfully
	PeerEventDownloadSucceeded = "DownloadSucceeded"

	// Peer downloaded failed
	PeerEventDownloadFailed = "DownloadFailed"

	// Peer back to initial pending state
	PeerEventRestart = "Restart"

	// Peer leaves
	PeerEventLeave = "Leave"
)

type Peer struct {
	// ID is peer id
	ID string

	// Pieces is piece bitset
	Pieces *bitset.BitSet

	// pieceCosts is piece downloaded time
	pieceCosts []int64

	// Stream is grpc stream instance
	Stream *atomic.Value

	// Task state machine
	FSM *fsm.FSM

	// Task is peer task
	Task *Task

	// Host is peer host
	Host *Host

	// Parent is peer parent
	Parent *atomic.Value

	// Children is peer children
	Children *sync.Map

	// CreateAt is peer create time
	CreateAt *atomic.Time

	// UpdateAt is peer update time
	UpdateAt *atomic.Time

	// Peer mutex
	mu *sync.RWMutex

	// Peer log
	Log *logger.SugaredLoggerOnWith
}

// New Peer instance
func NewPeer(id string, task *Task, host *Host) *Peer {
	p := &Peer{
		ID:         id,
		Pieces:     &bitset.BitSet{},
		pieceCosts: []int64{},
		Stream:     &atomic.Value{},
		Task:       task,
		Host:       host,
		Parent:     &atomic.Value{},
		Children:   &sync.Map{},
		CreateAt:   atomic.NewTime(time.Now()),
		UpdateAt:   atomic.NewTime(time.Now()),
		mu:         &sync.RWMutex{},
		Log:        logger.WithTaskAndPeerID(task.ID, id),
	}

	// Initialize state machine
	p.FSM = fsm.NewFSM(
		PeerStatePending,
		fsm.Events{
			{Name: PeerEventRegisterTiny, Src: []string{PeerStatePending}, Dst: PeerStateReceivedTiny},
			{Name: PeerEventRegisterSmall, Src: []string{PeerStatePending}, Dst: PeerStateReceivedSmall},
			{Name: PeerEventRegisterNormal, Src: []string{PeerStatePending}, Dst: PeerStateReceivedNormal},
			{Name: PeerEventDownload, Src: []string{PeerStateReceivedTiny, PeerStateReceivedSmall, PeerStateReceivedNormal}, Dst: PeerStateRunning},
			{Name: PeerEventDownloadFromBackToSource, Src: []string{PeerStateReceivedTiny, PeerStateReceivedSmall, PeerStateReceivedNormal, PeerStateRunning}, Dst: PeerStateBackToSource},
			{Name: PeerEventDownloadSucceeded, Src: []string{PeerStateRunning, PeerStateBackToSource}, Dst: PeerStateSucceeded},
			{Name: PeerEventDownloadFailed, Src: []string{
				PeerStatePending, PeerStateReceivedTiny, PeerStateReceivedSmall, PeerStateReceivedNormal,
				PeerStateRunning, PeerStateBackToSource, PeerStateSucceeded,
			}, Dst: PeerStateFailed},
			{Name: PeerEventRestart, Src: []string{PeerStateSucceeded}, Dst: PeerStatePending},
			{Name: PeerEventLeave, Src: []string{PeerStateFailed, PeerStateSucceeded}, Dst: PeerEventLeave},
		},
		fsm.Callbacks{
			PeerEventRegisterTiny: func(e *fsm.Event) {
				p.UpdateAt.Store(time.Now())
				p.Log.Infof("peer state is %s", e.FSM.Current())
			},
			PeerEventRegisterSmall: func(e *fsm.Event) {
				p.UpdateAt.Store(time.Now())
				p.Log.Infof("peer state is %s", e.FSM.Current())
			},
			PeerEventRegisterNormal: func(e *fsm.Event) {
				p.UpdateAt.Store(time.Now())
				p.Log.Infof("peer state is %s", e.FSM.Current())
			},
			PeerEventDownload: func(e *fsm.Event) {
				p.UpdateAt.Store(time.Now())
				p.Log.Infof("peer state is %s", e.FSM.Current())
			},
			PeerEventDownloadFromBackToSource: func(e *fsm.Event) {
				p.Task.BackToSourcePeers.Add(p)
				p.UpdateAt.Store(time.Now())
				p.Log.Infof("peer state is %s", e.FSM.Current())
			},
			PeerEventDownloadSucceeded: func(e *fsm.Event) {
				if e.Src == PeerStateBackToSource {
					p.Task.BackToSourcePeers.Delete(p)
				}

				p.DeleteParent()
				p.UpdateAt.Store(time.Now())
				p.Log.Infof("peer state is %s", e.FSM.Current())
			},
			PeerEventDownloadFailed: func(e *fsm.Event) {
				if e.Src == PeerStateBackToSource {
					p.Task.BackToSourcePeers.Delete(p)
				}

				p.UpdateAt.Store(time.Now())
				p.Log.Infof("peer state is %s", e.FSM.Current())
			},
			PeerEventRestart: func(e *fsm.Event) {
				p.UpdateAt.Store(time.Now())
				p.Log.Infof("peer state is %s", e.FSM.Current())
			},
			PeerEventLeave: func(e *fsm.Event) {
				p.Log.Infof("peer state is %s", e.FSM.Current())
			},
		},
	)

	return p
}

// LoadChild return peer child for a key
func (p *Peer) LoadChild(key string) (*Peer, bool) {
	rawChild, ok := p.Children.Load(key)
	if !ok {
		return nil, false
	}

	return rawChild.(*Peer), ok
}

// StoreChild set peer child
func (p *Peer) StoreChild(child *Peer) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.Children.Store(child.ID, child)
	child.Parent.Store(p)
}

// DeleteChild deletes peer child for a key
func (p *Peer) DeleteChild(key string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	child, ok := p.LoadChild(key)
	if !ok {
		return
	}

	p.Children.Delete(child.ID)
	child.Parent = &atomic.Value{}
}

// LenChildren return length of children sync map
func (p *Peer) LenChildren() int {
	var len int
	p.Children.Range(func(_, _ interface{}) bool {
		len++
		return true
	})

	return len
}

// LoadParent return peer parent
func (p *Peer) LoadParent() (*Peer, bool) {
	rawParent := p.Parent.Load()
	if rawParent == nil {
		return nil, false
	}

	return rawParent.(*Peer), true
}

// StoreParent set peer parent
func (p *Peer) StoreParent(parent *Peer) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.Parent.Store(parent)
	parent.Children.Store(p.ID, p)
}

// DeleteParent deletes peer parent
func (p *Peer) DeleteParent() {
	p.mu.Lock()
	defer p.mu.Unlock()

	parent, ok := p.LoadParent()
	if !ok {
		return
	}

	p.Parent = &atomic.Value{}
	parent.Children.Delete(p.ID)
}

// ReplaceParent replaces peer parent
func (p *Peer) ReplaceParent(parent *Peer) {
	p.DeleteParent()
	p.StoreParent(parent)
}

// TreeTotalNodeCount represents tree's total node count
func (p *Peer) TreeTotalNodeCount() int {
	count := 1
	p.Children.Range(func(_, value interface{}) bool {
		node, ok := value.(*Peer)
		if !ok {
			return true
		}

		count += node.TreeTotalNodeCount()
		return true
	})

	return count
}

// IsDescendant determines whether it is ancestor of peer
func (p *Peer) IsDescendant(ancestor *Peer) bool {
	return isDescendant(ancestor, p)
}

// IsAncestor determines whether it is descendant of peer
func (p *Peer) IsAncestor(descendant *Peer) bool {
	return isDescendant(p, descendant)
}

// isDescendant determines whether it is ancestor of peer
func isDescendant(ancestor, descendant *Peer) bool {
	node := descendant
	for node != nil {
		parent, ok := node.LoadParent()
		if !ok {
			return false
		}
		if parent.ID == ancestor.ID {
			return true
		}
		node = parent
	}

	return false
}

// AppendPieceCost append piece cost to costs slice
func (p *Peer) AppendPieceCost(cost int64) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.pieceCosts = append(p.pieceCosts, cost)
}

// PieceCosts return piece costs slice
func (p *Peer) PieceCosts() []int64 {
	p.mu.RLock()
	defer p.mu.RUnlock()

	return p.pieceCosts
}

// LoadStream return grpc stream
func (p *Peer) LoadStream() (scheduler.Scheduler_ReportPieceResultServer, bool) {
	rawStream := p.Stream.Load()
	if rawStream == nil {
		return nil, false
	}

	return rawStream.(scheduler.Scheduler_ReportPieceResultServer), true
}

// StoreStream set grpc stream
func (p *Peer) StoreStream(stream scheduler.Scheduler_ReportPieceResultServer) {
	p.Stream.Store(stream)
}

// DeleteStream deletes grpc stream
func (p *Peer) DeleteStream() {
	p.Stream = &atomic.Value{}
}

// DownloadTinyFile downloads tiny file from peer
func (p *Peer) DownloadTinyFile() ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), downloadTinyFileContextTimeout)
	defer cancel()

	// Download url: http://${host}:${port}/download/${taskIndex}/${taskID}?peerId=${peerID}
	targetURL := url.URL{
		Scheme:   "http",
		Host:     fmt.Sprintf("%s:%d", p.Host.IP, p.Host.DownloadPort),
		Path:     fmt.Sprintf("download/%s/%s", p.Task.ID[:3], p.Task.ID),
		RawQuery: fmt.Sprintf("peerId=%s", p.ID),
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, targetURL.String(), nil)
	if err != nil {
		return []byte{}, err
	}
	req.Header.Set(headers.Range, fmt.Sprintf("bytes=%d-%d", 0, p.Task.ContentLength.Load()-1))
	p.Log.Infof("download tiny file %s, header is : %#v", targetURL.String(), req.Header)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	// The HTTP 206 Partial Content success status response code indicates that
	// the request has succeeded and the body contains the requested ranges of data, as described in the Range header of the request.
	// Refer to https://developer.mozilla.org/en-US/docs/Web/HTTP/Status/206
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusBadRequest {
		return []byte{}, fmt.Errorf("%v: %v", targetURL.String(), resp.Status)
	}

	return io.ReadAll(resp.Body)
}
