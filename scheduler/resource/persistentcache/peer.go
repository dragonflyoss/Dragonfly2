/*
 *     Copyright 2024 The Dragonfly Authors
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

package persistentcache

import (
	"context"
	"time"

	"github.com/bits-and-blooms/bitset"
	"github.com/looplab/fsm"

	logger "d7y.io/dragonfly/v2/internal/dflog"
)

const (
	// Peer has been created but did not start running.
	PeerStatePending = "Pending"

	// Peer is uploading resources for p2p cluster.
	PeerStateUploading = "Uploading"

	// Peer successfully registered and perpared to download.
	PeerStateReceived = "Received"

	// Peer is downloading resources from peer.
	PeerStateRunning = "Running"

	// Peer has been downloaded successfully.
	PeerStateSucceeded = "Succeeded"

	// Peer has been downloaded failed.
	PeerStateFailed = "Failed"
)

const (
	// Peer is uploding.
	PeerEventUpload = "Uploda"

	// Peer is registered and perpared to download.
	PeerEventRegister = "Register"

	// Peer is downloading.
	PeerEventDownload = "Download"

	// Peer downloaded or uploaded successfully.
	PeerEventSucceeded = "Succeeded"

	// Peer downloaded or uploaded failed.
	PeerEventFailed = "Failed"
)

// Peer contains content for persistent cache peer.
type Peer struct {
	// ID is persistent cache peer id.
	ID string

	// Persistent is whether the peer is persistent.
	Persistent bool

	// Pieces is finished pieces bitset.
	FinishedPieces *bitset.BitSet

	// Persistent cache peer state machine.
	FSM *fsm.FSM

	// Task is persistent cache task.
	Task *Task

	// Host is the peer host.
	Host *Host

	// BlockParents is bad parents ids.
	BlockParents []string

	// Cost is the cost of downloading.
	Cost time.Duration

	// CreatedAt is persistent cache peer create time.
	CreatedAt time.Time

	// UpdatedAt is persistent cache peer update time.
	UpdatedAt time.Time

	// Persistent cache peer log.
	Log *logger.SugaredLoggerOnWith
}

// New persistent cache peer instance.
func NewPeer(id, state string, persistent bool, finishedPieces *bitset.BitSet, blockParents []string, task *Task, host *Host,
	cost time.Duration, createdAt, updatedAt time.Time, log *logger.SugaredLoggerOnWith) *Peer {
	p := &Peer{
		ID:             id,
		FinishedPieces: finishedPieces,
		Task:           task,
		Host:           host,
		BlockParents:   blockParents,
		Cost:           cost,
		CreatedAt:      createdAt,
		UpdatedAt:      updatedAt,
		Log:            logger.WithPeer(host.ID, task.ID, id),
	}

	// Initialize state machine.
	p.FSM = fsm.NewFSM(
		PeerStatePending,
		fsm.Events{
			fsm.EventDesc{Name: PeerEventUpload, Src: []string{PeerStatePending, PeerStateFailed}, Dst: PeerStateUploading},
			fsm.EventDesc{Name: PeerEventRegister, Src: []string{PeerStatePending, PeerStateFailed}, Dst: PeerStateReceived},
			fsm.EventDesc{Name: PeerEventDownload, Src: []string{PeerStateReceived}, Dst: PeerStateRunning},
			fsm.EventDesc{Name: PeerEventSucceeded, Src: []string{PeerStateUploading, PeerStateRunning}, Dst: PeerStateSucceeded},
			fsm.EventDesc{Name: PeerEventFailed, Src: []string{PeerStateUploading, PeerStateRunning}, Dst: PeerStateFailed},
		},
		fsm.Callbacks{
			PeerEventUpload: func(ctx context.Context, e *fsm.Event) {
				p.Log.Infof("peer state is %s", e.FSM.Current())
			},
			PeerEventRegister: func(ctx context.Context, e *fsm.Event) {
				p.Log.Infof("peer state is %s", e.FSM.Current())
			},
			PeerEventDownload: func(ctx context.Context, e *fsm.Event) {
				p.Log.Infof("peer state is %s", e.FSM.Current())
			},
			PeerEventSucceeded: func(ctx context.Context, e *fsm.Event) {
				p.Log.Infof("peer state is %s", e.FSM.Current())
			},
			PeerEventFailed: func(ctx context.Context, e *fsm.Event) {
				p.Log.Infof("peer state is %s", e.FSM.Current())
			},
		},
	)
	p.FSM.SetState(state)

	return p
}
