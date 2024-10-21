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
	// Peer is registered and perpared to download.
	PeerEventRegister = "Register"

	// Peer is downloading.
	PeerEventDownload = "Download"

	// Peer downloaded successfully.
	PeerEventDownloadSucceeded = "DownloadSucceeded"

	// Peer downloaded failed.
	PeerEventDownloadFailed = "DownloadFailed"
)

// Peer contains content for persistent cache peer.
type Peer struct {
	// ID is persistent cache peer id.
	ID string

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
func NewPeer(id, state string, finishedPieces *bitset.BitSet, blockParents []string, task *Task, host *Host,
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
			{Name: PeerEventRegister, Src: []string{PeerStatePending}, Dst: PeerStateReceived},
			{Name: PeerEventDownload, Src: []string{PeerStateReceived}, Dst: PeerStateRunning},
			{Name: PeerEventDownloadSucceeded, Src: []string{PeerStateRunning}, Dst: PeerStateSucceeded},
			{Name: PeerEventDownloadFailed, Src: []string{PeerStateRunning}, Dst: PeerStateFailed},
		},
		fsm.Callbacks{
			PeerEventRegister: func(ctx context.Context, e *fsm.Event) {
				p.Log.Infof("peer state is %s", e.FSM.Current())
			},
			PeerEventDownload: func(ctx context.Context, e *fsm.Event) {
				p.Log.Infof("peer state is %s", e.FSM.Current())
			},
			PeerEventDownloadSucceeded: func(ctx context.Context, e *fsm.Event) {
				p.Log.Infof("peer state is %s", e.FSM.Current())
			},
			PeerEventDownloadFailed: func(ctx context.Context, e *fsm.Event) {
				p.Log.Infof("peer state is %s", e.FSM.Current())
			},
		},
	)
	p.FSM.SetState(state)

	return p
}
