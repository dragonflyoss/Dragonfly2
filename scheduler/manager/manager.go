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

package manager

import (
	"google.golang.org/grpc"

	"d7y.io/dragonfly/v2/pkg/gc"
	"d7y.io/dragonfly/v2/scheduler/config"
)

type Manager struct {
	// CDN manager
	CDN CDN

	// Host manager
	Host Host

	// Peer manager
	Peer Peer

	// Task manager
	Task Task
}

func New(cfg *config.Config, gc gc.GC, dynConfig config.DynconfigInterface, opts []grpc.DialOption) (*Manager, error) {
	// Initialize host manager
	host := newHost()

	// Initialize peer manager
	peer, err := newPeer(cfg.Scheduler.GC, gc)
	if err != nil {
		return nil, err
	}

	// Initialize task manager
	task, err := newTask(cfg.Scheduler.GC, gc)
	if err != nil {
		return nil, err
	}

	// Initialize cdn manager
	cdn, err := newCDN(peer, host, dynConfig, opts)
	if err != nil {
		return nil, err
	}

	return &Manager{
		CDN:  cdn,
		Host: host,
		Peer: peer,
		Task: task,
	}, nil
}
