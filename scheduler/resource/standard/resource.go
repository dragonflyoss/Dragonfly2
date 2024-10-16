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

//go:generate mockgen -destination resource_mock.go -source resource.go -package standard

package standard

import (
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"d7y.io/dragonfly/v2/pkg/gc"
	"d7y.io/dragonfly/v2/scheduler/config"
)

// Resource is the interface used for resource.
type Resource interface {
	// SeedPeer interface.
	SeedPeer() SeedPeer

	// Host manager interface.
	HostManager() HostManager

	// Peer manager interface.
	PeerManager() PeerManager

	// Task manager interface.
	TaskManager() TaskManager

	// Stop resource service.
	Stop() error
}

// resource contains content for resource.
type resource struct {
	// seedPeer interface.
	seedPeer SeedPeer

	// Host manager interface.
	hostManager HostManager

	// Peer manager interface.
	peerManager PeerManager

	// Task manager interface.
	taskManager TaskManager

	// Scheduler config.
	config *config.Config
}

// New returns Resource interface.
func New(cfg *config.Config, gc gc.GC, dynconfig config.DynconfigInterface, transportCredentials credentials.TransportCredentials) (Resource, error) {
	resource := &resource{config: cfg}

	// Initialize host manager interface.
	hostManager, err := newHostManager(&cfg.Scheduler.GC, gc)
	if err != nil {
		return nil, err
	}
	resource.hostManager = hostManager

	// Initialize task manager interface.
	taskManager, err := newTaskManager(&cfg.Scheduler.GC, gc)
	if err != nil {
		return nil, err
	}
	resource.taskManager = taskManager

	// Initialize peer manager interface.
	peerManager, err := newPeerManager(&cfg.Scheduler.GC, gc)
	if err != nil {
		return nil, err
	}
	resource.peerManager = peerManager

	// Initialize seed peer interface.
	if cfg.SeedPeer.Enable {
		dialOptions := []grpc.DialOption{grpc.WithStatsHandler(otelgrpc.NewClientHandler()), grpc.WithTransportCredentials(transportCredentials)}
		client, err := newSeedPeerClient(dynconfig, hostManager, dialOptions...)
		if err != nil {
			return nil, err
		}

		resource.seedPeer = newSeedPeer(client, peerManager, hostManager)
	}

	return resource, nil
}

// SeedPeer interface.
func (r *resource) SeedPeer() SeedPeer {
	return r.seedPeer
}

// Host manager interface.
func (r *resource) HostManager() HostManager {
	return r.hostManager
}

// Peer manager interface.
func (r *resource) PeerManager() PeerManager {
	return r.peerManager
}

// Task manager interface.
func (r *resource) TaskManager() TaskManager {
	return r.taskManager
}

// Stop resource service.
func (r *resource) Stop() error {
	if r.config.SeedPeer.Enable {
		return r.seedPeer.Stop()
	}

	return nil
}
