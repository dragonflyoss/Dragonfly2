/*
 *     Copyright 2022 The Dragonfly Authors
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

//go:generate mockgen -destination seed_peer_client_mock.go -source seed_peer_client.go -package resource

package resource

import (
	"context"
	"fmt"
	reflect "reflect"

	"google.golang.org/grpc"

	managerv1 "d7y.io/api/pkg/apis/manager/v1"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/dfnet"
	"d7y.io/dragonfly/v2/pkg/idgen"
	"d7y.io/dragonfly/v2/pkg/rpc/cdnsystem/client"
	"d7y.io/dragonfly/v2/pkg/types"
	"d7y.io/dragonfly/v2/scheduler/config"
)

type SeedPeerClient interface {
	// client is seed peer grpc client interface.
	client.Client

	// Observer is dynconfig observer interface.
	config.Observer
}

type seedPeerClient struct {
	// client is sedd peer grpc client instance.
	client.Client

	// hostManager is host manager.
	hostManager HostManager

	// dynconfig is dynconfig interface.
	dynconfig config.DynconfigInterface

	// data is dynconfig data.
	data *config.DynconfigData
}

// New seed peer client interface.
func newSeedPeerClient(dynconfig config.DynconfigInterface, hostManager HostManager, opts ...grpc.DialOption) (SeedPeerClient, error) {
	config, err := dynconfig.Get()
	if err != nil {
		return nil, err
	}
	logger.Infof("initialize seed peer addresses: %#v", seedPeersToNetAddrs(config.Scheduler.SeedPeers))

	// Initialize seed peer grpc client.
	client, err := client.GetClient(context.Background(), dynconfig, opts...)
	if err != nil {
		return nil, err
	}

	sc := &seedPeerClient{
		hostManager: hostManager,
		Client:      client,
		dynconfig:   dynconfig,
		data:        config,
	}

	// Initialize seed peers for host manager.
	sc.updateSeedPeersForHostManager(config.Scheduler.SeedPeers)

	dynconfig.Register(sc)
	return sc, nil
}

// Dynamic config notify function.
func (sc *seedPeerClient) OnNotify(data *config.DynconfigData) {
	if reflect.DeepEqual(sc.data, data) {
		return
	}

	// If only the ip of the seed peer is changed,
	// the seed peer needs to be cleared.
	diffSeedPeers := diffSeedPeers(sc.data.Scheduler.SeedPeers, data.Scheduler.SeedPeers)
	for _, seedPeer := range diffSeedPeers {
		id := idgen.HostID(seedPeer.HostName, seedPeer.Port)
		logger.Infof("host %s has been reclaimed, because of seed peer ip is changed", id)
		if host, loaded := sc.hostManager.Load(id); loaded {
			host.LeavePeers()
			sc.hostManager.Delete(id)
		}
	}

	// Update seed peers for host manager.
	sc.updateSeedPeersForHostManager(data.Scheduler.SeedPeers)

	// Update dynamic data.
	sc.data = data

	// Update grpc seed peer addresses.
	logger.Infof("addresses have been updated: %#v", seedPeersToNetAddrs(data.Scheduler.SeedPeers))
}

// updateSeedPeersForHostManager updates seed peers for host manager.
func (sc *seedPeerClient) updateSeedPeersForHostManager(seedPeers []*managerv1.SeedPeer) {
	for _, seedPeer := range seedPeers {
		var concurrentUploadLimit int32
		if config, err := config.GetSeedPeerClusterConfigBySeedPeer(seedPeer); err == nil {
			concurrentUploadLimit = int32(config.LoadLimit)
		}

		id := idgen.HostID(seedPeer.HostName, seedPeer.Port)
		seedPeerHost, loaded := sc.hostManager.Load(id)
		if !loaded {
			options := []HostOption{WithNetwork(Network{
				Location: seedPeer.Location,
				IDC:      seedPeer.Idc,
			})}
			if concurrentUploadLimit > 0 {
				options = append(options, WithConcurrentUploadLimit(concurrentUploadLimit))
			}

			host := NewHost(
				id, seedPeer.Ip, seedPeer.HostName,
				seedPeer.Port, seedPeer.DownloadPort, types.HostTypeSuperSeed,
				options...,
			)

			sc.hostManager.Store(host)
			continue
		}

		seedPeerHost.Type = types.HostTypeSuperSeed
		seedPeerHost.IP = seedPeer.Ip
		seedPeerHost.DownloadPort = seedPeer.DownloadPort
		seedPeerHost.Network.Location = seedPeer.Location
		seedPeerHost.Network.IDC = seedPeer.Idc

		if concurrentUploadLimit > 0 {
			seedPeerHost.ConcurrentUploadLimit.Store(concurrentUploadLimit)
		}
	}

	return
}

// seedPeersToNetAddrs coverts []*config.SeedPeer to []dfnet.NetAddr.
func seedPeersToNetAddrs(seedPeers []*managerv1.SeedPeer) []dfnet.NetAddr {
	netAddrs := make([]dfnet.NetAddr, 0, len(seedPeers))
	for _, seedPeer := range seedPeers {
		netAddrs = append(netAddrs, dfnet.NetAddr{
			Type: dfnet.TCP,
			Addr: fmt.Sprintf("%s:%d", seedPeer.Ip, seedPeer.Port),
		})
	}

	return netAddrs
}

// diffSeedPeers find out different seed peers.
func diffSeedPeers(sx []*managerv1.SeedPeer, sy []*managerv1.SeedPeer) []*managerv1.SeedPeer {
	// Get seedPeers with the same HostID but different IP.
	var diff []*managerv1.SeedPeer
	for _, x := range sx {
		for _, y := range sy {
			if x.HostName != y.HostName {
				continue
			}

			if x.Port != y.Port {
				continue
			}

			if x.Ip == y.Ip {
				continue
			}

			diff = append(diff, x)
		}
	}

	// Get the removed seed peers.
	for _, x := range sx {
		found := false
		for _, y := range sy {
			if idgen.HostID(x.HostName, x.Port) == idgen.HostID(y.HostName, y.Port) {
				found = true
				break
			}
		}

		if !found {
			diff = append(diff, x)
		}
	}

	return diff
}
