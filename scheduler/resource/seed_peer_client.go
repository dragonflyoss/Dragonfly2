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

	"github.com/hashicorp/go-multierror"
	"google.golang.org/grpc"

	managerv2 "d7y.io/api/v2/pkg/apis/manager/v2"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/dfnet"
	"d7y.io/dragonfly/v2/pkg/idgen"
	cdnsystemclient "d7y.io/dragonfly/v2/pkg/rpc/cdnsystem/client"
	dfdaemonclient "d7y.io/dragonfly/v2/pkg/rpc/dfdaemon/client"
	"d7y.io/dragonfly/v2/pkg/types"
	"d7y.io/dragonfly/v2/scheduler/config"
)

// SeedPeerClient is the interface used for client of seed peer.
type SeedPeerClient interface {
	// Addrs returns the addresses of seed peers.
	Addrs() []string

	// Client is cdnsystem grpc client interface.
	cdnsystemclient.Client

	// V2 is dfdaemon v2 grpc client interface.
	dfdaemonclient.V2

	// Observer is dynconfig observer interface.
	config.Observer
}

// seedPeerClient contains content for client of seed peer.
type seedPeerClient struct {
	// Client is cdnsystem grpc client interface.
	cdnsystemclient.Client

	// V2 is dfdaemon v2 grpc client interface.
	dfdaemonclient.V2

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
	cdnsystemClient, err := cdnsystemclient.GetClient(context.Background(), dynconfig, opts...)
	if err != nil {
		return nil, err
	}

	fmt.Println("cdnsystemClient", cdnsystemClient)

	// Initialize dfdaemon v2 grpc client.
	dfdaemonClient, err := dfdaemonclient.GetV2(context.Background(), dynconfig, opts...)
	if err != nil {
		return nil, err
	}

	fmt.Println("dfdaemonClient", dfdaemonClient)

	sc := &seedPeerClient{
		hostManager: hostManager,
		Client:      cdnsystemClient,
		V2:          dfdaemonClient,
		dynconfig:   dynconfig,
		data:        config,
	}

	// Initialize seed peers for host manager.
	sc.updateSeedPeersForHostManager(config.Scheduler.SeedPeers)

	dynconfig.Register(sc)
	return sc, nil
}

// Close closes the seed peer client.
func (sc *seedPeerClient) Close() error {
	var errs error
	if err := sc.Client.Close(); err != nil {
		errs = multierror.Append(errs, err)
	}

	if err := sc.V2.Close(); err != nil {
		errs = multierror.Append(errs, err)
	}

	return errs
}

// Addrs returns the addresses of seed peers.
func (sc *seedPeerClient) Addrs() []string {
	var addrs []string
	for _, seedPeer := range sc.data.Scheduler.SeedPeers {
		addrs = append(addrs, fmt.Sprintf("%s:%d", seedPeer.Ip, seedPeer.Port))
	}

	return addrs
}

// Dynamic config notify function.
func (sc *seedPeerClient) OnNotify(data *config.DynconfigData) {
	if reflect.DeepEqual(sc.data, data) {
		return
	}

	// Update seed peers for host manager.
	sc.updateSeedPeersForHostManager(data.Scheduler.SeedPeers)

	// Update dynamic data.
	sc.data = data

	// Update grpc seed peer addresses.
	logger.Infof("addresses have been updated: %#v", seedPeersToNetAddrs(data.Scheduler.SeedPeers))
}

// updateSeedPeersForHostManager updates seed peers for host manager.
func (sc *seedPeerClient) updateSeedPeersForHostManager(seedPeers []*managerv2.SeedPeer) {
	for _, seedPeer := range seedPeers {
		var concurrentUploadLimit int32
		if config, err := config.GetSeedPeerClusterConfigBySeedPeer(seedPeer); err == nil {
			concurrentUploadLimit = int32(config.LoadLimit)
		}

		id := idgen.HostIDV2(seedPeer.Ip, seedPeer.Hostname)
		seedPeerHost, loaded := sc.hostManager.Load(id)
		if !loaded {
			options := []HostOption{WithNetwork(Network{
				Location: seedPeer.GetLocation(),
				IDC:      seedPeer.GetIdc(),
			})}
			if concurrentUploadLimit > 0 {
				options = append(options, WithConcurrentUploadLimit(concurrentUploadLimit))
			}

			host := NewHost(
				id, seedPeer.Ip, seedPeer.Hostname,
				seedPeer.Port, seedPeer.DownloadPort, types.HostTypeSuperSeed,
				options...,
			)

			sc.hostManager.Store(host)
			continue
		}

		seedPeerHost.Type = types.HostTypeSuperSeed
		seedPeerHost.Port = seedPeer.Port
		seedPeerHost.DownloadPort = seedPeer.DownloadPort
		seedPeerHost.Network.Location = seedPeer.GetLocation()
		seedPeerHost.Network.IDC = seedPeer.GetIdc()

		if concurrentUploadLimit > 0 {
			seedPeerHost.ConcurrentUploadLimit.Store(concurrentUploadLimit)
		}
	}

	return
}

// seedPeersToNetAddrs coverts []*config.SeedPeer to []dfnet.NetAddr.
func seedPeersToNetAddrs(seedPeers []*managerv2.SeedPeer) []dfnet.NetAddr {
	netAddrs := make([]dfnet.NetAddr, 0, len(seedPeers))
	for _, seedPeer := range seedPeers {
		netAddrs = append(netAddrs, dfnet.NetAddr{
			Type: dfnet.TCP,
			Addr: fmt.Sprintf("%s:%d", seedPeer.Ip, seedPeer.Port),
		})
	}

	return netAddrs
}
