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

package cdn

import (
	"context"
	"fmt"
	"reflect"
	"sync"

	"d7y.io/dragonfly/v2/internal/idgen"
	"d7y.io/dragonfly/v2/pkg/basic/dfnet"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	"d7y.io/dragonfly/v2/pkg/rpc/cdnsystem"
	cdnclient "d7y.io/dragonfly/v2/pkg/rpc/cdnsystem/client"
	"d7y.io/dragonfly/v2/scheduler/config"
	"d7y.io/dragonfly/v2/scheduler/supervisor"
	"google.golang.org/grpc"
)

type RefreshableCDNClient interface {
	cdnclient.CdnClient
	config.Observer
	GetCDNHost(hostID string) (*supervisor.PeerHost, bool)
}

type refreshableCDNClient struct {
	mu        sync.RWMutex
	cdnClient cdnclient.CdnClient
	cdnHosts  map[string]*supervisor.PeerHost
}

func (rcc *refreshableCDNClient) UpdateState(addrs []dfnet.NetAddr) {
	rcc.cdnClient.UpdateState(addrs)
}

func (rcc *refreshableCDNClient) ObtainSeeds(ctx context.Context, sr *cdnsystem.SeedRequest, opts ...grpc.CallOption) (*cdnclient.PieceSeedStream, error) {
	return rcc.cdnClient.ObtainSeeds(ctx, sr, opts...)
}

func (rcc *refreshableCDNClient) GetPieceTasks(ctx context.Context, addr dfnet.NetAddr, req *base.PieceTaskRequest, opts ...grpc.CallOption) (*base.PiecePacket, error) {
	return rcc.cdnClient.GetPieceTasks(ctx, addr, req, opts...)
}

func (rcc *refreshableCDNClient) GetCDNHost(hostID string) (*supervisor.PeerHost, bool) {
	rcc.mu.RLock()
	defer rcc.mu.RUnlock()
	if cdnHost, ok := rcc.cdnHosts[hostID]; ok {
		return cdnHost, true
	}
	return nil, false
}

func (rcc *refreshableCDNClient) Close() error {
	return rcc.cdnClient.Close()
}

func NewRefreshableCDNClient(dynConfig config.DynconfigInterface, opts []grpc.DialOption) (RefreshableCDNClient, error) {
	dynConfigData, err := dynConfig.Get()
	if err != nil {
		return nil, err
	}
	cdnHosts, cdnAddrs := cdnHostsToNetAddrs(dynConfigData.CDNs)
	cdnClient, err := cdnclient.GetClientByAddr(cdnAddrs, opts...)
	if err != nil {
		return nil, err
	}
	rcc := &refreshableCDNClient{
		cdnClient: cdnClient,
		cdnHosts:  cdnHosts,
	}
	dynConfig.Register(rcc)
	return rcc, nil
}

func (rcc *refreshableCDNClient) OnNotify(c *config.DynconfigData) {
	rcc.refresh(c.CDNs)
}

func (rcc *refreshableCDNClient) refresh(cdns []*config.CDN) {
	rcc.mu.Lock()
	defer rcc.mu.Unlock()
	cdnHosts, netAddrs := cdnHostsToNetAddrs(cdns)
	if reflect.DeepEqual(rcc.cdnHosts, cdnHosts) {
		return
	}
	rcc.cdnHosts = cdnHosts
	// Sync CDNManager client netAddrs
	rcc.cdnClient.UpdateState(netAddrs)
}

// cdnHostsToNetAddrs coverts manager.CdnHosts to []dfnet.NetAddr.
func cdnHostsToNetAddrs(hosts []*config.CDN) (map[string]*supervisor.PeerHost, []dfnet.NetAddr) {
	cdnHostMap := make(map[string]*supervisor.PeerHost, len(hosts))
	netAddrs := make([]dfnet.NetAddr, 0, len(hosts))
	for _, host := range hosts {
		hostID := idgen.CDNUUID(host.HostName, host.Port)
		cdnHostMap[hostID] = supervisor.NewCDNPeerHost(hostID, host.IP, host.HostName, host.Port, host.DownloadPort, host.SecurityGroup, host.Location,
			host.IDC, host.NetTopology, host.LoadLimit)
		netAddrs = append(netAddrs, dfnet.NetAddr{
			Type: dfnet.TCP,
			Addr: fmt.Sprintf("%s:%d", host.HostName, host.Port),
		})
	}
	return cdnHostMap, netAddrs
}
