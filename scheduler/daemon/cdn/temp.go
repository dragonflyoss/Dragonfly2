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
	"fmt"

	"d7y.io/dragonfly/v2/pkg/basic/dfnet"
	"d7y.io/dragonfly/v2/scheduler/config"
)

// cdnHostsToNetAddrs coverts manager.CdnHosts to []dfnet.NetAddr.
func cdnHostsToNetAddrs(hosts []*config.CDN) []dfnet.NetAddr {
	var netAddrs []dfnet.NetAddr
	for i := range hosts {
		netAddrs = append(netAddrs, dfnet.NetAddr{
			Type: dfnet.TCP,
			Addr: fmt.Sprintf("%s:%d", hosts[i].IP, hosts[i].Port),
		})
	}
	return netAddrs
}

func (cm *manager) OnNotify(c *config.DynconfigData) {
	netAddrs := cdnHostsToNetAddrs(c.CDNs)
	if reflect.DeepEqual(netAddrs, c.CDNs) {
		return
	}
	cm.lock.Lock()
	defer cm.lock.Unlock()
	// Sync CDNManager client netAddrs
	cm.cdnAddrs = netAddrs
	cm.client.UpdateState(netAddrs)
}
