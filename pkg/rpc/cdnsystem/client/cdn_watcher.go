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

package client

import (
	"context"
	"reflect"
	"sync"
	"time"

	logger "d7y.io/dragonfly/v2/pkg/dflog"
	"d7y.io/dragonfly/v2/pkg/dynconfig"
	"d7y.io/dragonfly/v2/pkg/rpc/manager"
	mgClient "d7y.io/dragonfly/v2/pkg/rpc/manager/client"
)

type CdnListWatcher struct {
	ctx    context.Context
	dyncfg *dynconfig.Dynconfig
	cancel context.CancelFunc
	wg     sync.WaitGroup
	adders []*manager.ServerInfo
}

func newCdnListWatcher(cfgServer mgClient.ManagerClient, expireTime time.Duration) (*CdnListWatcher, error) {
	d, err := dynconfig.New(dynconfig.ManagerSourceType, expireTime, []dynconfig.Option{dynconfig.WithManagerClient(NewCdnHostsMgr(cfgServer))}...)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	watcher := &CdnListWatcher{
		ctx:    ctx,
		cancel: cancel,
		dyncfg: d,
	}
	return watcher, nil
}

func (watcher *CdnListWatcher) GetClusterCDNs() ([]*manager.ServerInfo, error) {
	var cdnHosts []*manager.ServerInfo
	err := watcher.dyncfg.Unmarshal(&cdnHosts)
	if err != nil {
		return nil, err
	}
	logger.Debugf("cdn list: %s", cdnHosts)
	return cdnHosts, nil
}

func (watcher *CdnListWatcher) Close() {
	watcher.cancel()
	watcher.wg.Wait()
}

func (watcher *CdnListWatcher) Watch() chan []*manager.ServerInfo {
	out := make(chan []*manager.ServerInfo, 10)
	watcher.wg.Add(1)
	go func() {
		defer func() {
			close(out)
			watcher.wg.Done()
		}()
		adders, err := watcher.GetClusterCDNs()
		watcher.adders = adders
		if err != nil {
			logger.Error("failed to get cluster cdn list: %v", err)
		}
		out <- watcher.cloneAddresses(watcher.adders)

		timer := time.NewTicker(5 * time.Second)
		for {
			select {
			case <-timer.C:
				adders, err := watcher.GetClusterCDNs()
				if err != nil {
					logger.Error("failed to get cluster cdn list: %v", err)
				}
				if !isSameAdders(watcher.adders, adders) {
					logger.Debugf("cdn node list have changed, old list %v, new list %v", watcher.adders, adders)
					watcher.adders = adders
					out <- watcher.cloneAddresses(watcher.adders)
				} else {
					logger.Debugf("The list of cdn has not changed")
				}
			case <-watcher.ctx.Done():
				return
			}
		}

	}()
	return out
}

func (watcher *CdnListWatcher) cloneAddresses(in []*manager.ServerInfo) []*manager.ServerInfo {
	out := make([]*manager.ServerInfo, len(in))
	for i := 0; i < len(in); i++ {
		out[i] = in[i]
	}
	return out
}

func isSameAdders(oldAdders, newAdders []*manager.ServerInfo) bool {
	if len(oldAdders) != len(newAdders) {
		return false
	}
	for _, adder1 := range oldAdders {
		found := false
		for _, adder2 := range newAdders {
			if reflect.DeepEqual(adder1, adder2) {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}
