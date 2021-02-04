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

package mgr

import (
	"github.com/dragonflyoss/Dragonfly/v2/pkg/basic/dfnet"
	cdnclient "github.com/dragonflyoss/Dragonfly/v2/pkg/rpc/cdnsystem/client"
	"github.com/serialx/hashring"
	"sync"
)

var (
	// read config
	nodes     = []string{"ddd"}
	ring      = hashring.New(nodes)
	clientMgr = &CdnClientManager{
		HashRing:  ring,
		ClientMap: nil,
	}
)
// task <-> Client
// node <-> Client
// todo task 的回收怎么搞
type CdnClientManager struct {
	sync.RWMutex
	*hashring.HashRing
	ClientMap map[string]cdnclient.SeederClient
	TaskMap   map[string]cdnclient.SeederClient
}

func GetClient(filterUrl string) (cdnclient.SeederClient, bool) {
	// 根据hash获取某个cdn节点
	nodes, ok := clientMgr.GetNodes(filterUrl, ring.Size())
	if !ok {
		return nil, ok
	}
	// 如果当前到cdn的连接不存在，则创建链接，并与node节点关联
	value, ok := clientMgr.ClientMap[nodes[0]]
	if !ok {
		// todo 如果创建失败，怎么搞
		client, err := cdnclient.CreateClient([]dfnet.NetAddr{{
			Addr: nodes[0],
		}})
		if err != nil {
			// foreach nodes
			for _, node := range nodes {
				client, err = cdnclient.CreateClient([]dfnet.NetAddr{{
					Addr: node,
				}})
				if err != nil {
					continue
				}
			}
		}
		clientMgr.ClientMap[nodes[0]] = client
	}
	return value, true
}