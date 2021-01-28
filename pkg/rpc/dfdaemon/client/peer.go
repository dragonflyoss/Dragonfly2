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
	"fmt"
	"github.com/dragonflyoss/Dragonfly2/pkg/basic/dfnet"
	logger "github.com/dragonflyoss/Dragonfly2/pkg/dflog"
	"github.com/dragonflyoss/Dragonfly2/pkg/rpc/base"
	"github.com/dragonflyoss/Dragonfly2/pkg/rpc/base/common"
	cdnclient "github.com/dragonflyoss/Dragonfly2/pkg/rpc/cdnsystem/client"
	"github.com/dragonflyoss/Dragonfly2/pkg/rpc/scheduler"
	"google.golang.org/grpc"
	"reflect"
	"strings"
	"sync"
	"time"
)

var pieceTaskPuller = struct {
	// coordinate clean and create client
	rwMutex sync.RWMutex
	// destAddr -> clientFunc
	clientFuncs sync.Map
	// destAddr -> client
	clients sync.Map
	// destAddr -> time
	access sync.Map
}{}

// clientFunc returns client interface
type clientFunc func() (interface{}, error)

func init() {
	go func() {
		for {
			time.Sleep(5 * time.Second)

			pieceTaskPuller.access.Range(func(key interface{}, value interface{}) bool {
				pieceTaskPuller.rwMutex.Lock()
				defer pieceTaskPuller.rwMutex.Unlock()

				value, ok := pieceTaskPuller.access.Load(key)
				if ok {
					if time.Now().Sub(value.(time.Time))/time.Second >= 10 {
						if v, loaded := pieceTaskPuller.clients.LoadAndDelete(key); loaded {
							err := reflect.ValueOf(v).MethodByName("Close").Call([]reflect.Value{})
							if err != nil && logger.CoreLogger != nil {
								logger.Errorf("close client connected to %v error:%v on piece task puller", key, err)
							}
						}
						pieceTaskPuller.clientFuncs.Delete(key)
						pieceTaskPuller.access.Delete(key)
					}
				}

				time.Sleep(10 * time.Millisecond)

				return true
			})
		}
	}()
}

func GetPieceTasks(destPeer *scheduler.PeerPacket_DestPeer, ctx context.Context, ptr *base.PieceTaskRequest, opts ...grpc.CallOption) (*base.PiecePacket, error) {
	destAddr := fmt.Sprintf("%s:%d", destPeer.Ip, destPeer.RpcPort)
	peerId := destPeer.PeerId
	toCdn := strings.HasSuffix(peerId, common.CdnSuffix)

	var client interface{}
	var err error

	pieceTaskPuller.rwMutex.RLock()
	defer pieceTaskPuller.rwMutex.RUnlock()

	client, ok := pieceTaskPuller.clients.Load(destAddr)
	if !ok {
		m := new(sync.Mutex)
		var f clientFunc = func() (interface{}, error) {
			m.Lock()
			defer m.Unlock()

			if v, ok := pieceTaskPuller.clients.Load(destAddr); ok {
				return v, nil
			}

			if v, err := getClient(destAddr, toCdn); err == nil {
				pieceTaskPuller.clients.Store(destAddr, v)
				return v, nil
			} else {
				return nil, err
			}
		}

		xf, _ := pieceTaskPuller.clientFuncs.LoadOrStore(destAddr, f)

		if client, err = xf.(clientFunc)(); err != nil {
			return nil, err
		}
	}

	pieceTaskPuller.access.Store(destAddr, time.Now())

	if toCdn {
		return client.(cdnclient.SeederClient).GetPieceTasks(ctx, ptr, opts...)
	} else {
		return client.(DaemonClient).GetPieceTasks(ctx, ptr, opts...)
	}
}

func getClient(destAddr string, toCdn bool) (interface{}, error) {
	netAddr := dfnet.NetAddr{
		Type: dfnet.TCP,
		Addr: destAddr,
	}

	if toCdn {
		return cdnclient.CreateClient([]dfnet.NetAddr{netAddr})
	} else {
		return CreateClient([]dfnet.NetAddr{netAddr})
	}
}
