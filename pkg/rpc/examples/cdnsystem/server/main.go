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

package main

import _ "github.com/dragonflyoss/Dragonfly2/pkg/rpc/cdnsystem/server"
import (
	"context"
	"fmt"
	"github.com/dragonflyoss/Dragonfly2/pkg/basic"
	"github.com/dragonflyoss/Dragonfly2/pkg/rpc"
	"github.com/dragonflyoss/Dragonfly2/pkg/rpc/base"
	"github.com/dragonflyoss/Dragonfly2/pkg/rpc/cdnsystem"
	"github.com/dragonflyoss/Dragonfly2/pkg/safe"
	"time"
)

type helloSeeder struct {
}

func (hs *helloSeeder) ObtainSeeds(ctx context.Context, req *cdnsystem.SeedRequest, psc chan<- *cdnsystem.PieceSeed) (err error) {
	safe.Call(func() {
		fmt.Printf("req:%v\n", req)
		var i = 5
		for {
			select {
			case <-ctx.Done():
				return
			default:
				if i < 0 {
					psc <- &cdnsystem.PieceSeed{State: base.NewState(base.Code_SUCCESS, "success"),
						SeedAddr:      "localhost:12345",
						Done:          true,
						ContentLength: 100,
					}
					return
				}
				psc <- &cdnsystem.PieceSeed{State: base.NewState(base.Code_SUCCESS, "success"), SeedAddr: "localhost:12345"}
				time.Sleep(1 * time.Second)
				i--
			}
		}
	})

	return
}

func (hs helloSeeder) GetPieceTasks(ctx context.Context, req *base.PieceTaskRequest) (*base.PiecePacket, error) {
	return nil, nil
}

func main() {
	lisAddr := basic.NetAddr{
		Type: basic.TCP,
		Addr: ":8003",
	}

	err := rpc.StartServer(lisAddr, &helloSeeder{})

	if err != nil {
		fmt.Printf("finish error:%v\n", err)
	}
}
