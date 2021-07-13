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

import (
	"context"
	"fmt"
	"time"

	"d7y.io/dragonfly/v2/internal/dflog/logcore"
	"d7y.io/dragonfly/v2/pkg/rpc"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	"d7y.io/dragonfly/v2/pkg/rpc/cdnsystem"
	"d7y.io/dragonfly/v2/pkg/safe"
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
					psc <- &cdnsystem.PieceSeed{
						Done:          true,
						ContentLength: 100,
					}
					return
				}
				psc <- &cdnsystem.PieceSeed{}
				time.Sleep(1 * time.Second)
				i--
			}
		}
	})

	return
}

func (hs *helloSeeder) GetPieceTasks(context.Context, *base.PieceTaskRequest) (*base.PiecePacket, error) {
	return nil, nil
}

func main() {
	logcore.InitCdnSystem(false)
	err := rpc.StartTCPServer(12345, 12345, &helloSeeder{})

	if err != nil {
		fmt.Printf("finish error:%v\n", err)
	}
}
