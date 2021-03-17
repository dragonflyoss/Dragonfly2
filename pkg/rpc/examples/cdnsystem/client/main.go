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
	"d7y.io/dragonfly/v2/pkg/basic/dfnet"
	"d7y.io/dragonfly/v2/pkg/dflog/logcore"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	_ "d7y.io/dragonfly/v2/pkg/rpc/scheduler/server"
	"sync"
)

import (
	"context"
	"fmt"

	"d7y.io/dragonfly/v2/pkg/rpc/cdnsystem"
	"d7y.io/dragonfly/v2/pkg/rpc/cdnsystem/client"
)

func main() {
	logcore.InitCdnSystem(true)
	c, err := client.GetClientByAddr([]dfnet.NetAddr{
		{
			Type: dfnet.TCP,
			Addr: "127.0.0.1:8003",
		},
	})
	if err != nil {
		panic(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			psc, err := c.ObtainSeeds(context.TODO(), &cdnsystem.SeedRequest{
				TaskId: "test",
				Url:    "http://www.baidu.com",
				Filter: "",
			})
			if err != nil {
				panic(err)
			}

			for pieceSeed := range psc {
				fmt.Printf("response:%v\n", pieceSeed)
			}
		}()
	}
	wg.Wait()
	fmt.Println("client finish")
}

func main2() {
	c, err := client.GetClientByAddr([]dfnet.NetAddr{
		{
			Type: dfnet.TCP,
			Addr: "localhost:8003",
		},
	})
	if err != nil {
		panic(err)
	}

	psc, err := c.GetPieceTasks(context.TODO(), &base.PieceTaskRequest{
		TaskId:   "test",
		SrcIp:    "11.11.11.11",
		StartNum: 1,
		Limit:    4,
	})
	if err != nil {
		fmt.Printf("%v", err)
	}

	fmt.Printf("client finish:%v", psc)
}
