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
	"sync"

	"d7y.io/dragonfly/v2/pkg/basic/dfnet"
	"d7y.io/dragonfly/v2/pkg/dflog/logcore"
	"d7y.io/dragonfly/v2/pkg/idgen"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	_ "d7y.io/dragonfly/v2/pkg/rpc/scheduler/server"
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
	for i := 0; i < 1; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			//psc, _ := c.ObtainSeeds(context.TODO(), &cdnsystem.SeedRequest{
			//	TaskId: "test2",
			//	Url:    "https://desktop.docker.com/mac/stable/amd64/Docker.dmg",
			//	Filter: "",
			//})

			psc, _ := c.ObtainSeeds(context.TODO(), &cdnsystem.SeedRequest{
				TaskId: idgen.TaskID("https://desktop.docker.com/mac/stable/amd64/Docker.dmg?a=a&b=b&c=c", "a&b", &base.UrlMeta{
					Digest: "md5",
					Range:  "50-1000",
				}, "dragonfly"),
				Url:    "https://desktop.docker.com/mac/stable/amd64/Docker.dmg?a=a&b=b&c=c",
				Filter: "a&b",
			})
			for {
				piece, err := psc.Recv()
				if err != nil {
					fmt.Println(err)
					break
				}
				fmt.Println(piece)
			}
		}()
	}
	wg.Wait()
	fmt.Println("client finish")
}
