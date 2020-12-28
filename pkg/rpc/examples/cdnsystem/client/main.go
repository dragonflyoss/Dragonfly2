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

import _ "github.com/dragonflyoss/Dragonfly2/pkg/rpc/scheduler/server"

import (
	"context"
	"fmt"
	"github.com/dragonflyoss/Dragonfly2/pkg/basic"
	"github.com/dragonflyoss/Dragonfly2/pkg/rpc/cdnsystem"
	"github.com/dragonflyoss/Dragonfly2/pkg/rpc/cdnsystem/client"
	"github.com/google/uuid"
)

func main() {
	c, err := client.CreateClient([]basic.NetAddr{{Type: basic.TCP, Addr: "localhost:8002"}})
	if err != nil {
		panic(err)
	}

	psc, err := c.ObtainSeeds(context.TODO(), &cdnsystem.SeedRequest{
		TaskId:  uuid.New().String(),
		Url:     "http://www.baidu.com",
		Filter:  "",
		UrlMeta: nil,
	})
	if err != nil {
		panic(err)
	}

	for pieceSeed := range psc {
		fmt.Printf("response:%v\n", pieceSeed)
	}

	fmt.Println("client finish")
}
