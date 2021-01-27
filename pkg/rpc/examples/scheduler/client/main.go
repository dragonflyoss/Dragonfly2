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
	"github.com/dragonflyoss/Dragonfly2/pkg/basic/dfnet"
	"github.com/dragonflyoss/Dragonfly2/pkg/basic/env"
	logger "github.com/dragonflyoss/Dragonfly2/pkg/dflog"
	"github.com/dragonflyoss/Dragonfly2/pkg/rpc/scheduler"
	"github.com/dragonflyoss/Dragonfly2/pkg/rpc/scheduler/client"
	"github.com/google/uuid"
	"os"
)

func main() {
	os.Setenv(env.ActiveProfile, "local")
	logger.InitScheduler()

	if client, err := client.CreateClient([]dfnet.NetAddr{{
		Type: dfnet.TCP,
		Addr: ":32501",
	}}); err == nil {
		c0,c1,err := client.ReportPieceResult(context.Background(),uuid.New().String(),&scheduler.PeerTaskRequest{})
		if err == nil{
			c0 <- &scheduler.PieceResult{}
			x := <-c1
			fmt.Println(x)
		}
		//_, err := client.LeaveTask(context.Background(), &scheduler.PeerTarget{
		//	TaskId: uuid.New().String(),
		//	PeerId: uuid.New().String(),
		//})
		//
		//s := status.Convert(err)
		//for _, d := range s.Details() {
		//	switch info := d.(type) {
		//	case *base.ResponseState:
		//		log.Printf("Quota failure: %v", info)
		//	default:
		//		log.Printf("Unexpected type: %s", info)
		//	}
		//}
		//
		//e := errors.Cause(err)
		//
		//dferrors.CheckError(err,dfcodes.ClientError)
		//dferrors.CheckError(e,dfcodes.ClientError)

	}

}
