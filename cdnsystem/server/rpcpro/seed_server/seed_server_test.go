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

package seed_server

import (
	"context"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/config"
	pb "github.com/dragonflyoss/Dragonfly2/pkg/grpc/cdnsystem"
	"google.golang.org/grpc"
	"io"
	"log"
	"strconv"
	"testing"
)

const PORT = config.DefaultListenPort
func TestObtainSeeds(t *testing.T) {
	conn, err := grpc.Dial("localhost:"+strconv.Itoa(PORT), grpc.WithInsecure())
	if err != nil {
		log.Fatalf("grpc.Dial err: %v", err)
	}
	defer conn.Close()
	client := pb.NewSeederClient(conn)
	stream, err := client.ObtainSeeds(context.Background(), &pb.SeedRequest{
		TaskId:  "11311",
		Url:     "https://d1.music.126.net/dmusic/NeteaseMusic2.3.3_840_web.dmg",
		UrlMeta: nil,
	})
	if err != nil {
		log.Fatalf("client.Search err: %v", err)
	}
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalf("client.Search err: %v", err)
		}
		log.Printf("resp: %s", resp)
	}
}