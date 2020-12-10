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

package daemon

import (
	"context"
	"io"
	"net"
	"os"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/dragonflyoss/Dragonfly2/pkg/grpc/base"
	dfdaemongrpc "github.com/dragonflyoss/Dragonfly2/pkg/grpc/dfdaemon"
	"github.com/dragonflyoss/Dragonfly2/pkg/grpc/scheduler"
)

const (
	grpcSock  = "/tmp/df.grpc.sock"
	proxySock = "/tmp/df.proxy.sock"
)

type mockPeerTaskManager struct {
}

func (m mockPeerTaskManager) StartFilePeerTask(ctx context.Context, req *FilePeerTaskRequest) (chan *PeerTaskProgress, error) {
	ch := make(chan *PeerTaskProgress)
	go func() {
		for i := 0; i <= 100; i++ {
			ch <- &PeerTaskProgress{
				State: &base.ResponseState{
					Success: true,
					Code:    base.Code_SUCCESS,
					Msg:     "test ok",
				},
				TaskId:          "",
				ContentLength:   100,
				CompletedLength: uint64(i),
				Done:            i == 100,
			}
		}
		close(ch)
	}()
	return ch, nil
}

func (m mockPeerTaskManager) StartStreamPeerTask(
	ctx context.Context, request *scheduler.PeerTaskRequest) (
	reader io.Reader, attribute map[string]string, err error) {
	panic("implement me")
}

func (m mockPeerTaskManager) Stop(ctx context.Context) error {
	panic("implement me")
}

type mockUploadManager struct {
}

func (m mockUploadManager) Serve(lis net.Listener) error {
	return nil
}

func (m mockUploadManager) Stop() error {
	return nil
}

func TestPeerHost_Serve(t *testing.T) {
	defer os.Remove(grpcSock)
	defer os.Remove(proxySock)
	var host PeerHost
	host = &peerHost{
		host: nil,
		Option: PeerHostOption{
			Scheduler: nil,
			Download: DownloadOption{
				GRPCNetwork:   "unix",
				GRPCListen:    grpcSock,
				GRPInsecure:   true,
				ProxyNetwork:  "unix",
				ProxyListen:   proxySock,
				ProxyInsecure: true,
			},
			Upload: UploadOption{
				Network:  "tcp",
				Listen:   ":12345",
				Insecure: true,
			},
		},
		DownloadManager: &downloadManager{
			peerHost:        &scheduler.PeerHost{},
			peerTaskManager: mockPeerTaskManager{},
		},
		UploadManager: mockUploadManager{},
	}
	go host.Serve()
	defer host.GracefulStop()

	time.Sleep(time.Second)

	conn, err := grpc.Dial("unix://"+grpcSock, grpc.WithInsecure())
	if err != nil {
		t.Fatal(err)
	}
	client := dfdaemongrpc.NewDownloaderClient(conn)
	request := &dfdaemongrpc.DownloadRequest{
		Url:    "http://localhost/test",
		Output: "./testdata/file1",
		BizId:  "unit test",
	}
	down, err := client.Download(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}

	for {
		downResult, err := down.Recv()
		if err == io.EOF {
			t.Logf("recv done")
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if downResult.Done {
			t.Logf("last result: %#v", downResult)
		}
	}
}
