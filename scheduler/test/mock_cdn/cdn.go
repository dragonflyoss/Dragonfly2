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

package mock_cdn

import (
	"d7y.io/dragonfly/v2/pkg/basic/dfnet"
	"d7y.io/dragonfly/v2/scheduler/test/common"
	"d7y.io/dragonfly/v2/scheduler/test/mock_client"
	"fmt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"math/rand"
	"net"
	"sync"

	"context"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	"d7y.io/dragonfly/v2/pkg/rpc/cdnsystem"
	"d7y.io/dragonfly/v2/pkg/safe"
	"time"
)

type MockCDN struct {
	cdnsystem.UnimplementedSeederServer
	logger        common.TestLogger
	addr          string
	listener      net.Listener
	pieceInfoList map[string][]*base.PieceInfo
	hostId        string
	finished      map[string]bool
	cdnName       string
	lock          *sync.Mutex
}

func NewMockCDN(addr string, tl common.TestLogger) *MockCDN {
	cdn := &MockCDN{
		logger:        tl,
		addr:          addr,
		hostId:        "cdn:" + addr,
		pieceInfoList: make(map[string][]*base.PieceInfo),
		finished:      make(map[string]bool),
		cdnName:       "cdn",
		lock:          new(sync.Mutex),
	}
	return cdn
}

func (mc *MockCDN) Start() {
	lis, err := net.Listen(string(dfnet.TCP), mc.addr)
	if err != nil {
		mc.logger.Errorf(err.Error())
		return
	}
	mc.listener = lis

	grpcServer := grpc.NewServer()

	cdnsystem.RegisterSeederServer(grpcServer, mc)

	go grpcServer.Serve(lis)
}

func (mc *MockCDN) Stop() {
	if mc.listener != nil {
		mc.listener.Close()
	}
}

func (mc *MockCDN) GetHostId() string {
	return mc.hostId
}

func (mc *MockCDN) GetPieceTasks(ctx context.Context, ptr *base.PieceTaskRequest) (*base.PiecePacket, error) {
	mc.lock.Lock()
	defer mc.lock.Unlock()
	pp := &base.PiecePacket{
		TaskId:     ptr.TaskId,
		PieceInfos: mc.pieceInfoList[ptr.TaskId],
		TotalPiece: -1,
	}
	if mc.finished[ptr.TaskId] {
		pp.TotalPiece = int32(len(mc.pieceInfoList[ptr.TaskId]))
	}
	return pp, nil
}

func (mc *MockCDN) doObtainSeeds(ctx context.Context, req *cdnsystem.SeedRequest, psc chan<- *cdnsystem.PieceSeed) (err error) {
	safe.Call(func() {
		mc.logger.Logf("req:%v\n", req)
		taskId := req.TaskId
		var pieceNum = int32(0)
		var i = 5
		for {
			select {
			case <-ctx.Done():
				return
			default:
				if i < 0 {
					ps := &cdnsystem.PieceSeed{
						PeerId: mc.getPeerId(mc.addr, req.TaskId),
						// cdn node host name
						SeederName:    mc.cdnName,
						Done:          true,
						ContentLength: 100,
					}
					psc <- ps
					mc.lock.Lock()
					mc.finished[taskId] = true
					mc.lock.Unlock()
					return
				}
				ps := &cdnsystem.PieceSeed{
					PieceInfo: &base.PieceInfo{PieceNum: pieceNum},
					PeerId:    mc.getPeerId(mc.addr, taskId),
					// cdn node host name
					SeederName: mc.addr,
				}
				psc <- ps
				mc.lock.Lock()
				mc.pieceInfoList[taskId] = append(mc.pieceInfoList[taskId], ps.PieceInfo)
				mc.lock.Unlock()
				time.Sleep(time.Millisecond * time.Duration(rand.Intn(1000)))
				i--
				pieceNum++
			}
		}
	})

	return
}

func (mc *MockCDN) getPeerId(addr string, taskId string) string {
	return fmt.Sprintf("%s:%s_CDN", addr, taskId)
}

func (mc *MockCDN) ObtainSeeds(sr *cdnsystem.SeedRequest, stream cdnsystem.Seeder_ObtainSeedsServer) (err error) {
	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()

	mock_client.RegisterClient(mc.getPeerId(mc.addr, sr.TaskId), mc)

	mc.logger.Logf("cdn receive a ObtainSeeds %s : %s", sr.GetUrl(), sr.TaskId)

	errChan := make(chan error, 8)
	psc := make(chan *cdnsystem.PieceSeed, 4)

	once := new(sync.Once)
	closePsc := func() {
		once.Do(func() {
			close(psc)
		})
	}
	defer closePsc()

	go call(ctx, psc, mc, sr, errChan)

	go send(psc, closePsc, stream, errChan)

	err = <-errChan

	return
}

func send(psc chan *cdnsystem.PieceSeed, closePsc func(), stream cdnsystem.Seeder_ObtainSeedsServer, errChan chan error) {
	err := safe.Call(func() {
		defer closePsc()

		for v := range psc {
			if err := stream.Send(v); err != nil {
				errChan <- err
				return
			}

			if v.Done {
				break
			}
		}

		errChan <- nil
	})

	if err != nil {
		errChan <- status.Error(codes.FailedPrecondition, err.Error())
	}
}

func call(ctx context.Context, psc chan *cdnsystem.PieceSeed, p *MockCDN, sr *cdnsystem.SeedRequest, errChan chan error) {
	err := safe.Call(func() {
		if err := p.doObtainSeeds(ctx, sr, psc); err != nil {
			errChan <- err
		}
	})

	if err != nil {
		errChan <- status.Error(codes.FailedPrecondition, err.Error())
	}
}
