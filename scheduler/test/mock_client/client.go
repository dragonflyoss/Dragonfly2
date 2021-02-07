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

package mock_client

import (
	"context"
	"d7y.io/dragonfly/v2/pkg/basic/dfnet"
	"d7y.io/dragonfly/v2/pkg/dfcodes"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	"d7y.io/dragonfly/v2/pkg/rpc/scheduler"
	"d7y.io/dragonfly/v2/pkg/rpc/scheduler/client"
	"d7y.io/dragonfly/v2/scheduler/test/common"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

var clientNum = map[string]*int32{}

var clientMap = make(map[string]IDownloadClient)
var clientMapLock = new(sync.RWMutex)

func ClearClient() {
	clientMapLock.Lock()
	clientMap = make(map[string]IDownloadClient)
	clientMapLock.Unlock()
}

func RegisterClient(id string, downloadClient IDownloadClient) {
	clientMapLock.Lock()
	clientMap[id] = downloadClient
	clientMapLock.Unlock()
}

func UnregisterClient(id string) {
	clientMapLock.Lock()
	delete(clientMap, id)
	clientMapLock.Unlock()
}

func GetClient(id string) IDownloadClient {
	clientMapLock.RLock()
	defer clientMapLock.RUnlock()
	return clientMap[id]
}

type IDownloadClient interface {
	GetPieceTasks(context.Context, *base.PieceTaskRequest) (*base.PiecePacket, error)
}

type MockClient struct {
	cli           client.SchedulerClient
	lock          *sync.Mutex
	logger        common.TestLogger
	replyChan     chan *scheduler.PieceResult
	replyFinished chan struct{}
	in            chan<- *scheduler.PieceResult
	out           <-chan *scheduler.PeerPacket
	url           string
	pid           string
	taskId        string
	waitStop      chan struct{}
	pieceInfoList []*base.PieceInfo
	parentId      string
	TotalPiece    int32 `protobuf:"varint,6,opt,name=total_piece,json=totalPiece,proto3" json:"total_piece,omitempty"`
	ContentLength int64 `protobuf:"varint,7,opt,name=content_length,json=contentLength,proto3" json:"content_length,omitempty"`
	// sha256 code of all piece md5
	PieceMd5Sign string `protobuf:"bytes,8,opt,name=piece_md5_sign,json=pieceMd5Sign,proto3" json:"piece_md5_sign,omitempty"`
}

func NewMockClient(addr string, url string, group string, logger common.TestLogger) *MockClient {
	c, err := client.CreateClient([]dfnet.NetAddr{{Type: dfnet.TCP, Addr: addr}})
	if err != nil {
		panic(err)
	}
	if clientNum[group] == nil {
		clientNum[group] = new(int32)
	}
	pid := atomic.AddInt32(clientNum[group], 1)
	mc := &MockClient{
		cli:           c,
		lock:          new(sync.Mutex),
		pid:           fmt.Sprintf("%s%02d", group, pid),
		url:           url,
		logger:        logger,
		replyChan:     make(chan *scheduler.PieceResult, 1000),
		waitStop:      make(chan struct{}),
		replyFinished: make(chan struct{}),
		TotalPiece:    -1,
	}
	RegisterClient(mc.pid, mc)
	logger.Logf("NewMockClient %s", mc.pid)
	return mc
}

func (mc *MockClient) Start() {
	err := mc.registerPeerTask()
	if err != nil {
		mc.logger.Errorf("start client [%d] failed: %e", mc.pid, err)
		return
	}

	go mc.downloadPieces()

	go mc.replyMessage()
}

func (mc *MockClient) SetDone() {
	close(mc.waitStop)
}

func (mc *MockClient) GetStopChan() chan struct{} {
	return mc.waitStop
}

func (mc *MockClient) GetPieceTasks(context.Context, *base.PieceTaskRequest) (*base.PiecePacket, error) {
	mc.lock.Lock()
	defer mc.lock.Unlock()
	return &base.PiecePacket{
		TaskId:        mc.taskId,
		TotalPiece:    mc.TotalPiece,
		ContentLength: mc.ContentLength,
		PieceInfos:    mc.pieceInfoList,
		// sha256 code of all piece md5
		PieceMd5Sign: mc.PieceMd5Sign,
	}, nil
}

func (mc *MockClient) registerPeerTask() (err error) {
	request := &scheduler.PeerTaskRequest{
		Url:    mc.url,
		Filter: "",
		BizId:  "12345",
		UrlMata: &base.UrlMeta{
			Md5:   "",
			Range: "",
		},
		PeerId: mc.pid,
		PeerHost: &scheduler.PeerHost{
			Uuid:           fmt.Sprintf("%s", mc.pid),
			Ip:             "127.0.0.1",
			RpcPort:        23456,
			DownPort:       23457,
			HostName:       fmt.Sprintf("host%s", mc.pid),
			SecurityDomain: "",
			Location:       "",
			Idc:            "",
			NetTopology:    "",
		},
	}
	pkg, err := mc.cli.RegisterPeerTask(context.TODO(), request)
	if err != nil {
		mc.logger.Errorf("[%s] RegisterPeerTask failed: %e", mc.pid, err)
		return
	}
	if pkg == nil {
		mc.logger.Errorf("[%s] RegisterPeerTask failed: pkg is null", mc.pid)
		return
	}
	mc.taskId = pkg.TaskId

	if pkg.DirectPiece != nil {
		switch pkg.DirectPiece.(type) {
		case *scheduler.RegisterResult_SinglePiece:
		case *scheduler.RegisterResult_PieceContent:
		default:
			panic("direct piece type error")
		}
		close(mc.waitStop)
		return
	}

	wait := make(chan bool)

	go func() {
		ctx, cancel := context.WithCancel(context.TODO())
		defer func() {
			time.Sleep(time.Second * 3)
			cancel()
		}()
		mc.in, mc.out, err = mc.cli.ReportPieceResult(ctx, mc.taskId, request)
		if err != nil {
			mc.logger.Errorf("[%s] PullPieceTasks failed: %e", mc.pid, err)
			return
		}
		wait <- true
		mc.replyChan <- nil
		var resp *scheduler.PeerPacket

		for {
			select {
			case resp = <-mc.out:
			case <-mc.waitStop:
				mc.logger.Logf("client[%s] is down", mc.pid)
				return
			}
			if resp.MainPeer != nil {
				mc.lock.Lock()
				mc.parentId = resp.MainPeer.PeerId
				mc.lock.Unlock()
				logMsg := fmt.Sprintf("[%s] recieve a parent: %s", mc.pid, mc.parentId)
				mc.logger.Log(logMsg)
			} else {
				mc.logger.Logf("[%s] receive a empty parent\n", mc.pid)
			}
		}
		<-mc.replyFinished
	}()
	<-wait

	return
}

func (mc *MockClient) replyMessage() {
	defer func() {
		recover()
		close(mc.replyFinished)
	}()
	var t *scheduler.PieceResult
	for {
		select {
		case t = <-mc.replyChan:
		case <-mc.waitStop:
			select {
			case t = <-mc.replyChan:
			default:
				return
			}
		}
		var pr *scheduler.PieceResult
		if t == nil {
			pr = &scheduler.PieceResult{
				TaskId:   mc.taskId,
				SrcPid:   mc.pid,
				PieceNum: -1,
			}
		} else {
			pr = t
		}
		time.Sleep(time.Millisecond * time.Duration(rand.Intn(10)))
		mc.in <- pr
		mc.logger.Logf("[%s][%s] send a pieceResult %d", mc.taskId, mc.pid, pr.PieceNum)
	}
}

func (mc *MockClient) downloadPieces() {
	for {
		select {
		case <-mc.waitStop:
			return
		default:
		}
		mc.lock.Lock()
		cli := GetClient(mc.parentId)
		mc.lock.Unlock()
		if cli == nil {
			time.Sleep(time.Second)
			continue
		}
		pieces, _ := cli.GetPieceTasks(nil, &base.PieceTaskRequest{
			TaskId: mc.taskId,
		})
		if pieces != nil {
			if pieces.TotalPiece >= 0 && len(pieces.PieceInfos) == len(mc.pieceInfoList) {
				mc.TotalPiece = pieces.TotalPiece
				mc.ContentLength = pieces.ContentLength
				mc.PieceMd5Sign = pieces.PieceMd5Sign
				mc.logger.Logf("client[%s] download finished from [%s], TotalPiece[%d]", mc.pid, mc.parentId, mc.TotalPiece)
				close(mc.waitStop)
				return
			}
			same := true
			for _, p := range pieces.PieceInfos {
				has := false
				for _, lp := range mc.pieceInfoList {
					if p.PieceNum == lp.PieceNum {
						has = true
						break
					}
				}
				if !has {
					pr := &scheduler.PieceResult{
						TaskId:        mc.taskId,
						SrcPid:        mc.pid,
						DstPid:        mc.parentId,
						PieceNum:      p.PieceNum,
						FinishedCount: int32(len(mc.pieceInfoList) + 1),
						Success:       true,
						Code:          dfcodes.Success,
						BeginTime:     uint64(time.Now().UnixNano() / int64(time.Millisecond)),
					}
					time.Sleep(time.Millisecond * time.Duration(rand.Intn(40)+10))
					pr.EndTime = uint64(time.Now().UnixNano() / int64(time.Millisecond))
					mc.pieceInfoList = append(mc.pieceInfoList, p)
					mc.logger.Logf("client[%s] download a piece [%d] from [%s]", mc.pid, p.PieceNum, mc.parentId)
					mc.replyChan <- pr
					same = false
				}
			}
			if same {
				time.Sleep(time.Second / 2)
			} else {
				if pieces.TotalPiece >= 0 && len(pieces.PieceInfos) == len(mc.pieceInfoList) {
					mc.TotalPiece = pieces.TotalPiece
					mc.ContentLength = pieces.ContentLength
					mc.PieceMd5Sign = pieces.PieceMd5Sign
					mc.logger.Logf("client[%s] download finished from [%s], TotalPiece[%d]", mc.pid, mc.parentId, mc.TotalPiece)
					select {
					case _, ok := <-mc.waitStop:
						if ok {
							close(mc.waitStop)
						}
					default:
						close(mc.waitStop)
					}
					return
				}
			}
		}
	}
}
