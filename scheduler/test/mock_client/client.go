package mock_client

import (
	"context"
	"fmt"
	"github.com/dragonflyoss/Dragonfly2/pkg/basic"
	"github.com/dragonflyoss/Dragonfly2/pkg/rpc/base"
	"github.com/dragonflyoss/Dragonfly2/pkg/rpc/scheduler"
	"github.com/dragonflyoss/Dragonfly2/pkg/rpc/scheduler/client"
	"github.com/dragonflyoss/Dragonfly2/scheduler/test/common"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

var clientNum = int32(0)

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
	logger        common.TestLogger
	replyChan     chan *scheduler.PieceResult
	replyFinished chan struct{}
	in            chan<- *scheduler.PieceResult
	out           <-chan *scheduler.PeerPacket
	pid           string
	taskId        string
	waitStop      chan struct{}
	pieceTaskList []*base.PieceTask
	parentId      string
	TotalPiece    int32 `protobuf:"varint,6,opt,name=total_piece,json=totalPiece,proto3" json:"total_piece,omitempty"`
	ContentLength int64 `protobuf:"varint,7,opt,name=content_length,json=contentLength,proto3" json:"content_length,omitempty"`
	// sha256 code of all piece md5
	PieceMd5Sign string `protobuf:"bytes,8,opt,name=piece_md5_sign,json=pieceMd5Sign,proto3" json:"piece_md5_sign,omitempty"`
}

func NewMockClient(addr string, logger common.TestLogger) *MockClient {
	c, err := client.CreateClient([]basic.NetAddr{{Type: basic.TCP, Addr: addr}})
	if err != nil {
		panic(err)
	}
	pid := atomic.AddInt32(&clientNum, 1)
	mc := &MockClient{
		cli:           c,
		pid:           fmt.Sprintf("%04d", pid),
		logger:        logger,
		replyChan:     make(chan *scheduler.PieceResult, 1000),
		waitStop:      make(chan struct{}),
		replyFinished: make(chan struct{}),
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

func (mc *MockClient) GetStopChan() chan struct{} {
	return mc.waitStop
}

func (mc *MockClient) GetPieceTasks(context.Context, *base.PieceTaskRequest) (*base.PiecePacket, error) {
	return &base.PiecePacket{
		TaskId:     mc.taskId,
		TotalPiece : mc.TotalPiece,
		ContentLength: mc.ContentLength,
		// sha256 code of all piece md5
		PieceMd5Sign: mc.PieceMd5Sign,
	}, nil
}

func (mc *MockClient) registerPeerTask() (err error) {
	request := &scheduler.PeerTaskRequest{
		Url:    "http://dragonfly.com/test-multi",
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
			RpcPort:           23456,
			HostName:       fmt.Sprintf("host%s", mc.pid),
			SecurityDomain: "",
			Location:       "",
			Idc:            "",
			NetTopology:         "",
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
	if pkg.MainPeer != nil {
		mc.parentId = pkg.MainPeer.PeerId
	}

	wait := make(chan bool)

	go func() {
		ctx, cancel := context.WithCancel(context.TODO())
		defer cancel()
		mc.in, mc.out, err = mc.cli.ReportPieceResult(ctx, mc.taskId, request)
		if err != nil {
			mc.logger.Errorf("[%s] PullPieceTasks failed: %e", mc.pid, err)
			return
		}
		wait <- true
		mc.replyChan <- nil

		for {
			resp := <-mc.out
			if resp.MainPeer != nil {
				mc.parentId = resp.MainPeer.PeerId
				logMsg := fmt.Sprintf("[%s] recieve a parent: %s", mc.pid, mc.parentId)
				mc.logger.Log(logMsg)
			} else {
				mc.logger.Logf("[%s] receive a empty parent\n", mc.pid)
			}
			if resp == nil {
				mc.logger.Logf("client[%s] download finished", mc.pid)
			 	break
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
	for  {
		select {
		case t = <-mc.replyChan:
		case <-mc.waitStop:
			break
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
		time.Sleep(time.Millisecond * time.Duration(rand.Intn(1000)))
		mc.in <- pr
	}
}

func (mc *MockClient) downloadPieces() {
	for {
		select {
		case <-mc.waitStop:
			return
		default:
		}
		cli := GetClient(mc.parentId)
		if cli == nil {
			time.Sleep(time.Second)
			continue
		}
		pieces, _ := cli.GetPieceTasks(nil, nil)
		if pieces != nil {
			if pieces.TotalPiece >= 0 && len(pieces.PieceTasks) == len(mc.pieceTaskList) {
				mc.TotalPiece = pieces.TotalPiece
				mc.ContentLength = pieces.ContentLength
				mc.PieceMd5Sign = pieces.PieceMd5Sign
				mc.logger.Logf("client[%s] download finished from [%s]", mc.pid, mc.parentId)
				close(mc.waitStop)
				return
			}
			for _, p := range pieces.PieceTasks {
				has := false
				for _, lp := range mc.pieceTaskList {
					if p.PieceNum == lp.PieceNum {
						has = true
						break
					}
				}
				if !has {
					pr := &scheduler.PieceResult{
						TaskId:     mc.taskId,
						SrcPid:     mc.pid,
						DstPid:     mc.parentId,
						PieceNum:   p.PieceNum,
						PieceRange: fmt.Sprintf("%d,%d", p.RangeStart, p.RangeStart+uint64(p.RangeSize)),
						Success:    true,
						Code:       base.Code_SUCCESS,
						BeginTime:  uint64(time.Now().UnixNano() / int64(time.Millisecond)),
					}
					time.Sleep(time.Millisecond * time.Duration(rand.Intn(1000)+100))
					pr.EndTime = uint64(time.Now().UnixNano() / int64(time.Millisecond))
					mc.pieceTaskList = append(mc.pieceTaskList, p)
					mc.logger.Logf("client[%s] download a piece [%d] from [%s]", mc.pid, p.PieceNum, mc.parentId)
					mc.replyChan <- pr
					break
				} else {
					time.Sleep(time.Second)
				}
			}
		}
	}
}
