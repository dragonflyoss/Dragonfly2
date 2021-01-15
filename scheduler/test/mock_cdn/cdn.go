package mock_cdn

import (
	"github.com/dragonflyoss/Dragonfly2/pkg/basic"
	"github.com/dragonflyoss/Dragonfly2/pkg/rpc"
	"github.com/dragonflyoss/Dragonfly2/scheduler/test/common"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"math/rand"
	"net"
	"sync"

	"context"
	"github.com/dragonflyoss/Dragonfly2/pkg/rpc/base"
	"github.com/dragonflyoss/Dragonfly2/pkg/rpc/cdnsystem"
	"github.com/dragonflyoss/Dragonfly2/pkg/safe"
	"time"
)

type MockCDN struct {
	cdnsystem.UnimplementedSeederServer
	logger   common.TestLogger
	addr     string
	listener net.Listener
	pieceTaskList []*base.PieceTask
	taskId string
	hostId string
	finished bool
}

func NewMockCDN(addr string, tl common.TestLogger) *MockCDN {
	cdn := &MockCDN{
		logger: tl,
		addr:   addr,
		hostId: "cdn:"+addr,
	}
	return cdn
}

func (mc *MockCDN) Start() {
	lis, err := net.Listen(string(basic.TCP), mc.addr)
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

func (mc *MockCDN) GetPieceTasks(context.Context, *base.PieceTaskRequest) (*base.PiecePacket, error)  {
	pp := &base.PiecePacket{
		TaskId:     mc.taskId,
		PieceTasks: mc.pieceTaskList,
		TotalPiece: -1,
	}
	if mc.finished {
		pp.TotalPiece = int32(len(mc.pieceTaskList))
	}
	return pp, nil
}

func (mc *MockCDN) doObtainSeeds(ctx context.Context, req *cdnsystem.SeedRequest, psc chan<- *cdnsystem.PieceSeed) (err error) {
	safe.Call(func() {
		mc.logger.Logf("req:%v\n", req)
		mc.taskId = req.TaskId
		var pieceNum = int32(0)
		var i = 5
		for {
			select {
			case <-ctx.Done():
				return
			default:
				if i < 0 {
					ps := &cdnsystem.PieceSeed{State: base.NewState(base.Code_SUCCESS, "success"),
						SeedAddr:      "localhost:12345",
						Done:          true,
						ContentLength: 100,
						TotalTraffic:  100,
					}
					psc <- ps
					mc.finished = true
					return
				}
				ps := &cdnsystem.PieceSeed{State: base.NewState(base.Code_SUCCESS, "success"), SeedAddr: "localhost:12345", PieceNum: pieceNum}
				psc <- ps
				mc.pieceTaskList = append(mc.pieceTaskList, &base.PieceTask{
					PieceNum: ps.PieceNum,
				})
					time.Sleep(time.Millisecond * time.Duration(rand.Intn(3000)))
				i--
				pieceNum++
			}
		}
	})

	return
}

func (mc *MockCDN) ObtainSeeds(sr *cdnsystem.SeedRequest, stream cdnsystem.Seeder_ObtainSeedsServer) (err error) {
	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()

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
			errChan <- rpc.ConvertServerError(err)
		}
	})

	if err != nil {
		errChan <- status.Error(codes.FailedPrecondition, err.Error())
	}
}
