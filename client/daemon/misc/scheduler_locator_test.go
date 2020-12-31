package misc

import (
	"context"
	"net"
	"os"
	"testing"
	"time"

	testifyassert "github.com/stretchr/testify/assert"
	"google.golang.org/grpc"

	"github.com/dragonflyoss/Dragonfly2/pkg/rpc/base"
	_ "github.com/dragonflyoss/Dragonfly2/pkg/rpc/dfdaemon/server"
	"github.com/dragonflyoss/Dragonfly2/pkg/rpc/scheduler"
)

const (
	schedulerSock = "/tmp/df.scheduler.grpc.sock"
)

type mockSchedulerServer struct {
	scheduler.UnimplementedSchedulerServer
	server *grpc.Server
}

func NewMockSchedulerServer() *mockSchedulerServer {
	f := &mockSchedulerServer{}
	s := grpc.NewServer()
	scheduler.RegisterSchedulerServer(s, f)
	f.server = s
	return f
}

func (f *mockSchedulerServer) Serve() error {
	listener, err := net.Listen("unix", schedulerSock)
	if err != nil {
		return err
	}
	return f.server.Serve(listener)
}

func (f mockSchedulerServer) RegisterPeerTask(ctx context.Context, request *scheduler.PeerTaskRequest) (*scheduler.PiecePackage, error) {
	panic("implement me")
}

func (f mockSchedulerServer) PullPieceTasks(server scheduler.Scheduler_PullPieceTasksServer) error {
	panic("implement me")
}

func (f mockSchedulerServer) ReportPeerResult(ctx context.Context, result *scheduler.PeerResult) (*base.ResponseState, error) {
	panic("implement me")
}

func (f mockSchedulerServer) LeaveTask(ctx context.Context, target *scheduler.PeerTarget) (*base.ResponseState, error) {
	panic("implement me")
}

func TestNewStaticSchedulerLocator(t *testing.T) {
	assert := testifyassert.New(t)
	defer os.Remove(schedulerSock)
	schedulerServer := NewMockSchedulerServer()
	go func() {
		if err := schedulerServer.Serve(); err != nil {
			t.Fatalf("mockSchedulerServer setup error: %s", err)
		}
	}()
	time.Sleep(time.Second)
	defer schedulerServer.server.GracefulStop()

	sl, err := NewStaticSchedulerLocator([]string{"unix://" + schedulerSock}, grpc.WithInsecure())
	assert.Nil(err, "NewStaticSchedulerLocator")
	sl.Refresh(nil)
	_, _, err = sl.Next()
	assert.Nil(err, "get next avaialbe scheduler")
}
