package daemon

import (
	"context"
	"net"
	"os"
	"testing"
	"time"

	"github.com/dragonflyoss/Dragonfly2/pkg/grpc/base"
	"github.com/dragonflyoss/Dragonfly2/pkg/grpc/scheduler"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

const (
	schedulerSock = "/tmp/df.scheduler.grpc.sock"
)

type mockSchedulerServer struct {
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
		logrus.Errorf("failed to listen for mockSchedulerServer grpc service: %v", err)
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
	if err != nil {
		t.Fatalf("NewStaticSchedulerLocator error: %s", err)
	}
	_, _, err = sl.Next()
	if err != nil {
		t.Errorf("get next avaialbe scheduler error: %s", err)
	}
}
