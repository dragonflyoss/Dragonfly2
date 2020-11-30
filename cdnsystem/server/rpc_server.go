package server

import (
	"context"
	"fmt"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/config"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/daemon/mgr"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/server/rpc"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/source"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/sourceclient"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/sourceclient/httpclient"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/store"
	"github.com/dragonflyoss/Dragonfly2/version"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"google.golang.org/genproto/googleapis/ads/googleads/v1/services"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"log"
	"net"
	"net/http"
	"time"

	pb "github.com/dragonflyoss/Dragonfly2/pkg/grpc/cdnsystem"
	"google.golang.org/grpc"
)

var rpcLogger *logrus.Logger

// New creates a brand new server instance.
func NewRpcServer(cfg *config.Config, logger *logrus.Logger, register prometheus.Registerer) (*RPCServer, error) {
	var err error
	// register supernode build information
	version.NewBuildInfo("cdnnode", register)

	dfgetLogger = logger

	sm, err := store.NewManager(cfg)
	if err != nil {
		return nil, err
	}
	storeLocal, err := sm.Get(store.LocalStorageDriver)
	if err != nil {
		return nil, err
	}

	originClient := http.NewOriginClient()
	peerMgr, err := peer.NewManager(register)
	if err != nil {
		return nil, err
	}

	dfgetTaskMgr, err := dfgettask.NewManager(cfg, register)
	if err != nil {
		return nil, err
	}

	progressMgr, err := progress.NewManager(cfg)
	if err != nil {
		return nil, err
	}

	schedulerMgr, err := scheduler.NewManager(cfg, progressMgr)
	if err != nil {
		return nil, err
	}
	cdnMgr, err := mgr.GetCDNManager(cfg, storeLocal, progressMgr, originClient, register)
	if err != nil {
		return nil, err
	}
}
// Server is supernode server struct.
type RPCServer struct {
	Config       *config.Config
	TaskMgr      mgr.TaskMgr  // task管理
	ProgressMgr  mgr.ProgressMgr  // 进度管理
	GCMgr        mgr.GCMgr  // 垃圾回收
	sourceClient source.SourceClient
}


// Start runs cdn rpc server.
func (s *RPCServer) Start() (*grpc.Server, error) {
	address := fmt.Sprintf("0.0.0.0:%d", s.Config.ListenRpcPort)
	lis, err := net.Listen("tcp", address)
	if err != nil {
		logrus.Errorf("failed to listen port %d: %v", s.Config.ListenHttpPort, err)
		return nil, err
	}
	rpcServer := grpc.NewServer()
	registerServers(rpcServer)

	if err := rpcServer.Serve(lis); err != nil {
		logrus.Errorf("failed to server %v", err)
	}
	s.GCMgr.StartGC(context.Background())
	return rpcServer, nil
}

func registerServers(s *grpc.Server) {
	pb.RegisterSeederServer(s, &rpc.CdnSeedServer{

	})
}
