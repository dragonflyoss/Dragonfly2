package server

import (
	"context"
	"fmt"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/config"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/daemon/mgr"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/server/rpc"
	pb "github.com/dragonflyoss/Dragonfly2/pkg/rpc/cdnsystem"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"net"
)

var rpcLogger *logrus.Logger

// New creates a brand new server instance.
func NewRpcServer(cfg *config.Config, logger *logrus.Logger, taskMgr mgr.SeedTaskMgr, gcMgr mgr.GCMgr) *RPCServer{
	rpcLogger = logger
	return &RPCServer{
		Config:       cfg,
		TaskMgr:      taskMgr,
		GCMgr:        gcMgr,
	}
}


type RPCServer struct {
	Config       *config.Config
	TaskMgr      mgr.SeedTaskMgr  // task管理
	GCMgr        mgr.GCMgr  // 垃圾回收
}


// Start runs cdn rpc server.
func (s *RPCServer) Start() (*grpc.Server, error) {
	address := fmt.Sprintf("0.0.0.0:%d", s.Config.ListenPort)
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
	pb.RegisterSeederServer(s, &rpc.CdnSeedServer{})
}
