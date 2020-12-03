package server

import (
	"context"
	"fmt"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/config"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/daemon/mgr"
	"github.com/sirupsen/logrus"
	"net"
	"net/http"
	"time"
)

var dfgetLogger *logrus.Logger


// New creates a brand new server instance.
func NewHttpServer(cfg *config.Config, logger *logrus.Logger, taskMgr mgr.SeedTaskMgr, gcMgr mgr.GCMgr) *HTTPServer {
	dfgetLogger = logger

	return &HTTPServer{
		Config: cfg,
		GCMgr: gcMgr,
		TaskMgr: taskMgr,
	}
}
// Server is supernode server struct.
type HTTPServer struct {
	Config       *config.Config
	TaskMgr      mgr.SeedTaskMgr  // task管理
	GCMgr        mgr.GCMgr  // 垃圾回收
}


// Start runs cdn server.
func (s *HTTPServer) Start() (*http.Server, error) {
	// 创建api路由
	router := createRouter(s)

	address := fmt.Sprintf("0.0.0.0:%d", s.Config.ListenHttpPort)

	l, err := net.Listen("tcp", address)
	if err != nil {
		logrus.Errorf("failed to listen port %d: %v", s.Config.ListenHttpPort, err)
		return nil, err
	}

	s.GCMgr.StartGC(context.Background())

	server := &http.Server{
		Handler:           router,
		ReadTimeout:       time.Minute * 10,
		ReadHeaderTimeout: time.Minute * 10,
		IdleTimeout:       time.Minute * 10,
	}
	return server, server.Serve(l)
}

