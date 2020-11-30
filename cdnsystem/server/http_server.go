package server

import (
	"context"
	"fmt"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/config"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/daemon/mgr"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/daemon/mgr/gc"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/daemon/mgr/preheat"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/source"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/store"
	"github.com/dragonflyoss/Dragonfly2/version"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"net"
	"net/http"
	"time"
)

var dfgetLogger *logrus.Logger

// Server is supernode server struct.
type Server struct {
	Config        *config.Config
	PeerMgr       mgr.PeerMgr
	TaskMgr       mgr.TaskMgr
	DfgetTaskMgr  mgr.DfgetTaskMgr
	GCMgr         mgr.GCMgr
	PieceErrorMgr mgr.PieceErrorMgr
	PreheatMgr    mgr.PreheatManager
}

// New creates a brand new server instance.
func NewHttpServer(cfg *config.Config, logger *logrus.Logger, register prometheus.Registerer) (*HTTPServer, error) {
	var err error
	// register cdn build information
	version.NewBuildInfo("cdnnode", register)

	dfgetLogger = logger

	storeMgr, err := store.NewManager(cfg)
	if err != nil {
		return nil, err
	}

	sourceClientMgr, err := source.NewManager(cfg)
	if err != nil {
		return nil, err
	}

	// cdn manager
	cdnMgr, err := mgr.GetCDNManager(cfg, storeMgr, sourceClientMgr, register)
	if err != nil {
		return nil, err
	}

	gcMgr, err := gc.NewManager(cfg, cdnMgr, register)
	if err != nil {
		return nil, err
	}

	preheatMgr, err := preheat.NewManager(cfg)
	if err != nil {
		return nil, err
	}

	return &Server{
		Config: cfg,
		GCMgr: gcMgr,
		PreheatMgr: preheatMgr,
	}
}
// Server is supernode server struct.
type HTTPServer struct {
	Config       *config.Config
	TaskMgr      mgr.TaskMgr  // task管理
	GCMgr        mgr.GCMgr  // 垃圾回收
	sourceClient source.SourceClient
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

