package daemon

import (
	"context"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/config"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/daemon/mgr"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/daemon/mgr/gc"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/daemon/mgr/task"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/plugins"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/server"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/source"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/store"
	"github.com/dragonflyoss/Dragonfly2/pkg/ratelimiter"
	"github.com/dragonflyoss/Dragonfly2/version"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
)

// Daemon is a struct to identify main instance of cdnNode.
type Daemon struct {
	Name string

	config *config.Config

	httpServer *server.HTTPServer

	rpcServer *server.RPCServer
}

// New creates a new Daemon.
func New(cfg *config.Config, dfgetLogger *logrus.Logger) (*Daemon, error) {
	if err := plugins.Initialize(cfg); err != nil {
		return nil, err
	}
	var err error
	// register cdn build information
	version.NewBuildInfo("cdnnode", prometheus.DefaultRegisterer)

	storeMgr, err := store.NewManager(cfg)
	if err != nil {
		return nil, err
	}
	storeLocal, err := storeMgr.Get(store.LocalStorageDriver)

	sourceClient := source.NewSourceClient()

	rateLimiter := ratelimiter.NewRateLimiter(ratelimiter.TransRate(int64(cfg.MaxBandwidth-cfg.SystemReservedBandwidth)), 2)

	// cdn manager
	cdnMgr, err := mgr.GetCDNManager(cfg, storeLocal, sourceClient, rateLimiter, prometheus.DefaultRegisterer)

	// task manager
	taskMgr, err := task.NewManager(cfg, cdnMgr, sourceClient, prometheus.DefaultRegisterer)
	if err != nil {
		return nil, err
	}

	// gc manager
	gcMgr, err := gc.NewManager(cfg, taskMgr, cdnMgr, prometheus.DefaultRegisterer)
	if err != nil {
		return nil, err
	}

	return &Daemon{
		config:     cfg,
		httpServer: server.NewHttpServer(cfg, dfgetLogger, taskMgr, gcMgr),
		rpcServer:  server.NewRpcServer(cfg, dfgetLogger, taskMgr, gcMgr),
	}, nil
}

// Run runs the daemon.
func (d *Daemon) Run() error {

	httpserver, err := d.httpServer.Start()
	if err != nil {
		logrus.Errorf("failed to start http server: %v", err)
		return err
	}

	if _, err := d.rpcServer.Start(); err != nil {
		logrus.Errorf("failed to start rpc server: %v", err)
		if err := httpserver.Shutdown(context.Background()); err != nil {
			logrus.Errorf("failed to shutdown http server: %v", err)
		}
		return err
	}

	return nil

}
