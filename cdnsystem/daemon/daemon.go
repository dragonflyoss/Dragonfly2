
package daemon

import (
	"context"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/config"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/plugins"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/server"
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

	httpServer, err := server.NewHttpServer(cfg, dfgetLogger, prometheus.DefaultRegisterer)
	if err != nil {
		return nil, err
	}

	rpcServer, err := server.NewRpcServer(cfg, dfgetLogger, prometheus.DefaultRegisterer)
	if err != nil {
		return nil, err
	}
	return &Daemon{
		config: cfg,
		httpServer: httpServer,
		rpcServer: rpcServer,
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
