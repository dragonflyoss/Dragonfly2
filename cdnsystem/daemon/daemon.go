package daemon

import (
	"fmt"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/config"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/plugins"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/server"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"os"
)

// Daemon is a struct to identify main instance of cdnNode.
type Daemon struct {
	Name   string
	config *config.Config
	server *server.Server
}

// New creates a new Daemon.
func New(cfg *config.Config) (*Daemon, error) {
	if err := plugins.Initialize(cfg); err != nil {
		return nil, err
	}
	s, err := server.New(cfg, prometheus.DefaultRegisterer)
	if err != nil {
		return nil, err
	}
	return &Daemon{
		Name: fmt.Sprint("CDN:",os.Getpid()),
		config: cfg,
		server: s,
	}, nil
}

// Run runs the daemon.
func (d *Daemon) Run() error {
	if err := d.server.Start(); err != nil {
		logrus.Errorf("failed to start cdn system %s : %v", d.Name, err)
		return err
	}
	logrus.Infof("start cdn system %s successfully", d.Name)
	return nil
}
