/*
 *     Copyright 2020 The Dragonfly Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// cdn server
package server

import (
	"context"
	"fmt"
	"runtime"
	"time"

	"d7y.io/dragonfly/v2/cdnsystem/config"
	"d7y.io/dragonfly/v2/cdnsystem/daemon/cdn"
	"d7y.io/dragonfly/v2/cdnsystem/daemon/cdn/storage"
	"d7y.io/dragonfly/v2/cdnsystem/daemon/gc"
	"d7y.io/dragonfly/v2/cdnsystem/daemon/progress"
	"d7y.io/dragonfly/v2/cdnsystem/daemon/task"
	"d7y.io/dragonfly/v2/cdnsystem/plugins"
	"d7y.io/dragonfly/v2/cdnsystem/server/service"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/internal/rpc"
	"d7y.io/dragonfly/v2/internal/rpc/cdnsystem/server"
	"d7y.io/dragonfly/v2/internal/rpc/manager"
	"d7y.io/dragonfly/v2/internal/rpc/manager/client"
	"d7y.io/dragonfly/v2/pkg/util/net/iputils"
	"github.com/pkg/errors"
)

type Server struct {
	Config        *config.Config
	seedServer    server.SeederServer
	managerClient client.ManagerClient
}

// New creates a brand new server instance.
func New(cfg *config.Config) (*Server, error) {
	if ok := storage.IsSupport(cfg.StorageMode); !ok {
		return nil, fmt.Errorf("os %s is not support storage mode %s", runtime.GOOS, cfg.StorageMode)
	}
	if err := plugins.Initialize(cfg.Plugins); err != nil {
		return nil, err
	}

	// progress manager
	progressMgr, err := progress.NewManager()
	if err != nil {
		return nil, errors.Wrapf(err, "create progress manager")
	}

	// storage manager
	storageMgr, ok := storage.Get(cfg.StorageMode)
	if !ok {
		return nil, fmt.Errorf("can not find storage pattern %s", cfg.StorageMode)
	}
	// cdn manager
	cdnMgr, err := cdn.NewManager(cfg, storageMgr, progressMgr)
	if err != nil {
		return nil, errors.Wrapf(err, "create cdn manager")
	}
	// task manager
	taskMgr, err := task.NewManager(cfg, cdnMgr, progressMgr)
	if err != nil {
		return nil, errors.Wrapf(err, "create task manager")
	}
	storageMgr.Initialize(taskMgr)
	// gc manager
	if err != nil {
		return nil, errors.Wrapf(err, "create gc manager")
	}

	cdnSeedServer, err := service.NewCdnSeedServer(cfg, taskMgr)
	if err != nil {
		return nil, errors.Wrap(err, "create seedServer")
	}

	// manager client
	var managerClient client.ManagerClient
	if len(cfg.Manager.NetAddrs) > 0 {
		managerClient, err = client.New(cfg.Manager.NetAddrs)
		if err != nil {
			return nil, errors.Wrap(err, "create manager service")
		}
	}

	return &Server{
		Config:        cfg,
		seedServer:    cdnSeedServer,
		managerClient: managerClient,
	}, nil
}

func (s *Server) Serve() (err error) {
	defer func() {
		if rec := recover(); rec != nil {
			err = errors.New(fmt.Sprintf("%v", rec))
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// start gc
	err = gc.StartGC(ctx)
	if err != nil {
		return err
	}

	if s.managerClient != nil {
		s.register(ctx)
		logger.Info("cdn register to manager")

		go s.keepAlive(ctx)
		logger.Info("start cdn keep alive")
	}

	err = rpc.StartTCPServer(s.Config.ListenPort, s.Config.ListenPort, s.seedServer)
	if err != nil {
		return errors.Wrap(err, "start tcp server")
	}
	return nil
}

func (s *Server) Stop() {
	rpc.StopServer()
}

func (s *Server) register(ctx context.Context) error {
	ip := s.Config.AdvertiseIP
	port := int32(s.Config.ListenPort)
	downloadPort := int32(s.Config.DownloadPort)
	if _, err := s.managerClient.CreateCDN(ctx, &manager.CreateCDNRequest{
		SourceType:   manager.SourceType_CDN_SOURCE,
		HostName:     iputils.HostName,
		Ip:           ip,
		Port:         port,
		DownloadPort: downloadPort,
	}); err != nil {
		if _, err := s.managerClient.UpdateCDN(ctx, &manager.UpdateCDNRequest{
			SourceType:   manager.SourceType_CDN_SOURCE,
			HostName:     iputils.HostName,
			Ip:           ip,
			Port:         port,
			DownloadPort: downloadPort,
		}); err != nil {
			return err
		}
	}

	return nil
}

func (s *Server) keepAlive(ctx context.Context) error {
	stream, err := s.managerClient.KeepAlive(ctx)
	if err != nil {
		logger.Errorf("create keepalive failed: %v\n", err)
		return err
	}

	tick := time.NewTicker(s.Config.Manager.KeepAliveInterval)
	hostName := iputils.HostName
	for {
		select {
		case <-tick.C:
			if err := stream.Send(&manager.KeepAliveRequest{
				HostName:   hostName,
				SourceType: manager.SourceType_CDN_SOURCE,
			}); err != nil {
				logger.Errorf("%s send keepalive failed: %v\n", hostName, err)
				return err
			}
		}
	}
}
