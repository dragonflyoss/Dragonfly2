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

package server

import (
	"context"
	"time"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/internal/dynconfig"
	"d7y.io/dragonfly/v2/internal/rpc"
	"d7y.io/dragonfly/v2/internal/rpc/manager"
	"d7y.io/dragonfly/v2/internal/rpc/manager/client"

	// Server registered to grpc
	_ "d7y.io/dragonfly/v2/internal/rpc/scheduler/server"
	"d7y.io/dragonfly/v2/pkg/util/net/iputils"
	"d7y.io/dragonfly/v2/scheduler/config"
	"d7y.io/dragonfly/v2/scheduler/service"
	"d7y.io/dragonfly/v2/scheduler/service/worker"
)

type Server struct {
	service       *service.SchedulerService
	worker        worker.IWorker
	server        *SchedulerServer
	config        config.Config
	managerClient client.ManagerClient
	running       bool
	dynconfig     config.DynconfigInterface
}

func New(cfg *config.Config) (*Server, error) {
	var err error

	s := &Server{
		running: false,
		config:  cfg,
	}

	// Initialize manager client
	if len(cfg.Manager.NetAddrs) > 0 {
		managerClient, err := client.New(cfg.Manager.NetAddrs)
		if err != nil {
			return nil, err
		}
		s.managerClient = managerClient
	}

	// Initialize dynconfig client
	options := []dynconfig.Option{}
	if cfg.Dynconfig.Type == dynconfig.LocalSourceType {
		options = []dynconfig.Option{
			dynconfig.WithLocalConfigPath(cfg.Dynconfig.Path),
		}
	}

	if cfg.Dynconfig.Type == dynconfig.ManagerSourceType {
		client, err := client.New(cfg.Dynconfig.NetAddrs)
		if err != nil {
			return nil, err
		}

		options = []dynconfig.Option{
			dynconfig.WithManagerClient(config.NewManagerClient(client)),
			dynconfig.WithCachePath(cfg.Dynconfig.CachePath),
			dynconfig.WithExpireTime(cfg.Dynconfig.ExpireTime),
		}
	}

	dynconfig, err := config.NewDynconfig(cfg.Dynconfig.Type, cfg.Dynconfig.CDNDirPath, options...)
	if err != nil {
		return nil, err
	}
	s.dynconfig = dynconfig

	// Initialize scheduler service
	service, err := service.NewSchedulerService(cfg, s.dynconfig)
	if err != nil {
		return nil, err
	}
	s.service = service

	s.worker = worker.NewGroup(cfg, s.service)
	s.server = NewSchedulerServer(cfg, WithSchedulerService(s.service),
		WithWorker(s.worker))

	return s, nil
}

func (s *Server) Serve() error {
	port := s.config.Server.Port
	s.running = true
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.dynconfig.Serve()
	go s.worker.Serve()
	defer s.worker.Stop()

	if s.managerClient != nil {
		s.register(ctx)
		logger.Info("scheduler register to manager")

		go s.keepAlive(ctx)
		logger.Info("start scheduler keep alive")
	}

	logger.Infof("start server at port %d", port)
	if err := rpc.StartTCPServer(port, port, s.server); err != nil {
		return err
	}
	return nil
}

func (s *Server) Stop() (err error) {
	if s.running {
		s.running = false
		s.dynconfig.Stop()
		rpc.StopServer()
	}
	return
}

func (s *Server) register(ctx context.Context) error {
	ip := s.config.Server.IP
	port := int32(s.config.Server.Port)

	if _, err := s.managerClient.CreateScheduler(ctx, &manager.CreateSchedulerRequest{
		SourceType: manager.SourceType_SCHEDULER_SOURCE,
		HostName:   iputils.HostName,
		Ip:         ip,
		Port:       port,
	}); err != nil {
		if _, err := s.managerClient.UpdateScheduler(ctx, &manager.UpdateSchedulerRequest{
			SourceType: manager.SourceType_SCHEDULER_SOURCE,
			HostName:   iputils.HostName,
			Ip:         ip,
			Port:       port,
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

	tick := time.NewTicker(s.config.Manager.KeepAliveInterval)
	hostName := iputils.HostName
	for {
		select {
		case <-tick.C:
			if err := stream.Send(&manager.KeepAliveRequest{
				HostName:   hostName,
				SourceType: manager.SourceType_SCHEDULER_SOURCE,
			}); err != nil {
				logger.Errorf("%s send keepalive failed: %v\n", hostName, err)
				return err
			}
		}
	}
}
