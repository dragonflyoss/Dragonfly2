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

	logger "d7y.io/dragonfly/v2/pkg/dflog"
	"d7y.io/dragonfly/v2/pkg/rpc"
	"d7y.io/dragonfly/v2/pkg/rpc/manager"
	"d7y.io/dragonfly/v2/pkg/rpc/manager/client"
	_ "d7y.io/dragonfly/v2/pkg/rpc/scheduler/server"
	"d7y.io/dragonfly/v2/pkg/util/net/iputils"
	"d7y.io/dragonfly/v2/scheduler/config"
	"d7y.io/dragonfly/v2/scheduler/service"
	"d7y.io/dragonfly/v2/scheduler/service/schedule_worker"
)

type Server struct {
	service       *service.SchedulerService
	worker        schedule_worker.IWorker
	server        *SchedulerServer
	config        config.ServerConfig
	managerClient client.ManagerClient
	running       bool
}

func New(cfg *config.Config) (*Server, error) {
	var err error

	s := &Server{
		running: false,
		config:  cfg.Server,
	}

	// Initialize manager client
	s.managerClient, err = client.NewClient(cfg.Manager.NetAddrs)
	if err != nil {
		return nil, err
	}

	// Initialize dynconfig client
	dynconfig, err := config.NewDynconfig(s.managerClient, cfg.Manager.ExpireTime)
	if err != nil {
		return nil, err
	}

	// Initialize scheduler service
	s.service, err = service.NewSchedulerService(cfg, dynconfig)
	if err != nil {
		return nil, err
	}

	s.worker = schedule_worker.NewWorkerGroup(cfg, s.service)
	s.server = NewSchedulerServer(cfg, WithSchedulerService(s.service),
		WithWorker(s.worker))

	return s, nil
}

func (s *Server) Serve() error {
	port := s.config.Port
	s.running = true
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go s.worker.Serve()
	defer s.worker.Stop()

	logger.Infof("start server at port %d", port)
	if err := rpc.StartTcpServer(port, port, s.server); err != nil {
		return err
	}

	// Start keepalive
	logger.Info("start scheduler keep alive")
	s.managerClient.KeepAlive(ctx, &manager.KeepAliveRequest{
		HostName: iputils.HostName,
		Type:     manager.ResourceType_Scheduler,
	})

	return nil
}

func (s *Server) Stop() (err error) {
	if s.running {
		s.running = false
		rpc.StopServer()
	}
	return
}
