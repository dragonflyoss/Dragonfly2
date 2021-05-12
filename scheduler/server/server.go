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

	"d7y.io/dragonfly/v2/pkg/basic/dfnet"
	logger "d7y.io/dragonfly/v2/pkg/dflog"
	"d7y.io/dragonfly/v2/pkg/rpc"
	"d7y.io/dragonfly/v2/pkg/rpc/manager"
	configServer "d7y.io/dragonfly/v2/pkg/rpc/manager/client"
	_ "d7y.io/dragonfly/v2/pkg/rpc/scheduler/server"
	"d7y.io/dragonfly/v2/pkg/util/net/iputils"
	"d7y.io/dragonfly/v2/pkg/util/stringutils"
	"d7y.io/dragonfly/v2/scheduler/config"
	"d7y.io/dragonfly/v2/scheduler/service"
	"d7y.io/dragonfly/v2/scheduler/service/schedule_worker"
	"github.com/pkg/errors"
)

type Server struct {
	service      *service.SchedulerService
	worker       schedule_worker.IWorker
	server       *SchedulerServer
	config       config.ServerConfig
	configServer configServer.ManagerClient
	running      bool
}

func New(cfg *config.Config) (*Server, error) {
	s := &Server{
		running: false,
		config:  cfg.Server,
	}
	if !stringutils.IsBlank(cfg.ConfigServer) {
		cfgServer, err := configServer.NewClient([]dfnet.NetAddr{{
			Type: dfnet.TCP,
			Addr: cfg.ConfigServer,
		}})
		if err != nil {
			return nil, errors.Wrap(err, "failed to create config server")
		}
		s.configServer = cfgServer
	}
	s.service = service.NewSchedulerService(cfg, s.configServer)
	s.worker = schedule_worker.NewWorkerGroup(cfg, s.service)
	s.server = NewSchedulerServer(cfg, WithSchedulerService(s.service),
		WithWorker(s.worker))
	return s, nil
}

func (s *Server) Serve() (err error) {
	port := s.config.Port

	go s.worker.Serve()
	defer s.worker.Stop()

	s.running = true
	logger.Infof("start server at port %d", port)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if s.configServer != nil {
		logger.Info("start scheduler keep alive")
		s.configServer.KeepAlive(ctx, &manager.KeepAliveRequest{
			HostName: iputils.HostName,
			Type:     manager.ResourceType_Scheduler,
		})
	}
	err = rpc.StartTcpServer(port, port, s.server)
	return
}

func (s *Server) Stop() (err error) {
	if s.running {
		s.running = false
		rpc.StopServer()
	}
	return
}
