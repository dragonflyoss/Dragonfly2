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

	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/internal/dynconfig"
	"d7y.io/dragonfly/v2/internal/rpc"
	"d7y.io/dragonfly/v2/internal/rpc/manager"
	"d7y.io/dragonfly/v2/internal/rpc/manager/client"
	"d7y.io/dragonfly/v2/internal/rpc/scheduler/server"
	"d7y.io/dragonfly/v2/scheduler/server/service"

	// Server registered to grpc
	_ "d7y.io/dragonfly/v2/internal/rpc/scheduler/server"
	"d7y.io/dragonfly/v2/pkg/util/net/iputils"
	"d7y.io/dragonfly/v2/scheduler/config"
)

type Server struct {
	config          *config.ServerConfig
	schedulerServer server.SchedulerServer
	managerClient   client.ManagerClient
	running         bool
	dynConfig       config.DynconfigInterface
}

func New(cfg *config.Config) (*Server, error) {
	s := &Server{
		running: false,
		config:  cfg.Server,
	}
	// Initialize manager client
	//if cfg.Manager != nil {
	//	managerClient, err := client.NewClient(cfg.Manager.NetAddrs)
	//	if err != nil {
	//		return nil, errors.Wrapf(err, "create manager client")
	//	}
	//	s.managerClient = managerClient
	//}

	var options []dynconfig.Option
	if cfg.DynConfig.Type == dynconfig.LocalSourceType {
		options = []dynconfig.Option{
			dynconfig.WithLocalConfigPath(cfg.DynConfig.Path),
		}
	}

	if cfg.DynConfig.Type == dynconfig.ManagerSourceType {
		client, err := client.NewClient(cfg.DynConfig.NetAddrs)
		if err != nil {
			return nil, err
		}

		options = []dynconfig.Option{
			dynconfig.WithManagerClient(config.NewManagerClient(client)),
			dynconfig.WithCachePath(cfg.DynConfig.CachePath),
			dynconfig.WithExpireTime(cfg.DynConfig.ExpireTime),
		}
	}

	dynConfig, err := config.NewDynconfig(cfg.DynConfig.Type, cfg.DynConfig.CDNDirPath, options...)
	if err != nil {
		return nil, err
	}
	s.dynConfig = dynConfig

	// Initialize scheduler service
	s.schedulerServer, err = service.NewSchedulerServer(cfg.Scheduler, s.dynConfig)
	if err != nil {
		return nil, err
	}

	return s, nil
}

func (s *Server) Serve() error {
	port := s.config.Port
	s.running = true
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.dynConfig.Serve()

	if s.managerClient != nil {
		s.managerClient.KeepAlive(ctx, &manager.KeepAliveRequest{
			HostName: iputils.HostName,
			Type:     manager.ResourceType_Scheduler,
		})
		logger.Info("start scheduler keep alive")
	}

	logger.Infof("start server at port %d", port)
	if err := rpc.StartTCPServer(port, port, s.schedulerServer); err != nil {
		return err
	}
	return nil
}

func (s *Server) Stop() (err error) {
	if s.running {
		s.running = false
		s.dynConfig.Stop()
		rpc.StopServer()
	}
	return
}
