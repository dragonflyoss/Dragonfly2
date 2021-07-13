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
	"d7y.io/dragonfly/v2/pkg/rpc"
	"d7y.io/dragonfly/v2/pkg/rpc/manager"
	"d7y.io/dragonfly/v2/pkg/rpc/scheduler/server"
	"d7y.io/dragonfly/v2/scheduler/server/service"
	"google.golang.org/grpc"

	// Server registered to grpc
	"d7y.io/dragonfly/v2/pkg/retry"
	"d7y.io/dragonfly/v2/pkg/util/net/iputils"
	"d7y.io/dragonfly/v2/scheduler/config"
)

type Server struct {
	config          *config.Config
	schedulerServer server.SchedulerServer
	managerClient   manager.ManagerClient
	managerConn     *grpc.ClientConn
	dynconfigConn   *grpc.ClientConn
	running         bool
	dynConfig       config.DynconfigInterface
}

func New(cfg *config.Config) (*Server, error) {
	var err error
	s := &Server{
		running: false,
		config:  cfg,
	}

	// Initialize manager client
	var managerConn *grpc.ClientConn
	if cfg.Manager.Addr != "" {
		managerConn, err = grpc.Dial(
			cfg.Manager.Addr,
			grpc.WithInsecure(),
			grpc.WithBlock(),
		)
		if err != nil {
			logger.Errorf("did not connect: %v", err)
			return nil, err
		}
		s.managerConn = managerConn
		s.managerClient = manager.NewManagerClient(managerConn)

		// Register to manager
		if err := s.register(context.Background()); err != nil {
			return nil, err
		}
		logger.Info("scheduler register to manager")
	}

	// Initialize dynconfig client
	options := []dynconfig.Option{}
	if cfg.DynConfig.Type == dynconfig.LocalSourceType {
		options = []dynconfig.Option{
			dynconfig.WithLocalConfigPath(cfg.DynConfig.Path),
		}
	}
	if cfg.DynConfig.Type == dynconfig.ManagerSourceType {
		dynconfigConn, err := grpc.Dial(
			cfg.DynConfig.Addr,
			grpc.WithInsecure(),
			grpc.WithBlock(),
		)
		if err != nil {
			logger.Errorf("did not connect: %v", err)
			return nil, err
		}
		s.dynconfigConn = dynconfigConn
		options = []dynconfig.Option{
			dynconfig.WithManagerClient(config.NewManagerClient(manager.NewManagerClient(dynconfigConn))),
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
	port := s.config.Server.Port
	s.running = true
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.dynConfig.Serve()

	if s.managerClient != nil {
		retry.Run(ctx, func() (interface{}, bool, error) {
			if err := s.keepAlive(ctx); err != nil {
				logger.Errorf("keepalive to manager failed %v", err)
				return nil, false, err
			}
			return nil, false, nil
		},
			s.config.Manager.KeepAlive.RetryInitBackOff,
			s.config.Manager.KeepAlive.RetryMaxBackOff,
			s.config.Manager.KeepAlive.RetryMaxAttempts,
			nil,
		)
	}

	logger.Infof("start server at port %d", port)
	if err := rpc.StartTCPServer(port, port, s.schedulerServer); err != nil {
		return err
	}
	return nil
}

func (s *Server) Stop() (err error) {
	if s.running {
		s.dynconfigConn.Close()
		s.managerConn.Close()
		s.running = false
		s.dynConfig.Stop()
		rpc.StopServer()
		s.dynConfig.Stop()
	}
	return
}

func (s *Server) register(ctx context.Context) error {
	ip := s.config.Server.IP
	port := int32(s.config.Server.Port)

	var scheduler *manager.Scheduler
	var err error
	scheduler, err = s.managerClient.CreateScheduler(ctx, &manager.CreateSchedulerRequest{
		SourceType: manager.SourceType_SCHEDULER_SOURCE,
		HostName:   iputils.HostName,
		Ip:         ip,
		Port:       port,
	})
	if err != nil {
		scheduler, err = s.managerClient.UpdateScheduler(ctx, &manager.UpdateSchedulerRequest{
			SourceType: manager.SourceType_SCHEDULER_SOURCE,
			HostName:   iputils.HostName,
			Ip:         ip,
			Port:       port,
		})
		if err != nil {
			logger.Warnf("update scheduler to manager failed %v", err)
			return err
		}
		logger.Infof("update scheduler %s successfully", scheduler.HostName)
	}
	logger.Infof("create scheduler %s successfully", scheduler.HostName)

	schedulerClusterID := s.config.Manager.SchedulerClusterID
	if schedulerClusterID != 0 {
		if _, err := s.managerClient.AddSchedulerClusterToSchedulerCluster(ctx, &manager.AddSchedulerClusterToSchedulerClusterRequest{
			SchedulerId:        scheduler.Id,
			SchedulerClusterId: schedulerClusterID,
		}); err != nil {
			logger.Warnf("add scheduler to scheduler cluster failed %v", err)
			return err
		}
		logger.Infof("add scheduler %s to scheduler cluster %s successfully", scheduler.HostName, schedulerClusterID)
	}
	return nil
}

func (s *Server) keepAlive(ctx context.Context) error {
	stream, err := s.managerClient.KeepAlive(ctx)
	if err != nil {
		logger.Errorf("create keepalive failed: %v\n", err)
		return err
	}

	tick := time.NewTicker(s.config.Manager.KeepAlive.Interval)
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
