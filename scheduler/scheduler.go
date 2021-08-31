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

package scheduler

import (
	"context"

	"d7y.io/dragonfly/v2/cmd/dependency"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/internal/dynconfig"
	"d7y.io/dragonfly/v2/pkg/rpc"
	"d7y.io/dragonfly/v2/pkg/rpc/manager"
	managerclient "d7y.io/dragonfly/v2/pkg/rpc/manager/client"
	"d7y.io/dragonfly/v2/pkg/rpc/scheduler/server"
	"d7y.io/dragonfly/v2/pkg/util/net/iputils"
	"d7y.io/dragonfly/v2/scheduler/config"
	"d7y.io/dragonfly/v2/scheduler/core"
	"d7y.io/dragonfly/v2/scheduler/job"
	"d7y.io/dragonfly/v2/scheduler/rpcserver"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
)

type Server struct {
	config           *config.Config
	schedulerServer  server.SchedulerServer
	schedulerService *core.SchedulerService
	managerClient    managerclient.Client
	dynConfig        config.DynconfigInterface
	job              job.Job
}

func New(cfg *config.Config) (*Server, error) {
	s := &Server{config: cfg}

	// Initialize manager client
	if cfg.Manager.Addr != "" {
		managerClient, err := managerclient.New(cfg.Manager.Addr)
		if err != nil {
			return nil, err
		}
		s.managerClient = managerClient

		// Register to manager
		if _, err := s.managerClient.UpdateScheduler(&manager.UpdateSchedulerRequest{
			SourceType:         manager.SourceType_SCHEDULER_SOURCE,
			HostName:           s.config.Server.Host,
			Ip:                 s.config.Server.IP,
			Port:               int32(s.config.Server.Port),
			Idc:                s.config.Host.IDC,
			Location:           s.config.Host.Location,
			SchedulerClusterId: uint64(s.config.Manager.SchedulerClusterID),
		}); err != nil {
			return nil, err
		}
	}

	// Initialize dynconfig client
	options := []dynconfig.Option{dynconfig.WithLocalConfigPath(dependency.GetConfigPath("scheduler"))}
	if s.managerClient != nil && cfg.DynConfig.Type == dynconfig.ManagerSourceType {
		options = append(options,
			dynconfig.WithManagerClient(config.NewManagerClient(s.managerClient, cfg.Manager.SchedulerClusterID)),
			dynconfig.WithCachePath(config.DefaultDynconfigCachePath),
			dynconfig.WithExpireTime(cfg.DynConfig.ExpireTime),
		)
	}
	dynConfig, err := config.NewDynconfig(cfg.DynConfig.Type, cfg.DynConfig.CDNDirPath, options...)
	if err != nil {
		return nil, err
	}
	s.dynConfig = dynConfig

	// Initialize scheduler service
	var openTel bool
	if cfg.Options.Telemetry.Jaeger != "" {
		openTel = true
	}
	schedulerService, err := core.NewSchedulerService(cfg.Scheduler, dynConfig, core.WithDisableCDN(cfg.DisableCDN), core.WithOpenTel(openTel))
	if err != nil {
		return nil, err
	}
	s.schedulerService = schedulerService

	// Initialize grpc service
	schedulerServer, err := rpcserver.NewSchedulerServer(schedulerService)
	if err != nil {
		return nil, err
	}
	s.schedulerServer = schedulerServer

	// Initialize job service
	if cfg.Job.Redis.Host != "" {
		s.job, err = job.New(context.Background(), cfg.Job, iputils.HostName, s.schedulerService)
		if err != nil {
			return nil, err
		}
	}

	return s, nil
}

func (s *Server) Serve() error {
	// Serve dynConfig
	go func() {
		if err := s.dynConfig.Serve(); err != nil {
			logger.Fatalf("dynconfig start failed %v", err)
		}
		logger.Info("dynconfig start successfully")
	}()

	// Serve schedulerService
	go func() {
		s.schedulerService.Serve()
		logger.Info("scheduler service start successfully")
	}()

	// Serve Job
	if s.job != nil {
		go func() {
			if err := s.job.Serve(); err != nil {
				logger.Fatalf("job start failed %v", err)
			}
			logger.Info("job start successfully")
		}()
	}

	// Serve Keepalive
	if s.managerClient != nil {
		go func() {
			logger.Info("start keepalive to manager")
			s.managerClient.KeepAlive(s.config.Manager.KeepAlive.Interval, &manager.KeepAliveRequest{
				HostName:   s.config.Server.Host,
				SourceType: manager.SourceType_SCHEDULER_SOURCE,
				ClusterId:  uint64(s.config.Manager.SchedulerClusterID),
			})
		}()
	}

	// Serve GRPC
	logger.Infof("start server at port %d", s.config.Server.Port)
	var opts []grpc.ServerOption
	if s.config.Options.Telemetry.Jaeger != "" {
		opts = append(opts, grpc.ChainUnaryInterceptor(otelgrpc.UnaryServerInterceptor()), grpc.ChainStreamInterceptor(otelgrpc.StreamServerInterceptor()))
	}
	if err := rpc.StartTCPServer(
		s.config.Server.Port,
		s.config.Server.Port,
		s.schedulerServer,
		opts...,
	); err != nil {
		logger.Errorf("grpc start failed %v", err)
		return err
	}

	return nil
}

func (s *Server) Stop() {
	s.dynConfig.Stop()
	logger.Info("dynconfig client closed")

	if s.managerClient != nil {
		s.managerClient.Close()
		logger.Info("manager client closed")
	}

	s.schedulerService.Stop()
	logger.Info("scheduler service closed")

	rpc.StopServer()
	logger.Info("grpc server closed under request")
}
