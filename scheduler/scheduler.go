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
	"time"

	"d7y.io/dragonfly/v2/scheduler/job"
	"d7y.io/dragonfly/v2/scheduler/rpcserver"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"

	"d7y.io/dragonfly/v2/cmd/dependency"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/internal/dynconfig"
	"d7y.io/dragonfly/v2/pkg/rpc"
	"d7y.io/dragonfly/v2/pkg/rpc/manager"
	"d7y.io/dragonfly/v2/pkg/rpc/scheduler/server"
	"d7y.io/dragonfly/v2/scheduler/core"
	"github.com/pkg/errors"
	"google.golang.org/grpc"

	// Server registered to grpc
	"d7y.io/dragonfly/v2/pkg/retry"
	"d7y.io/dragonfly/v2/pkg/util/net/iputils"
	"d7y.io/dragonfly/v2/scheduler/config"
)

type Server struct {
	config           *config.Config
	schedulerServer  server.SchedulerServer
	schedulerService *core.SchedulerService
	managerClient    manager.ManagerClient
	managerConn      *grpc.ClientConn
	dynconfigConn    *grpc.ClientConn
	running          bool
	dynConfig        config.DynconfigInterface
	job              job.Job
}

func New(cfg *config.Config) (*Server, error) {
	var err error
	s := &Server{
		running: false,
		config:  cfg,
	}

	// Initialize manager client
	options := []dynconfig.Option{dynconfig.WithLocalConfigPath(dependency.GetConfigPath("scheduler"))}
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

		if cfg.DynConfig.Type == dynconfig.ManagerSourceType {
			options = append(options,
				dynconfig.WithManagerClient(config.NewManagerClient(s.managerClient, cfg.Manager.SchedulerClusterID)),
				dynconfig.WithCachePath(config.DefaultDynconfigCachePath),
				dynconfig.WithExpireTime(cfg.DynConfig.ExpireTime),
			)
		}
	}

	dynConfig, err := config.NewDynconfig(cfg.DynConfig.Type, cfg.DynConfig.CDNDirPath, options...)
	if err != nil {
		return nil, errors.Wrap(err, "create dynamic config")
	}
	s.dynConfig = dynConfig

	schedulerService, err := core.NewSchedulerService(cfg.Scheduler, dynConfig)
	if err != nil {
		return nil, errors.Wrap(err, "create scheduler service")
	}
	s.schedulerService = schedulerService
	// Initialize scheduler service
	s.schedulerServer, err = rpcserver.NewSchedulerServer(schedulerService)

	if err != nil {
		return nil, err
	}
	if cfg.Job.Redis.Host != "" {
		s.job, err = job.New(context.Background(), cfg.Job, iputils.HostName, s.schedulerService)
		if err != nil {
			return nil, err
		}
	}

	return s, nil
}

func (s *Server) Serve() error {
	port := s.config.Server.Port
	s.running = true
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.dynConfig.Serve()
	s.schedulerService.Serve()

	if s.job != nil {
		go s.job.Serve()
	}

	if s.managerClient != nil {
		go retry.Run(ctx, func() (interface{}, bool, error) {
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
	var opts []grpc.ServerOption
	if s.config.Options.Telemetry.Jaeger != "" {
		opts = append(opts, grpc.ChainUnaryInterceptor(otelgrpc.UnaryServerInterceptor()), grpc.ChainStreamInterceptor(otelgrpc.StreamServerInterceptor()))
	}
	if err := rpc.StartTCPServer(port, port, s.schedulerServer, opts...); err != nil {
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
		s.schedulerService.Stop()
		s.job.Stop()
	}
	return
}

func (s *Server) register(ctx context.Context) error {
	ip := s.config.Server.IP
	host := s.config.Server.Host
	port := int32(s.config.Server.Port)
	idc := s.config.Host.IDC
	location := s.config.Host.Location
	schedulerClusterID := uint64(s.config.Manager.SchedulerClusterID)

	var scheduler *manager.Scheduler
	var err error
	scheduler, err = s.managerClient.UpdateScheduler(ctx, &manager.UpdateSchedulerRequest{
		SourceType:         manager.SourceType_SCHEDULER_SOURCE,
		HostName:           host,
		Ip:                 ip,
		Port:               port,
		Idc:                idc,
		Location:           location,
		SchedulerClusterId: schedulerClusterID,
	})
	if err != nil {
		logger.Warnf("update scheduler %s to manager failed %v", scheduler.HostName, err)
		return err
	}
	logger.Infof("update scheduler %s to manager successfully", scheduler.HostName)

	return nil
}

func (s *Server) keepAlive(ctx context.Context) error {
	schedulerClusterID := uint64(s.config.Manager.SchedulerClusterID)
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
				ClusterId:  schedulerClusterID,
			}); err != nil {
				logger.Errorf("%s send keepalive failed: %v\n", hostName, err)
				return err
			}
		}
	}
}
