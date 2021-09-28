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
	"net/http"
	"time"

	"d7y.io/dragonfly/v2/cmd/dependency"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/internal/dynconfig"
	"d7y.io/dragonfly/v2/pkg/gc"
	"d7y.io/dragonfly/v2/pkg/rpc"
	"d7y.io/dragonfly/v2/pkg/rpc/manager"
	managerclient "d7y.io/dragonfly/v2/pkg/rpc/manager/client"
	"d7y.io/dragonfly/v2/pkg/util/net/iputils"
	"d7y.io/dragonfly/v2/scheduler/config"
	"d7y.io/dragonfly/v2/scheduler/core"
	"d7y.io/dragonfly/v2/scheduler/job"
	"d7y.io/dragonfly/v2/scheduler/metric"
	"d7y.io/dragonfly/v2/scheduler/rpcserver"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
)

const (
	gracefulStopTimeout = 10 * time.Second
)

type Server struct {
	// Server configuration
	config *config.Config

	// GRPC server
	grpcServer *grpc.Server

	// Metric server
	metricServer *http.Server

	// Scheduler service
	service *core.SchedulerService

	// Manager client
	managerClient managerclient.Client

	// Dynamic config
	dynConfig config.DynconfigInterface

	// Async job
	job job.Job

	// GC server
	gc gc.GC
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

	// Initialize GC
	s.gc = gc.New(gc.WithLogger(logger.GcLogger))

	// Initialize scheduler service
	var openTel bool
	if cfg.Options.Telemetry.Jaeger != "" {
		openTel = true
	}
	service, err := core.NewSchedulerService(cfg.Scheduler, dynConfig, s.gc, core.WithDisableCDN(cfg.DisableCDN), core.WithOpenTel(openTel))
	if err != nil {
		return nil, err
	}
	s.service = service

	// Initialize grpc service
	var opts []grpc.ServerOption
	if s.config.Options.Telemetry.Jaeger != "" {
		opts = append(opts, grpc.ChainUnaryInterceptor(otelgrpc.UnaryServerInterceptor()), grpc.ChainStreamInterceptor(otelgrpc.StreamServerInterceptor()))
	}
	grpcServer, err := rpcserver.New(s.service, opts...)
	if err != nil {
		return nil, err
	}
	s.grpcServer = grpcServer

	// Initialize prometheus
	if cfg.Metric != nil {
		s.metricServer = metric.New(cfg.Metric, grpcServer)
	}

	// Initialize job service
	if cfg.Job.Redis.Host != "" {
		s.job, err = job.New(context.Background(), cfg.Job, iputils.HostName, s.service)
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

	// Serve GC
	s.gc.Serve()
	logger.Info("gc start successfully")

	// Serve service
	go func() {
		s.service.Serve()
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

	// Started metric server
	if s.metricServer != nil {
		go func() {
			logger.Infof("started metric server at %s", s.metricServer.Addr)
			if err := s.metricServer.ListenAndServe(); err != nil {
				if err == http.ErrServerClosed {
					return
				}
				logger.Fatalf("metric server closed unexpect: %+v", err)
			}
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

	// Generate GRPC listener
	lis, _, err := rpc.ListenWithPortRange(s.config.Server.IP, s.config.Server.Port, s.config.Server.Port)
	if err != nil {
		logger.Fatalf("net listener failed to start: %+v", err)
	}
	defer lis.Close()

	// Started GRPC server
	logger.Infof("started grpc server at %s://%s", lis.Addr().Network(), lis.Addr().String())
	if err := s.grpcServer.Serve(lis); err != nil {
		logger.Errorf("stoped grpc server: %+v", err)
		return err
	}

	return nil
}

func (s *Server) Stop() {
	// Stop dynamic server
	s.dynConfig.Stop()
	logger.Info("dynconfig client closed")

	// Stop manager client
	if s.managerClient != nil {
		if err := s.managerClient.Close(); err != nil {
			logger.Errorf("manager client failed to stop: %+v", err)
		}
		logger.Info("manager client closed")
	}

	// Stop GC
	s.gc.Stop()
	logger.Info("gc closed")

	// Stop scheduler service
	s.service.Stop()
	logger.Info("scheduler service closed")

	// Stop metric server
	if s.metricServer != nil {
		if err := s.metricServer.Shutdown(context.Background()); err != nil {
			logger.Errorf("metric server failed to stop: %+v", err)
		}
		logger.Info("metric server closed under request")
	}

	// Stop GRPC server
	stopped := make(chan struct{})
	go func() {
		s.grpcServer.GracefulStop()
		logger.Info("grpc server closed under request")
		close(stopped)
	}()

	t := time.NewTimer(gracefulStopTimeout)
	select {
	case <-t.C:
		s.grpcServer.Stop()
	case <-stopped:
		t.Stop()
	}
}
