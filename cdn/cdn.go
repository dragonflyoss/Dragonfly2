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

package cdn

import (
	"context"
	"fmt"
	"runtime"

	"github.com/pkg/errors"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"

	"d7y.io/dragonfly/v2/cdn/config"
	"d7y.io/dragonfly/v2/cdn/gc"
	"d7y.io/dragonfly/v2/cdn/metrics"
	"d7y.io/dragonfly/v2/cdn/plugins"
	"d7y.io/dragonfly/v2/cdn/rpcserver"
	"d7y.io/dragonfly/v2/cdn/supervisor"
	"d7y.io/dragonfly/v2/cdn/supervisor/cdn"
	"d7y.io/dragonfly/v2/cdn/supervisor/cdn/storage"
	"d7y.io/dragonfly/v2/cdn/supervisor/progress"
	"d7y.io/dragonfly/v2/cdn/supervisor/task"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/rpc/manager"
	managerClient "d7y.io/dragonfly/v2/pkg/rpc/manager/client"
	"d7y.io/dragonfly/v2/pkg/util/hostutils"
)

type Server struct {
	// Server configuration
	config *config.Config

	// GRPC server
	grpcServer *rpcserver.Server

	// Metrics server
	metricsServer *metrics.Server

	// Manager client
	configServer managerClient.Client

	// gc Server
	gcServer *gc.Server
}

// New creates a brand-new server instance.
func New(cfg *config.Config) (*Server, error) {
	if ok := storage.IsSupport(cfg.StorageMode); !ok {
		return nil, fmt.Errorf("os %s is not support storage mode %s", runtime.GOOS, cfg.StorageMode)
	}

	// Initialize plugins
	if err := plugins.Initialize(cfg.Plugins); err != nil {
		return nil, errors.Wrapf(err, "init plugins")
	}

	// Initialize task manager
	taskManager, err := task.NewManager(task.Config{
		GCInitialDelay:     cfg.GCInitialDelay,
		GCMetaInterval:     cfg.GCMetaInterval,
		TaskExpireTime:     cfg.TaskExpireTime,
		FailAccessInterval: cfg.FailAccessInterval,
	})
	if err != nil {
		return nil, errors.Wrapf(err, "create task manager")
	}

	// Initialize progress manager
	progressManager, err := progress.NewManager(taskManager)
	if err != nil {
		return nil, errors.Wrapf(err, "create progress manager")
	}

	// Initialize storage manager
	storageManagerBuilder := storage.Get(cfg.StorageMode)
	if storageManagerBuilder == nil {
		return nil, fmt.Errorf("can not find storage manager mode %s", cfg.StorageMode)
	}
	storageManager, err := storageManagerBuilder.Build(config.NewStorage(cfg.StorageMode), taskManager)
	if err != nil {
		return nil, errors.Wrapf(err, "create storage manager")
	}

	// Initialize CDN manager
	cdnManager, err := cdn.NewManager(cdn.Config{
		SystemReservedBandwidth: cfg.SystemReservedBandwidth,
		MaxBandwidth:            cfg.MaxBandwidth,
	}, storageManager, progressManager, taskManager)
	if err != nil {
		return nil, errors.Wrapf(err, "create cdn manager")
	}

	// Initialize CDN service
	service, err := supervisor.NewCDNService(taskManager, cdnManager, progressManager)
	if err != nil {
		return nil, errors.Wrapf(err, "create cdn service")
	}
	// Initialize storage manager
	var opts []grpc.ServerOption
	if cfg.Options.Telemetry.Jaeger != "" {
		opts = append(opts, grpc.ChainUnaryInterceptor(otelgrpc.UnaryServerInterceptor()), grpc.ChainStreamInterceptor(otelgrpc.StreamServerInterceptor()))
	}
	grpcServer, err := rpcserver.New(rpcserver.Config{
		AdvertiseIP:  cfg.AdvertiseIP,
		ListenPort:   cfg.ListenPort,
		DownloadPort: cfg.DownloadPort,
	}, service, opts...)
	if err != nil {
		return nil, errors.Wrap(err, "create rpcServer")
	}

	// Initialize gc server
	gcServer, err := gc.New(gc.Config{})
	if err != nil {
		return nil, errors.Wrap(err, "create gcServer")
	}

	// Initialize metrics server
	metricsServer, err := metrics.New(cfg.Metrics, grpcServer)
	if err != nil {
		return nil, errors.Wrap(err, "create metricsServer")
	}

	// Initialize configServer
	var configServer managerClient.Client
	if cfg.Manager.Addr != "" {
		configServer, err = managerClient.New(cfg.Manager.Addr)
		if err != nil {
			return nil, errors.Wrap(err, "create configServer")
		}
	}
	return &Server{
		config:        cfg,
		grpcServer:    grpcServer,
		metricsServer: metricsServer,
		configServer:  configServer,
		gcServer:      gcServer,
	}, nil
}

func (s *Server) Serve() error {
	go func() {
		// Start GC
		if err := s.gcServer.Serve(); err != nil {
			logger.Fatalf("start gc task failed: %v", err)
		}
	}()

	go func() {
		// Start metrics server
		if err := s.metricsServer.ListenAndServe(s.metricsServer.Handler()); err != nil {
			logger.Fatalf("start metrics server failed: %v", err)
		}
	}()

	go func() {
		if s.configServer != nil {
			var rpcServerConfig = s.grpcServer.GetConfig()
			CDNInstance, err := s.configServer.UpdateCDN(&manager.UpdateCDNRequest{
				SourceType:   manager.SourceType_CDN_SOURCE,
				HostName:     hostutils.FQDNHostname,
				Ip:           rpcServerConfig.AdvertiseIP,
				Port:         int32(rpcServerConfig.ListenPort),
				DownloadPort: int32(rpcServerConfig.DownloadPort),
				Idc:          s.config.Host.IDC,
				Location:     s.config.Host.Location,
				CdnClusterId: uint64(s.config.Manager.CDNClusterID),
			})
			if err != nil {
				logger.Fatalf("update cdn instance failed: %v", err)
			}
			// Serve Keepalive
			logger.Infof("====starting keepalive cdn instance %#v to manager %s====", CDNInstance)
			s.configServer.KeepAlive(s.config.Manager.KeepAlive.Interval, &manager.KeepAliveRequest{
				HostName:   hostutils.FQDNHostname,
				SourceType: manager.SourceType_CDN_SOURCE,
				ClusterId:  uint64(s.config.Manager.CDNClusterID),
			})
		}
	}()

	// Start grpc server
	return s.grpcServer.ListenAndServe()
}

func (s *Server) Stop() error {
	g, ctx := errgroup.WithContext(context.Background())

	g.Go(func() error {
		return s.gcServer.Shutdown()
	})

	if s.configServer != nil {
		// Stop manager client
		g.Go(func() error {
			return s.configServer.Close()
		})
	}
	g.Go(func() error {
		// Stop metrics server
		return s.metricsServer.Shutdown(ctx)
	})

	g.Go(func() error {
		// Stop grpc server
		return s.grpcServer.Shutdown()
	})
	return g.Wait()
}
