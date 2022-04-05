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
	"os"

	"github.com/pkg/errors"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"gopkg.in/yaml.v3"

	"d7y.io/dragonfly/v2/cdn/config"
	"d7y.io/dragonfly/v2/cdn/dynconfig"
	"d7y.io/dragonfly/v2/cdn/gc"
	"d7y.io/dragonfly/v2/cdn/metrics"
	"d7y.io/dragonfly/v2/cdn/rpcserver"
	"d7y.io/dragonfly/v2/cdn/supervisor"
	"d7y.io/dragonfly/v2/cdn/supervisor/cdn"
	"d7y.io/dragonfly/v2/cdn/supervisor/cdn/storage"
	"d7y.io/dragonfly/v2/cdn/supervisor/progress"
	"d7y.io/dragonfly/v2/cdn/supervisor/task"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	dc "d7y.io/dragonfly/v2/internal/dynconfig"
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

	// dynamic config
	d dynconfig.Interface

	// gc Server
	gcServer *gc.Server
}

// New creates a brand-new server instance.
func New(cfg *config.Config) (*Server, error) {
	// Initialize configServer
	configServer, err := managerClient.New(cfg.Manager.Addr)
	if err != nil {
		return nil, errors.Wrap(err, "create configServer")
	}
	cdnInstance, err := configServer.UpdateCDN(&manager.UpdateCDNRequest{
		SourceType:   manager.SourceType_CDN_SOURCE,
		HostName:     hostutils.FQDNHostname,
		Ip:           cfg.RPCServer.AdvertiseIP,
		Port:         int32(cfg.RPCServer.ListenPort),
		DownloadPort: int32(cfg.RPCServer.DownloadPort),
		Idc:          cfg.Host.IDC,
		Location:     cfg.Host.Location,
		CdnClusterId: uint64(cfg.Manager.CDNClusterID),
	})
	if err != nil {
		return nil, errors.Wrap(err, "update cdn instance")
	}
	logger.Info("success update cdn instance: %s", cdnInstance)
	dynamicConfig, err := initDynamicConfig(configServer, cfg)
	if err != nil {
		return nil, errors.Wrap(err, "create dynamic config")
	}
	// Initialize task manager
	taskManager, err := task.NewManager(cfg.Task)
	if err != nil {
		return nil, errors.Wrapf(err, "create task manager")
	}

	notifyScheduler, err := task.NewNotifySchedulerTaskGCSubscriber(dynamicConfig)
	if err != nil {
		return nil, errors.Wrapf(err, "create notify scheduler task gc subscriber")
	}
	taskManager.GCSubscribe(notifyScheduler)
	// Initialize progress manager
	progressManager, err := progress.NewManager(taskManager)
	if err != nil {
		return nil, errors.Wrapf(err, "create progress manager")
	}

	// Initialize storage manager
	storageManager, err := storage.NewManager(cfg.Storage, taskManager)
	if err != nil {
		return nil, errors.Wrapf(err, "create storage manager")
	}

	// Initialize CDN manager
	cdnManager, err := cdn.NewManager(cfg.CDN, storageManager, progressManager, taskManager)
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
	grpcServer, err := rpcserver.New(cfg.RPCServer, service, opts...)
	if err != nil {
		return nil, errors.Wrap(err, "create rpcServer")
	}

	// Initialize gc server
	gcServer, err := gc.New()
	if err != nil {
		return nil, errors.Wrap(err, "create gcServer")
	}

	var metricsServer *metrics.Server
	if cfg.Metrics.Addr != "" {
		// Initialize metrics server
		metricsServer, err = metrics.New(cfg.Metrics, grpcServer.Server)
		if err != nil {
			return nil, errors.Wrap(err, "create metricsServer")
		}
	}
	return &Server{
		config:        cfg,
		grpcServer:    grpcServer,
		metricsServer: metricsServer,
		configServer:  configServer,
		d:             dynamicConfig,
		gcServer:      gcServer,
	}, nil
}

func initDynamicConfig(configServer managerClient.Client, cfg *config.Config) (dynamicConfig dynconfig.Interface, err error) {
	// Initialize dynconfig client
	if cfg.DynConfig.SourceType == dc.ManagerSourceType {
		dynamicConfig, err = dynconfig.NewDynconfig(cfg.DynConfig, func() (interface{}, error) {
			cdn, err := configServer.GetCDN(&manager.GetCDNRequest{
				HostName:     cfg.Host.Hostname,
				SourceType:   manager.SourceType_SCHEDULER_SOURCE,
				CdnClusterId: uint64(cfg.Manager.CDNClusterID),
			})
			if err != nil {
				return nil, err
			}
			return cdn, nil
		})
		return
	}
	dynamicConfig, err = dynconfig.NewDynconfig(cfg.DynConfig, func() (interface{}, error) {
		b, err := os.ReadFile(cfg.DynConfig.CachePath)
		if err != nil {
			return nil, err
		}
		cdn := new(manager.CDN)
		if err = yaml.Unmarshal(b, cdn); err != nil {
			return nil, err
		}
		return cdn, nil
	})
	return
}

func (s *Server) Serve() error {
	go func() {
		// Start GC
		if err := s.gcServer.Serve(); err != nil {
			logger.Fatalf("start gc task failed: %v", err)
		}
	}()

	go func() {
		if s.metricsServer != nil {
			// Start metrics server
			if err := s.metricsServer.ListenAndServe(s.metricsServer.Handler()); err != nil {
				logger.Fatalf("start metrics server failed: %v", err)
			}
		}
	}()

	go func() {
		if s.configServer != nil {
			// Serve Keepalive
			logger.Infof("====starting keepalive cdn instance to manager %s====", s.config.Manager.Addr)
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
	if s.d != nil {
		g.Go(func() error {
			s.d.Stop()
			return nil
		})
	}
	return g.Wait()
}
