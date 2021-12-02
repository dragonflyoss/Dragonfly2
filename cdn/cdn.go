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
	"net/http"
	"runtime"
	"time"

	"github.com/pkg/errors"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"

	"d7y.io/dragonfly/v2/cdn/config"
	"d7y.io/dragonfly/v2/cdn/metrics"
	"d7y.io/dragonfly/v2/cdn/plugins"
	"d7y.io/dragonfly/v2/cdn/rpcserver"
	"d7y.io/dragonfly/v2/cdn/supervisor/cdn"
	"d7y.io/dragonfly/v2/cdn/supervisor/cdn/storage"
	"d7y.io/dragonfly/v2/cdn/supervisor/gc"
	"d7y.io/dragonfly/v2/cdn/supervisor/progress"
	"d7y.io/dragonfly/v2/cdn/supervisor/task"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/rpc"
	"d7y.io/dragonfly/v2/pkg/rpc/manager"
	managerclient "d7y.io/dragonfly/v2/pkg/rpc/manager/client"
	"d7y.io/dragonfly/v2/pkg/util/hostutils"
	"d7y.io/dragonfly/v2/pkg/util/net/iputils"
)

const (
	gracefulStopTimeout = 10 * time.Second
)

type Server struct {
	// Server configuration
	config *config.Config

	// GRPC server
	grpcServer *grpc.Server

	// Metrics server
	metricsServer *http.Server

	// Manager client
	managerClient managerclient.Client
}

// New creates a brand new server instance.
func New(cfg *config.Config) (*Server, error) {
	s := &Server{config: cfg}

	if ok := storage.IsSupport(cfg.StorageMode); !ok {
		return nil, fmt.Errorf("os %s is not support storage mode %s", runtime.GOOS, cfg.StorageMode)
	}
	if err := plugins.Initialize(cfg.Plugins); err != nil {
		return nil, err
	}

	// Initialize progress manager
	progressMgr, err := progress.NewManager()
	if err != nil {
		return nil, errors.Wrapf(err, "create progress manager")
	}

	// Initialize storage manager
	storageMgr, ok := storage.Get(cfg.StorageMode)
	if !ok {
		return nil, fmt.Errorf("can not find storage pattern %s", cfg.StorageMode)
	}

	// Initialize CDN manager
	cdnMgr, err := cdn.NewManager(cfg, storageMgr, progressMgr)
	if err != nil {
		return nil, errors.Wrapf(err, "create cdn manager")
	}

	// Initialize task manager
	taskMgr, err := task.NewManager(cfg, cdnMgr, progressMgr)
	if err != nil {
		return nil, errors.Wrapf(err, "create task manager")
	}

	// Initialize storage manager
	storageMgr.Initialize(taskMgr)

	// Initialize storage manager
	var opts []grpc.ServerOption
	if s.config.Options.Telemetry.Jaeger != "" {
		opts = append(opts, grpc.ChainUnaryInterceptor(otelgrpc.UnaryServerInterceptor()), grpc.ChainStreamInterceptor(otelgrpc.StreamServerInterceptor()))
	}
	grpcServer, err := rpcserver.New(cfg, taskMgr, opts...)
	if err != nil {
		return nil, errors.Wrap(err, "create seedServer")
	}
	s.grpcServer = grpcServer

	// Initialize prometheus
	if cfg.Metrics != nil {
		s.metricsServer = metrics.New(cfg.Metrics, grpcServer)
	}

	// Initialize manager client
	if cfg.Manager.Addr != "" {
		managerClient, err := managerclient.New(cfg.Manager.Addr)
		if err != nil {
			return nil, err
		}
		s.managerClient = managerClient

		// Register to manager
		if _, err := s.managerClient.UpdateCDN(&manager.UpdateCDNRequest{
			SourceType:   manager.SourceType_CDN_SOURCE,
			HostName:     hostutils.FQDNHostname,
			Ip:           s.config.AdvertiseIP,
			Port:         int32(s.config.ListenPort),
			DownloadPort: int32(s.config.DownloadPort),
			Idc:          s.config.Host.IDC,
			Location:     s.config.Host.Location,
			CdnClusterId: uint64(s.config.Manager.CDNClusterID),
		}); err != nil {
			return nil, err
		}
	}

	return s, nil
}

func (s *Server) Serve() error {
	// Start GC
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := gc.StartGC(ctx); err != nil {
		return err
	}

	// Started metrics server
	if s.metricsServer != nil {
		go func() {
			logger.Infof("started metrics server at %s", s.metricsServer.Addr)
			if err := s.metricsServer.ListenAndServe(); err != nil {
				if err == http.ErrServerClosed {
					return
				}
				logger.Fatalf("metrics server closed unexpect: %#v", err)
			}
		}()
	}

	// Serve Keepalive
	if s.managerClient != nil {
		go func() {
			logger.Info("start keepalive to manager")
			s.managerClient.KeepAlive(s.config.Manager.KeepAlive.Interval, &manager.KeepAliveRequest{
				HostName:   hostutils.FQDNHostname,
				SourceType: manager.SourceType_CDN_SOURCE,
				ClusterId:  uint64(s.config.Manager.CDNClusterID),
			})
		}()
	}

	// Generate GRPC listener
	var listen = iputils.IPv4
	if s.config.AdvertiseIP != "" {
		listen = s.config.AdvertiseIP
	}
	lis, _, err := rpc.ListenWithPortRange(listen, s.config.ListenPort, s.config.ListenPort)
	if err != nil {
		logger.Fatalf("net listener failed to start: %v", err)
	}
	defer lis.Close()

	// Started GRPC server
	logger.Infof("started grpc server at %s://%s", lis.Addr().Network(), lis.Addr().String())
	if err := s.grpcServer.Serve(lis); err != nil {
		logger.Errorf("stoped grpc server: %v", err)
		return err
	}

	return nil
}

func (s *Server) Stop() {
	// Stop manager client
	if s.managerClient != nil {
		s.managerClient.Close()
		logger.Info("manager client closed")
	}

	// Stop metrics server
	if s.metricsServer != nil {
		if err := s.metricsServer.Shutdown(context.Background()); err != nil {
			logger.Errorf("metrics server failed to stop: %#v", err)
		}
		logger.Info("metrics server closed under request")
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
