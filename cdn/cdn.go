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

// Package cdnsystem cdn server
package cdn

import (
	"context"
	"fmt"
	"runtime"

	"d7y.io/dragonfly/v2/cdn/config"
	"d7y.io/dragonfly/v2/cdn/plugins"
	"d7y.io/dragonfly/v2/cdn/rpcserver"
	"d7y.io/dragonfly/v2/cdn/supervisor/cdn"
	"d7y.io/dragonfly/v2/cdn/supervisor/cdn/storage"
	"d7y.io/dragonfly/v2/cdn/supervisor/gc"
	"d7y.io/dragonfly/v2/cdn/supervisor/progress"
	"d7y.io/dragonfly/v2/cdn/supervisor/task"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/rpc"
	"d7y.io/dragonfly/v2/pkg/rpc/cdnsystem/server"
	"d7y.io/dragonfly/v2/pkg/rpc/manager"
	managerclient "d7y.io/dragonfly/v2/pkg/rpc/manager/client"
	"d7y.io/dragonfly/v2/pkg/util/net/iputils"
	"github.com/pkg/errors"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
)

type Server struct {
	config        *config.Config
	seedServer    server.SeederServer
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
	cdnSeedServer, err := rpcserver.NewCdnSeedServer(cfg, taskMgr)
	if err != nil {
		return nil, errors.Wrap(err, "create seedServer")
	}
	s.seedServer = cdnSeedServer

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
			HostName:     iputils.HostName,
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

func (s *Server) Serve() (err error) {
	defer func() {
		if rec := recover(); rec != nil {
			err = errors.New(fmt.Sprintf("%v", rec))
		}
	}()

	// Start GC
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err = gc.StartGC(ctx)
	if err != nil {
		return err
	}

	// Serve Keepalive
	if s.managerClient != nil {
		go func() {
			logger.Info("start keepalive to manager")
			s.managerClient.KeepAlive(s.config.Manager.KeepAlive.Interval, &manager.KeepAliveRequest{
				HostName:   iputils.HostName,
				SourceType: manager.SourceType_CDN_SOURCE,
				ClusterId:  uint64(s.config.Manager.CDNClusterID),
			})
		}()
	}

	// Serve GRPC
	var opts []grpc.ServerOption
	if s.config.Options.Telemetry.Jaeger != "" {
		opts = append(opts, grpc.ChainUnaryInterceptor(otelgrpc.UnaryServerInterceptor()), grpc.ChainStreamInterceptor(otelgrpc.StreamServerInterceptor()))
	}
	err = rpc.StartTCPServer(s.config.ListenPort, s.config.ListenPort, s.seedServer, opts...)
	if err != nil {
		return errors.Wrap(err, "start tcp server")
	}
	return nil
}

func (s *Server) Stop() {
	if s.managerClient != nil {
		s.managerClient.Close()
		logger.Info("manager client closed")
	}

	rpc.StopServer()
	logger.Info("grpc server closed under request")
}
