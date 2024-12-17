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
	"errors"
	"fmt"
	"net"
	"net/http"
	"path/filepath"
	"time"

	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/internal/dynconfig"
	managertypes "d7y.io/dragonfly/v2/manager/types"
	"d7y.io/dragonfly/v2/pkg/dfpath"
	"d7y.io/dragonfly/v2/pkg/gc"
	"d7y.io/dragonfly/v2/pkg/net/ip"
	pkgredis "d7y.io/dragonfly/v2/pkg/redis"
	"d7y.io/dragonfly/v2/pkg/rpc"
	managerclient "d7y.io/dragonfly/v2/pkg/rpc/manager/client"
	"d7y.io/dragonfly/v2/scheduler/announcer"
	"d7y.io/dragonfly/v2/scheduler/config"
	"d7y.io/dragonfly/v2/scheduler/job"
	"d7y.io/dragonfly/v2/scheduler/metrics"
	"d7y.io/dragonfly/v2/scheduler/resource/persistentcache"
	"d7y.io/dragonfly/v2/scheduler/resource/standard"
	"d7y.io/dragonfly/v2/scheduler/rpcserver"
	"d7y.io/dragonfly/v2/scheduler/scheduling"
)

const (
	// gracefulStopTimeout specifies a time limit for
	// grpc server to complete a graceful shutdown.
	gracefulStopTimeout = 10 * time.Minute
)

// Server is the scheduler server.
type Server struct {
	// Server configuration.
	config *config.Config

	// GRPC server.
	grpcServer *grpc.Server

	// Metrics server.
	metricsServer *http.Server

	// Manager client.
	managerClient managerclient.V2

	// Resource interface.
	resource standard.Resource

	// Persistent cache resource interface.
	persistentCacheResource persistentcache.Resource

	// Dynamic config.
	dynconfig config.DynconfigInterface

	// Async job.
	job job.Job

	// Announcer interface.
	announcer announcer.Announcer

	// GC service.
	gc gc.GC
}

// New creates a new scheduler server.
func New(ctx context.Context, cfg *config.Config, d dfpath.Dfpath) (*Server, error) {
	s := &Server{config: cfg}

	// Initialize dial options of manager grpc client.
	managerDialOptions := []grpc.DialOption{grpc.WithStatsHandler(otelgrpc.NewClientHandler())}
	if cfg.Manager.TLS != nil {
		clientTransportCredentials, err := rpc.NewClientCredentials(cfg.Manager.TLS.CACert, cfg.Manager.TLS.Cert, cfg.Manager.TLS.Key)
		if err != nil {
			logger.Errorf("failed to create client credentials: %v", err)
			return nil, err
		}

		managerDialOptions = append(managerDialOptions, grpc.WithTransportCredentials(clientTransportCredentials))
	} else {
		managerDialOptions = append(managerDialOptions, grpc.WithTransportCredentials(rpc.NewInsecureCredentials()))
	}

	// Initialize manager client.
	managerClient, err := managerclient.GetV2ByAddr(ctx, cfg.Manager.Addr, managerDialOptions...)
	if err != nil {
		return nil, err
	}
	s.managerClient = managerClient

	// Initialize redis client.
	var rdb redis.UniversalClient
	if pkgredis.IsEnabled(cfg.Database.Redis.Addrs) {
		rdb, err = pkgredis.NewRedis(&redis.UniversalOptions{
			Addrs:            cfg.Database.Redis.Addrs,
			MasterName:       cfg.Database.Redis.MasterName,
			Username:         cfg.Database.Redis.Username,
			Password:         cfg.Database.Redis.Password,
			SentinelUsername: cfg.Database.Redis.SentinelUsername,
			SentinelPassword: cfg.Database.Redis.SentinelPassword,
		})
		if err != nil {
			return nil, err
		}
	}

	// Initialize announcer. If job is enabled, add scheduler feature preheat.
	schedulerFeatures := []string{managertypes.SchedulerFeatureSchedule}
	if cfg.Job.Enable && rdb != nil {
		schedulerFeatures = append(schedulerFeatures, managertypes.SchedulerFeaturePreheat)
	}
	announcer, err := announcer.New(cfg, s.managerClient, schedulerFeatures)
	if err != nil {
		return nil, err
	}
	s.announcer = announcer

	// Initialize GC.
	s.gc = gc.New(gc.WithLogger(logger.GCLogger))

	// Initialize seed peer client transport credentials.
	seedPeerClientTransportCredentials := rpc.NewInsecureCredentials()
	if cfg.SeedPeer.TLS != nil {
		seedPeerClientTransportCredentials, err = rpc.NewClientCredentials(cfg.SeedPeer.TLS.CACert, cfg.SeedPeer.TLS.Cert, cfg.SeedPeer.TLS.Key)
		if err != nil {
			logger.Errorf("failed to create seed peer client credentials: %v", err)
			return nil, err
		}
	}

	// Initialize dynconfig.
	dynconfig, err := config.NewDynconfig(s.managerClient, filepath.Join(d.CacheDir(), dynconfig.CacheDirName), cfg, seedPeerClientTransportCredentials)
	if err != nil {
		return nil, err
	}
	s.dynconfig = dynconfig

	// Initialize resource.
	resource, err := standard.New(cfg, s.gc, dynconfig, seedPeerClientTransportCredentials)
	if err != nil {
		return nil, err
	}
	s.resource = resource

	// Initialize seed peer client transport credentials.
	peerClientTransportCredentials := rpc.NewInsecureCredentials()
	if cfg.Peer.TLS != nil {
		peerClientTransportCredentials, err = rpc.NewClientCredentials(cfg.Peer.TLS.CACert, cfg.Peer.TLS.Cert, cfg.Peer.TLS.Key)
		if err != nil {
			logger.Errorf("failed to create peer client credentials: %v", err)
			return nil, err
		}
	}

	// Initialize persistent cache resource.
	if rdb != nil {
		s.persistentCacheResource, err = persistentcache.New(cfg, s.gc, rdb, peerClientTransportCredentials)
		if err != nil {
			logger.Errorf("failed to create persistent cache resource: %v", err)
			return nil, err
		}
	}

	// Initialize job service.
	if cfg.Job.Enable && rdb != nil {
		s.job, err = job.New(cfg, resource)
		if err != nil {
			return nil, err
		}
	}

	// Initialize scheduling.
	scheduling := scheduling.New(&cfg.Scheduler, dynconfig, d.PluginDir())

	// Initialize server options of scheduler grpc server.
	schedulerServerOptions := []grpc.ServerOption{}
	if cfg.Server.TLS != nil {
		// Initialize grpc server with tls.
		transportCredentials, err := rpc.NewServerCredentials(cfg.Server.TLS.CACert, cfg.Server.TLS.Cert, cfg.Server.TLS.Key)
		if err != nil {
			logger.Errorf("failed to create server credentials: %v", err)
			return nil, err
		}

		schedulerServerOptions = append(schedulerServerOptions, grpc.Creds(transportCredentials))
	} else {
		// Initialize grpc server without tls.
		schedulerServerOptions = append(schedulerServerOptions, grpc.Creds(rpc.NewInsecureCredentials()))
	}

	svr := rpcserver.New(cfg, resource, s.persistentCacheResource, scheduling, dynconfig, schedulerServerOptions...)
	s.grpcServer = svr

	// Initialize metrics.
	if cfg.Metrics.Enable {
		s.metricsServer = metrics.New(&cfg.Metrics, s.grpcServer)
	}

	return s, nil
}

// Serve starts the scheduler server.
func (s *Server) Serve() error {
	// Serve dynconfig.
	go func() {
		if err := s.dynconfig.Serve(); err != nil {
			logger.Fatalf("dynconfig start failed %s", err.Error())
		}

		logger.Info("dynconfig start successfully")
	}()

	// Serve GC.
	s.gc.Start()
	logger.Info("gc start successfully")

	// Serve Job.
	if s.job != nil {
		s.job.Serve()
		logger.Info("job start successfully")
	}

	// Started metrics server.
	if s.metricsServer != nil {
		go func() {
			logger.Infof("started metrics server at %s", s.metricsServer.Addr)
			if err := s.metricsServer.ListenAndServe(); err != nil {
				if err == http.ErrServerClosed {
					return
				}

				logger.Fatalf("metrics server closed unexpect: %s", err.Error())
			}
		}()
	}

	// Serve announcer.
	go func() {
		s.announcer.Serve()
		logger.Info("announcer start successfully")
	}()

	// Generate GRPC listener.
	ip, ok := ip.FormatIP(s.config.Server.ListenIP.String())
	if !ok {
		return errors.New("format ip failed")
	}

	listener, err := net.Listen("tcp", fmt.Sprintf("%s:%d", ip, s.config.Server.Port))
	if err != nil {
		logger.Fatalf("net listener failed to start: %s", err.Error())
	}
	defer listener.Close()

	// Started GRPC server.
	logger.Infof("started grpc server at %s://%s", listener.Addr().Network(), listener.Addr().String())
	if err := s.grpcServer.Serve(listener); err != nil {
		logger.Errorf("stoped grpc server: %s", err.Error())
		return err
	}

	return nil
}

// Stop stops the scheduler server.
func (s *Server) Stop() {
	// Stop dynconfig.
	if err := s.dynconfig.Stop(); err != nil {
		logger.Errorf("stop dynconfig failed %s", err.Error())
	} else {
		logger.Info("stop dynconfig closed")
	}

	// Stop resource.
	if err := s.resource.Stop(); err != nil {
		logger.Errorf("stop resource failed %s", err.Error())
	} else {
		logger.Info("stop resource closed")
	}

	// Stop GC.
	s.gc.Stop()
	logger.Info("gc closed")

	// Stop metrics server.
	if s.metricsServer != nil {
		if err := s.metricsServer.Shutdown(context.Background()); err != nil {
			logger.Errorf("metrics server failed to stop: %s", err.Error())
		} else {
			logger.Info("metrics server closed under request")
		}
	}

	// Stop announcer.
	s.announcer.Stop()
	logger.Info("stop announcer closed")

	// Stop manager client.
	if s.managerClient != nil {
		if err := s.managerClient.Close(); err != nil {
			logger.Errorf("manager client failed to stop: %s", err.Error())
		} else {
			logger.Info("manager client closed")
		}
	}

	// Stop GRPC server.
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
