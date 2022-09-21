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
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"path/filepath"
	"time"

	"github.com/johanbrandhorst/certify"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	zapadapter "logur.dev/adapter/zap"

	managerv1 "d7y.io/api/pkg/apis/manager/v1"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/internal/dynconfig"
	"d7y.io/dragonfly/v2/pkg/cache"
	"d7y.io/dragonfly/v2/pkg/dfpath"
	"d7y.io/dragonfly/v2/pkg/gc"
	"d7y.io/dragonfly/v2/pkg/issuer"
	"d7y.io/dragonfly/v2/pkg/rpc"
	managerclient "d7y.io/dragonfly/v2/pkg/rpc/manager/client"
	"d7y.io/dragonfly/v2/pkg/types"
	"d7y.io/dragonfly/v2/scheduler/config"
	"d7y.io/dragonfly/v2/scheduler/job"
	"d7y.io/dragonfly/v2/scheduler/metrics"
	"d7y.io/dragonfly/v2/scheduler/resource"
	"d7y.io/dragonfly/v2/scheduler/rpcserver"
	"d7y.io/dragonfly/v2/scheduler/scheduler"
	"d7y.io/dragonfly/v2/scheduler/service"
	"d7y.io/dragonfly/v2/scheduler/storage"
)

const (
	// gracefulStopTimeout specifies a time limit for
	// grpc server to complete a graceful shutdown.
	gracefulStopTimeout = 10 * time.Minute
)

type Server struct {
	// Server configuration.
	config *config.Config

	// GRPC server.
	grpcServer *grpc.Server

	// Metrics server.
	metricsServer *http.Server

	// Manager client.
	managerClient managerclient.Client

	// Resource interface.
	resource resource.Resource

	// Dynamic config.
	dynconfig config.DynconfigInterface

	// Async job.
	job job.Job

	// Storage service.
	storage storage.Storage

	// GC server.
	gc gc.GC
}

func New(ctx context.Context, cfg *config.Config, d dfpath.Dfpath) (*Server, error) {
	s := &Server{config: cfg}

	// Initialize manager client and dial options of manager grpc client.
	managerDialOptions := []grpc.DialOption{}
	if cfg.Security.AutoIssueCert {
		clientTransportCredentials, err := rpc.NewClientCredentials(cfg.Security.TLSPolicy, nil, []byte(cfg.Security.CACert))
		if err != nil {
			return nil, err
		}

		managerDialOptions = append(managerDialOptions, grpc.WithTransportCredentials(clientTransportCredentials))
	} else {
		managerDialOptions = append(managerDialOptions, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	managerClient, err := managerclient.GetClient(ctx, cfg.Manager.Addr, managerDialOptions...)
	if err != nil {
		return nil, err
	}
	s.managerClient = managerClient

	// Register to manager.
	if _, err := s.managerClient.UpdateScheduler(context.Background(), &managerv1.UpdateSchedulerRequest{
		SourceType:         managerv1.SourceType_SCHEDULER_SOURCE,
		HostName:           s.config.Server.Host,
		Ip:                 s.config.Server.AdvertiseIP,
		Port:               int32(s.config.Server.Port),
		Idc:                s.config.Host.IDC,
		Location:           s.config.Host.Location,
		SchedulerClusterId: uint64(s.config.Manager.SchedulerClusterID),
	}); err != nil {
		logger.Fatalf("register to manager failed %s", err.Error())
	}

	// Initialize dynconfig client.
	dynconfig, err := config.NewDynconfig(s.managerClient, filepath.Join(d.CacheDir(), dynconfig.CacheDirName), cfg)
	if err != nil {
		return nil, err
	}
	s.dynconfig = dynconfig

	// Initialize GC.
	s.gc = gc.New(gc.WithLogger(logger.GCLogger))

	// Initialize certify client.
	var certifyClient *certify.Certify
	if cfg.Security.AutoIssueCert {
		certifyClient = &certify.Certify{
			CommonName:   types.SchedulerName,
			Issuer:       issuer.NewDragonflyIssuer(managerClient, issuer.WithValidityPeriod(cfg.Security.CertSpec.ValidityPeriod)),
			RenewBefore:  time.Hour,
			CertConfig:   nil,
			IssueTimeout: 0,
			Logger:       zapadapter.New(logger.CoreLogger.Desugar()),
			Cache: cache.NewCertifyMutliCache(
				certify.NewMemCache(),
				certify.DirCache(filepath.Join(d.CacheDir(), cache.CertifyCacheDirName, types.SchedulerName))),
		}

		// Issue a certificate to reduce first time delay.
		if _, err := certifyClient.GetCertificate(&tls.ClientHelloInfo{
			ServerName: cfg.Server.AdvertiseIP,
		}); err != nil {
			logger.Errorf("issue certificate error: %s", err.Error())
			return nil, err
		}
	}

	// Initialize resource and dial options of seed peer grpc client.
	seedPeerDialOptions := []grpc.DialOption{}
	if certifyClient != nil {
		clientTransportCredentials, err := rpc.NewClientCredentialsByCertify(cfg.Security.TLSPolicy, []byte(cfg.Security.CACert), certifyClient)
		if err != nil {
			return nil, err
		}

		seedPeerDialOptions = append(seedPeerDialOptions, grpc.WithTransportCredentials(clientTransportCredentials))
	} else {
		seedPeerDialOptions = append(seedPeerDialOptions, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	resource, err := resource.New(cfg, s.gc, dynconfig, seedPeerDialOptions...)
	if err != nil {
		return nil, err
	}

	// Initialize scheduler.
	scheduler := scheduler.New(cfg.Scheduler, dynconfig, d.PluginDir())

	// Initialize Storage.
	storage, err := storage.New(d.DataDir())
	if err != nil {
		return nil, err
	}
	s.storage = storage

	// Initialize scheduler service.
	service := service.New(cfg, resource, scheduler, dynconfig, s.storage)

	// Initialize grpc service and server options of scheduler grpc server.
	schedulerServerOptions := []grpc.ServerOption{}
	if certifyClient != nil {
		serverTransportCredentials, err := rpc.NewServerCredentialsByCertify(cfg.Security.TLSPolicy, cfg.Security.TLSVerify, []byte(cfg.Security.CACert), certifyClient)
		if err != nil {
			return nil, err
		}

		schedulerServerOptions = append(schedulerServerOptions, grpc.Creds(serverTransportCredentials))
	} else {
		schedulerServerOptions = append(schedulerServerOptions, grpc.Creds(insecure.NewCredentials()))
	}

	svr := rpcserver.New(service, schedulerServerOptions...)
	s.grpcServer = svr

	// Initialize job service.
	if cfg.Job.Enable {
		s.job, err = job.New(cfg, resource)
		if err != nil {
			return nil, err
		}
	}

	// Initialize metrics.
	if cfg.Metrics.Enable {
		s.metricsServer = metrics.New(cfg.Metrics, s.grpcServer)
	}

	return s, nil
}

func (s *Server) Serve() error {
	// Serve dynConfig.
	go func() {
		if err := s.dynconfig.Serve(); err != nil {
			logger.Fatalf("dynconfig start failed %s", err.Error())
		}
		logger.Info("dynconfig start successfully")
	}()

	// Serve GC.
	s.gc.Serve()
	logger.Info("gc start successfully")

	// Serve Job.
	if s.config.Job.Enable {
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

	if s.managerClient != nil {
		// scheduler keepalive with manager.
		go func() {
			logger.Info("start keepalive to manager")
			s.managerClient.KeepAlive(s.config.Manager.KeepAlive.Interval, &managerv1.KeepAliveRequest{
				SourceType: managerv1.SourceType_SCHEDULER_SOURCE,
				HostName:   s.config.Server.Host,
				Ip:         s.config.Server.AdvertiseIP,
				ClusterId:  uint64(s.config.Manager.SchedulerClusterID),
			})
		}()
	}

	// Generate GRPC limit listener.
	listener, err := net.Listen("tcp", fmt.Sprintf("%s:%d", s.config.Server.ListenIP, s.config.Server.Port))
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

func (s *Server) Stop() {
	// Stop dynconfig service.
	if err := s.dynconfig.Stop(); err != nil {
		logger.Errorf("stop dynconfig service failed %s", err.Error())
	} else {
		logger.Info("stop dynconfig service closed")
	}

	// Stop resource service.
	if err := s.resource.Stop(); err != nil {
		logger.Errorf("stop resource service failed %s", err.Error())
	} else {
		logger.Info("stop resource service closed")
	}

	// Clean storage.
	if err := s.storage.Clear(); err != nil {
		logger.Errorf("clean storage failed %s", err.Error())
	} else {
		logger.Info("clean storage completed")
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
