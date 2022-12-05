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
	"errors"
	"fmt"
	"net"
	"net/http"
	"path/filepath"
	"time"

	"github.com/johanbrandhorst/certify"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	zapadapter "logur.dev/adapter/zap"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/internal/dynconfig"
	"d7y.io/dragonfly/v2/pkg/cache"
	"d7y.io/dragonfly/v2/pkg/dfpath"
	"d7y.io/dragonfly/v2/pkg/gc"
	"d7y.io/dragonfly/v2/pkg/issuer"
	"d7y.io/dragonfly/v2/pkg/net/ip"
	"d7y.io/dragonfly/v2/pkg/rpc"
	managerclient "d7y.io/dragonfly/v2/pkg/rpc/manager/client"
	"d7y.io/dragonfly/v2/pkg/types"
	"d7y.io/dragonfly/v2/scheduler/announcer"
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

	// Storage interface.
	storage storage.Storage

	// Announcer interface.
	announcer announcer.Announcer

	// GC service.
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

	// Initialize announcer.
	announcer, err := announcer.New(cfg, s.managerClient)
	if err != nil {
		return nil, err
	}
	s.announcer = announcer

	// Initialize certify client.
	var (
		certifyClient              *certify.Certify
		clientTransportCredentials credentials.TransportCredentials
	)
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

		clientTransportCredentials, err = rpc.NewClientCredentialsByCertify(cfg.Security.TLSPolicy, []byte(cfg.Security.CACert), certifyClient)
		if err != nil {
			return nil, err
		}

		// Issue a certificate to reduce first time delay.
		if _, err := certifyClient.GetCertificate(&tls.ClientHelloInfo{
			ServerName: cfg.Server.AdvertiseIP,
		}); err != nil {
			return nil, err
		}
	}

	// Initialize dynconfig client.
	dynconfig, err := config.NewDynconfig(s.managerClient, filepath.Join(d.CacheDir(), dynconfig.CacheDirName), cfg, config.WithTransportCredentials(clientTransportCredentials))
	if err != nil {
		return nil, err
	}
	s.dynconfig = dynconfig

	// Initialize GC.
	s.gc = gc.New(gc.WithLogger(logger.GCLogger))

	// Initialize resource.
	resource, err := resource.New(cfg, s.gc, dynconfig, resource.WithTransportCredentials(clientTransportCredentials))
	if err != nil {
		return nil, err
	}
	s.resource = resource

	// Initialize scheduler.
	scheduler := scheduler.New(&cfg.Scheduler, dynconfig, d.PluginDir())

	// Initialize Storage.
	storage, err := storage.New(
		d.DataDir(),
		cfg.Storage.MaxSize,
		cfg.Storage.MaxBackups,
		cfg.Storage.BufferSize,
	)
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
		s.metricsServer = metrics.New(&cfg.Metrics, s.grpcServer)
	}

	return s, nil
}

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

	// Serve announcer.
	go func() {
		if err := s.announcer.Serve(); err != nil {
			logger.Fatalf("announcer start failed %s", err.Error())
		}
		logger.Info("announcer start successfully")
	}()

	// Generate GRPC limit listener.
	ip, ok := ip.FormatIP(s.config.Server.ListenIP)
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

	// Stop announcer.
	if err := s.announcer.Stop(); err != nil {
		logger.Errorf("stop announcer failed %s", err.Error())
	} else {
		logger.Info("stop announcer closed")
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
