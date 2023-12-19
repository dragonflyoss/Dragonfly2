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

	"github.com/go-redis/redis/v8"
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
	pkgredis "d7y.io/dragonfly/v2/pkg/redis"
	"d7y.io/dragonfly/v2/pkg/rpc"
	managerclient "d7y.io/dragonfly/v2/pkg/rpc/manager/client"
	securityclient "d7y.io/dragonfly/v2/pkg/rpc/security/client"
	trainerclient "d7y.io/dragonfly/v2/pkg/rpc/trainer/client"
	"d7y.io/dragonfly/v2/pkg/types"
	"d7y.io/dragonfly/v2/scheduler/announcer"
	"d7y.io/dragonfly/v2/scheduler/config"
	"d7y.io/dragonfly/v2/scheduler/job"
	"d7y.io/dragonfly/v2/scheduler/metrics"
	"d7y.io/dragonfly/v2/scheduler/networktopology"
	"d7y.io/dragonfly/v2/scheduler/resource"
	"d7y.io/dragonfly/v2/scheduler/rpcserver"
	"d7y.io/dragonfly/v2/scheduler/scheduling"
	"d7y.io/dragonfly/v2/scheduler/storage"
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

	// Security client.
	securityClient securityclient.V1

	// Trainer client.
	trainerClient trainerclient.V1

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

	// Network topology interface.
	networkTopology networktopology.NetworkTopology

	// GC service.
	gc gc.GC
}

// New creates a new scheduler server.
func New(ctx context.Context, cfg *config.Config, d dfpath.Dfpath) (*Server, error) {
	s := &Server{config: cfg}

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

	// Initialize dial options of manager grpc client.
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

	// Initialize manager client.
	managerClient, err := managerclient.GetV2ByAddr(ctx, cfg.Manager.Addr, managerDialOptions...)
	if err != nil {
		return nil, err
	}
	s.managerClient = managerClient

	// Initialize dial options of trainer grpc client.
	if cfg.Trainer.Enable {
		trainerDialOptions := []grpc.DialOption{}
		if cfg.Security.AutoIssueCert {
			clientTransportCredentials, err := rpc.NewClientCredentials(cfg.Security.TLSPolicy, nil, []byte(cfg.Security.CACert))
			if err != nil {
				return nil, err
			}

			trainerDialOptions = append(trainerDialOptions, grpc.WithTransportCredentials(clientTransportCredentials))
		} else {
			trainerDialOptions = append(trainerDialOptions, grpc.WithTransportCredentials(insecure.NewCredentials()))
		}

		// Initialize trainer client.
		trainerClient, err := trainerclient.GetV1ByAddr(ctx, cfg.Trainer.Addr, trainerDialOptions...)
		if err != nil {
			return nil, err
		}
		s.trainerClient = trainerClient
	}

	// Initialize dial options of announcer.
	announcerOptions := []announcer.Option{}
	if s.trainerClient != nil {
		announcerOptions = append(announcerOptions, announcer.WithTrainerClient(s.trainerClient))
	}

	// Initialize announcer.
	announcer, err := announcer.New(cfg, s.managerClient, storage, announcerOptions...)
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
		// Initialize security client.
		securityClient, err := securityclient.GetV1(ctx, cfg.Manager.Addr, managerDialOptions...)
		if err != nil {
			return nil, err
		}
		s.securityClient = securityClient

		certifyClient = &certify.Certify{
			CommonName:  types.SchedulerName,
			Issuer:      issuer.NewDragonflyIssuer(s.securityClient, issuer.WithValidityPeriod(cfg.Security.CertSpec.ValidityPeriod)),
			RenewBefore: time.Hour,
			CertConfig: &certify.CertConfig{
				SubjectAlternativeNames:   cfg.Security.CertSpec.DNSNames,
				IPSubjectAlternativeNames: append(cfg.Security.CertSpec.IPAddresses, cfg.Server.AdvertiseIP),
			},
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
			ServerName: cfg.Server.AdvertiseIP.String(),
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

	// Initialize redis client.
	var rdb redis.UniversalClient
	if pkgredis.IsEnabled(cfg.Database.Redis.Addrs) {
		rdb, err = pkgredis.NewRedis(&redis.UniversalOptions{
			Addrs:      cfg.Database.Redis.Addrs,
			MasterName: cfg.Database.Redis.MasterName,
			DB:         cfg.Database.Redis.NetworkTopologyDB,
			Username:   cfg.Database.Redis.Username,
			Password:   cfg.Database.Redis.Password,
		})
		if err != nil {
			return nil, err
		}
	}

	// Initialize job service.
	if cfg.Job.Enable && pkgredis.IsEnabled(cfg.Database.Redis.Addrs) {
		s.job, err = job.New(cfg, resource)
		if err != nil {
			return nil, err
		}
	}

	// Initialize network topology service.
	if cfg.NetworkTopology.Enable && pkgredis.IsEnabled(cfg.Database.Redis.Addrs) {
		cache := cache.New(cfg.NetworkTopology.Cache.TTL, cfg.NetworkTopology.Cache.Interval)
		s.networkTopology, err = networktopology.NewNetworkTopology(cfg.NetworkTopology, rdb, cache, resource, s.storage)
		if err != nil {
			return nil, err
		}
	}

	// Initialize scheduling.
	scheduling := scheduling.New(&cfg.Scheduler, dynconfig, d.PluginDir())

	// Initialize server options of scheduler grpc server.
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

	svr := rpcserver.New(cfg, resource, scheduling, dynconfig, s.storage, s.networkTopology, schedulerServerOptions...)
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

	// Serve network topology.
	if s.networkTopology != nil {
		go func() {
			s.networkTopology.Serve()
			logger.Info("network topology start successfully")
		}()
	}

	// Generate GRPC limit listener.
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

	// Clean download storage.
	if err := s.storage.ClearDownload(); err != nil {
		logger.Errorf("clean download storage failed %s", err.Error())
	} else {
		logger.Info("clean download storage completed")
	}

	// Clean network topology storage.
	if err := s.storage.ClearNetworkTopology(); err != nil {
		logger.Errorf("clean network topology storage failed %s", err.Error())
	} else {
		logger.Info("clean network topology storage completed")
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

	// Stop trainer client.
	if s.trainerClient != nil {
		if err := s.trainerClient.Close(); err != nil {
			logger.Errorf("trainer client failed to stop: %s", err.Error())
		} else {
			logger.Info("trainer client closed")
		}
	}

	// Stop security client.
	if s.securityClient != nil {
		if err := s.securityClient.Close(); err != nil {
			logger.Errorf("security client failed to stop: %s", err.Error())
		} else {
			logger.Info("security client closed")
		}
	}

	// Stop network topology.
	if s.networkTopology != nil {
		s.networkTopology.Stop()
		logger.Info("network topology closed")
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
