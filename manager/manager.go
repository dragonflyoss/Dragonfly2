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

package manager

import (
	"context"
	"net/http"
	"time"

	"google.golang.org/grpc"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/manager/cache"
	"d7y.io/dragonfly/v2/manager/config"
	"d7y.io/dragonfly/v2/manager/database"
	"d7y.io/dragonfly/v2/manager/job"
	"d7y.io/dragonfly/v2/manager/metrics"
	"d7y.io/dragonfly/v2/manager/permission/rbac"
	"d7y.io/dragonfly/v2/manager/router"
	"d7y.io/dragonfly/v2/manager/searcher"
	"d7y.io/dragonfly/v2/manager/service"
	"d7y.io/dragonfly/v2/pkg/rpc"
	grpc_manager_server "d7y.io/dragonfly/v2/pkg/rpc/manager/server"
)

const (
	gracefulStopTimeout = 10 * time.Second
)

type Server struct {
	// Server configuration
	config *config.Config

	// GRPC server
	grpcServer *grpc.Server

	// REST server
	restServer *http.Server

	// Metrics server
	metricsServer *http.Server
}

func New(cfg *config.Config) (*Server, error) {
	s := &Server{config: cfg}

	// Initialize database
	db, err := database.New(cfg)
	if err != nil {
		return nil, err
	}

	// Initialize enforcer
	enforcer, err := rbac.NewEnforcer(db.DB)
	if err != nil {
		return nil, err
	}

	// Initialize cache
	cache, err := cache.New(cfg)
	if err != nil {
		return nil, err
	}

	// Initialize searcher
	searcher := searcher.New()

	// Initialize job
	job, err := job.New(cfg)
	if err != nil {
		return nil, err
	}

	// Initialize REST server
	restService := service.NewREST(db, cache, job, enforcer)
	router, err := router.Init(cfg, restService, enforcer)
	if err != nil {
		return nil, err
	}
	s.restServer = &http.Server{
		Addr:    cfg.Server.REST.Addr,
		Handler: router,
	}

	// Initialize roles and check roles
	err = rbac.InitRBAC(enforcer, router, db.DB)
	if err != nil {
		return nil, err
	}

	// Initialize GRPC server
	grpcService := service.NewGRPC(db, cache, searcher)
	var enableJaeger bool
	if cfg.Options.Telemetry.Jaeger != "" {
		enableJaeger = true
	}
	grpcServer := grpc_manager_server.New(grpcService, enableJaeger)
	s.grpcServer = grpcServer

	// Initialize prometheus
	if cfg.Metrics != nil {
		s.metricsServer = metrics.New(cfg.Metrics, grpcServer)
	}

	return s, nil
}

func (s *Server) Serve() error {
	// Started REST server
	go func() {
		logger.Infof("started rest server at %s", s.restServer.Addr)
		if err := s.restServer.ListenAndServe(); err != nil {
			if err == http.ErrServerClosed {
				return
			}
			logger.Fatalf("rest server closed unexpect: %v", err)
		}
	}()

	// Started metrics server
	if s.metricsServer != nil {
		go func() {
			logger.Infof("started metrics server at %s", s.metricsServer.Addr)
			if err := s.metricsServer.ListenAndServe(); err != nil {
				if err == http.ErrServerClosed {
					return
				}
				logger.Fatalf("metrics server closed unexpect: %v", err)
			}
		}()
	}

	// Generate GRPC listener
	lis, _, err := rpc.ListenWithPortRange(s.config.Server.GRPC.Listen, s.config.Server.GRPC.PortRange.Start, s.config.Server.GRPC.PortRange.End)
	if err != nil {
		logger.Fatalf("net listener failed to start: %v", err)
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
	// Stop REST server
	if err := s.restServer.Shutdown(context.Background()); err != nil {
		logger.Errorf("rest server failed to stop: %+v", err)
	}
	logger.Info("rest server closed under request")

	// Stop metrics server
	if s.metricsServer != nil {
		if err := s.metricsServer.Shutdown(context.Background()); err != nil {
			logger.Errorf("metrics server failed to stop: %+v", err)
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
