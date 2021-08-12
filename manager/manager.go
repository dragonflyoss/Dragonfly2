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

	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/manager/cache"
	"d7y.io/dragonfly/v2/manager/config"
	"d7y.io/dragonfly/v2/manager/database"
	"d7y.io/dragonfly/v2/manager/job"
	"d7y.io/dragonfly/v2/manager/permission/rbac"
	"d7y.io/dragonfly/v2/manager/proxy"
	"d7y.io/dragonfly/v2/manager/router"
	"d7y.io/dragonfly/v2/manager/searcher"
	"d7y.io/dragonfly/v2/manager/service"
	"d7y.io/dragonfly/v2/pkg/rpc"
	"d7y.io/dragonfly/v2/pkg/rpc/manager"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
)

type Server struct {
	// Server configuration
	config *config.Config

	// GRPC service
	service *service.GRPC

	// REST server
	restServer *http.Server

	// Proxy server
	proxyServer proxy.Proxy
}

func New(cfg *config.Config) (*Server, error) {
	// Initialize database
	db, err := database.New(cfg)
	if err != nil {
		return nil, err
	}
	enforcer, err := rbac.NewEnforcer(db.DB)
	if err != nil {
		return nil, err
	}

	// Initialize cache
	cache := cache.New(cfg)

	// Initialize searcher
	searcher := searcher.New()

	// Initialize job
	job, err := job.New(cfg)
	if err != nil {
		return nil, err
	}

	// Initialize REST service
	restService := service.NewREST(db, cache, job, enforcer)

	// Initialize GRPC service
	grpcService := service.NewGRPC(db, cache, searcher)

	// Initialize Proxy service
	proxyServer := proxy.New(cfg.Database.Redis)

	// Initialize router
	router, err := router.Init(cfg.Verbose, restService, enforcer)
	if err != nil {
		return nil, err
	}

	return &Server{
		config:  cfg,
		service: grpcService,
		restServer: &http.Server{
			Addr:    cfg.Server.REST.Addr,
			Handler: router,
		},
		proxyServer: proxyServer,
	}, nil
}

func (s *Server) Serve() error {
	g := errgroup.Group{}

	// GRPC listener
	lis, _, err := rpc.ListenWithPortRange(s.config.Server.GRPC.Listen, s.config.Server.GRPC.PortRange.Start, s.config.Server.GRPC.PortRange.End)
	if err != nil {
		logger.Errorf("failed to net listen: %+v", err)
		return err
	}

	// Serve GRPC
	g.Go(func() error {
		defer lis.Close()
		grpcServer := grpc.NewServer()
		manager.RegisterManagerServer(grpcServer, s.service)
		logger.Infof("serve grpc at %s://%s", lis.Addr().Network(), lis.Addr().String())
		if err := grpcServer.Serve(lis); err != nil {
			logger.Errorf("failed to start manager grpc server: %+v", err)
		}
		return nil
	})

	// Serve REST
	g.Go(func() error {
		if err := s.restServer.ListenAndServe(); err != nil {
			logger.Errorf("failed to start manager rest server: %+v", err)
			return err
		}
		return nil
	})

	// Serve Proxy
	g.Go(func() error {
		if err := s.proxyServer.Serve(); err != nil {
			logger.Errorf("failed to start manager proxy server: %+v", err)
			return err
		}
		return nil
	})

	return g.Wait()
}

func (s *Server) Stop() {
	// Stop REST
	err := s.restServer.Shutdown(context.TODO())
	if err != nil {
		logger.Errorf("failed to stop manager rest server: %+v", err)
	}

	// Stop Proxy
	s.proxyServer.Stop()
}
