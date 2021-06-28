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

package server

import (
	"context"
	"fmt"
	"net"
	"net/http"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/internal/rpc/manager"
	"d7y.io/dragonfly/v2/manager/cache"
	"d7y.io/dragonfly/v2/manager/config"
	"d7y.io/dragonfly/v2/manager/database"
	"d7y.io/dragonfly/v2/manager/service"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
)

type Server struct {
	// Server configuration
	config *config.Config

	// GRPC service
	service *service.ServiceGRPC

	// REST server
	restServer *http.Server
}

func New(cfg *config.Config) (*Server, error) {
	// Initialize database
	db, err := database.New(cfg)
	if err != nil {
		return nil, err
	}

	// Initialize database
	cache := cache.New(cfg)

	// Initialize REST service
	restService := service.New(
		service.WithDatabase(db),
		service.WithCache(cache),
	)

	// Initialize GRPC service
	grpcService := service.NewGRPC(
		service.GRPCWithDatabase(db),
		service.GRPCWithCache(cache),
	)

	// Initialize router
	router, err := initRouter(cfg.Verbose, restService)
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
	}, nil
}

func (s *Server) Serve() error {
	g := errgroup.Group{}

	// Serve GRPC
	g.Go(func() error {
		lis, err := net.Listen("tcp", fmt.Sprintf(":%d", s.config.Server.GRPC.PortRange.Start))
		if err != nil {
			logger.Errorf("failed to net listen: %+v", err)
			return err
		}
		grpcServer := grpc.NewServer()
		manager.RegisterManagerServer(grpcServer, s.service)
		go func() {
			if err := grpcServer.Serve(lis); err != nil {
				logger.Errorf("failed to start manager grpc server: %+v", err)
			}
		}()
		logger.Infof("start grpc server with port %s", lis.Addr())
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

	return g.Wait()
}

func (s *Server) Stop() {
	// Stop REST
	err := s.restServer.Shutdown(context.TODO())
	if err != nil {
		logger.Errorf("failed to stop manager rest server: %+v", err)
	}
}
