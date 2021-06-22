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
	"net/http"

	"d7y.io/dragonfly/v2/manager/cache"
	"d7y.io/dragonfly/v2/manager/config"
	"d7y.io/dragonfly/v2/manager/database"
	"d7y.io/dragonfly/v2/manager/service"
	logger "d7y.io/dragonfly/v2/pkg/dflog"
	"golang.org/x/sync/errgroup"

	// manager server rpc
	_ "d7y.io/dragonfly/v2/pkg/rpc/manager/server"
)

type Server struct {
	config     *config.Config
	service    service.Service
	restServer *http.Server
}

func New(cfg *config.Config) (*Server, error) {
	// Initialize database
	db, err := database.New(cfg)
	if err != nil {
		return nil, err
	}

	// Initialize database
	cache := cache.New(cfg.Cache)

	// Initialize service
	service := service.New(
		service.WithDatabase(db),
		service.WithCache(cache),
	)

	// Initialize router
	router, err := initRouter(cfg.Verbose, service)
	if err != nil {
		return nil, err
	}

	return &Server{
		config:  cfg,
		service: service,
		restServer: &http.Server{
			Addr:    cfg.Server.Addr,
			Handler: router,
		},
	}, nil
}

func (s *Server) Serve() error {
	g := errgroup.Group{}
	// go func() {
	// port := s.cfg.Server.Port
	// err := rpc.StartTCPServer(port, port, s.rpcService)
	// if err != nil {
	// logger.Errorf("failed to start manager tcp server: %+v", err)
	// }

	// s.stop <- struct{}{}
	// }()

	g.Go(func() error {
		if err := s.restServer.ListenAndServe(); err != nil {
			logger.Errorf("failed to start manager rest server: %+v", err)
			return err
		}
		return nil
	})

	werr := g.Wait()

	return werr
}

func (s *Server) Stop() {
	// if s.ms != nil {
	// err := s.ms.Close()
	// if err != nil {
	// logger.Errorf("failed to stop manager server: %+v", err)
	// }

	// s.ms = nil
	// }

	// rpc.StopServer()

	// ctx, cancel := context.WithTimeout(context.TODO(), 5*time.Second)
	// defer cancel()

	err := s.restServer.Shutdown(context.TODO())
	if err != nil {
		logger.Errorf("failed to stop manager rest server: %+v", err)
	}
}
