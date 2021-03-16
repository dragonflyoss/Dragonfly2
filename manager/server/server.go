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
	"d7y.io/dragonfly/v2/manager/config"
	"d7y.io/dragonfly/v2/manager/server/service"
	logger "d7y.io/dragonfly/v2/pkg/dflog"
	"d7y.io/dragonfly/v2/pkg/rpc"
	_ "d7y.io/dragonfly/v2/pkg/rpc/manager/server"
	"github.com/pkg/errors"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

type Server struct {
	cfg        *config.Config
	ms         *service.ManagerServer
	httpServer *http.Server
	stop       chan struct{}
}

func NewServer(cfg *config.Config) (*Server, error) {
	if ms := service.NewManagerServer(cfg); ms != nil {
		router, err := InitRouter(ms)
		if err != nil {
			return nil, err
		}

		return &Server{
			cfg: cfg,
			ms:  ms,
			httpServer: &http.Server{
				Addr:    ":8080",
				Handler: router,
			},
			stop: make(chan struct{}),
		}, nil
	} else {
		return nil, errors.New("failed to create manager server")
	}
}

func (s *Server) Start() (error) {
	go func() {
		port := s.cfg.Server.Port
		err := rpc.StartTcpServer(port, port, s.ms)
		if err != nil {
			logger.Errorf("failed to start manager tcp server: %+v", err)
		}

		s.stop <- struct{}{}
	}()

	go func() {
		if err := s.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Errorf("failed to start manager http server: %+v", err)
		}

		s.stop <- struct{}{}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-s.stop:
		s.Stop()
	case <-quit:
		s.Stop()
	}

	return nil
}

func (s *Server) Stop() {
	rpc.StopServer()

	ctx, cancel := context.WithTimeout(context.TODO(), 5*time.Second)
	defer cancel()
	if err := s.httpServer.Shutdown(ctx); err != nil {
		logger.Errorf("failed to stop manager http server: %+v", err)
	}
}
