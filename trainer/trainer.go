/*
 *     Copyright 2023 The Dragonfly Authors
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

package trainer

import (
	"context"
	"net/http"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/dfpath"
	"d7y.io/dragonfly/v2/trainer/config"
	"d7y.io/dragonfly/v2/trainer/metrics"
	"d7y.io/dragonfly/v2/trainer/rpcserver"
	"d7y.io/dragonfly/v2/trainer/storage"
	"google.golang.org/grpc"
)

type Server struct {
	// Server configuration.
	config *config.Config

	// GRPC server.
	grpcServer *grpc.Server

	// Metrics server.
	metricsServer *http.Server

	// Storage interface.
	storage storage.Storage
}

func New(ctx context.Context, cfg *config.Config, d dfpath.Dfpath) (*Server, error) {
	s := &Server{config: cfg}

	// Initialize Storage.
	s.storage = storage.New(d.DataDir())

	// Initialize trainer grpc server.
	s.grpcServer = rpcserver.New(cfg, s.storage)

	// Initialize metrics.
	if cfg.Metrics.Enable {
		s.metricsServer = metrics.New(&cfg.Metrics, s.grpcServer)
	}

	return s, nil
}

func (s *Server) Serve() error {
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

	return nil
}

func (s *Server) Stop() {
	// Clean storage file.
	if err := s.storage.Clear(); err != nil {
		logger.Errorf("clean storage file failed %s", err.Error())
	} else {
		logger.Info("clean storage file completed")
	}
}
