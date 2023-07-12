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
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/dfpath"
	"d7y.io/dragonfly/v2/pkg/net/ip"
	"d7y.io/dragonfly/v2/pkg/rpc"
	managerclient "d7y.io/dragonfly/v2/pkg/rpc/manager/client"
	"d7y.io/dragonfly/v2/trainer/config"
	"d7y.io/dragonfly/v2/trainer/metrics"
	"d7y.io/dragonfly/v2/trainer/rpcserver"
	"d7y.io/dragonfly/v2/trainer/storage"
	"d7y.io/dragonfly/v2/trainer/training"
)

const (
	// gracefulStopTimeout specifies a time limit for
	// grpc server to complete a graceful shutdown.
	gracefulStopTimeout = 10 * time.Minute
)

// Server is the trainer server.
type Server struct {
	// Server configuration.
	config *config.Config

	// GRPC server.
	grpcServer *grpc.Server

	// Metrics server.
	metricsServer *http.Server

	// Manager client.
	managerClient managerclient.V2

	// Storage interface.
	storage storage.Storage
}

// New creates a new Server.
func New(ctx context.Context, cfg *config.Config, d dfpath.Dfpath) (*Server, error) {
	s := &Server{config: cfg}

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

	// Initialize Storage.
	s.storage = storage.New(d.DataDir())

	// Initialize Training.
	training := training.New(cfg, s.managerClient, s.storage)

	// Initialize trainer grpc server.
	s.grpcServer = rpcserver.New(cfg, s.storage, training)

	// Initialize metrics.
	if cfg.Metrics.Enable {
		s.metricsServer = metrics.New(&cfg.Metrics, s.grpcServer)
	}

	return s, nil
}

// Serve starts the trainer server.
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

// Stop stops the trainer server.
func (s *Server) Stop() {
	// Stop manager client.
	if s.managerClient != nil {
		if err := s.managerClient.Close(); err != nil {
			logger.Errorf("manager client failed to stop: %s", err.Error())
		} else {
			logger.Info("manager client closed")
		}
	}

	// Clean storage file.
	if err := s.storage.Clear(); err != nil {
		logger.Errorf("clean storage file failed %s", err.Error())
	} else {
		logger.Info("clean storage file completed")
	}

	// Stop metrics server.
	if s.metricsServer != nil {
		if err := s.metricsServer.Shutdown(context.Background()); err != nil {
			logger.Errorf("metrics server failed to stop: %s", err.Error())
		} else {
			logger.Info("metrics server closed under request")
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
