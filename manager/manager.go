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
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"net"
	"net/http"
	"time"

	"github.com/gin-contrib/static"
	"google.golang.org/grpc"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/internal/ratelimiter"
	"d7y.io/dragonfly/v2/manager/cache"
	"d7y.io/dragonfly/v2/manager/config"
	"d7y.io/dragonfly/v2/manager/database"
	"d7y.io/dragonfly/v2/manager/job"
	"d7y.io/dragonfly/v2/manager/metrics"
	"d7y.io/dragonfly/v2/manager/permission/rbac"
	"d7y.io/dragonfly/v2/manager/router"
	"d7y.io/dragonfly/v2/manager/rpcserver"
	"d7y.io/dragonfly/v2/manager/searcher"
	"d7y.io/dragonfly/v2/manager/service"
	"d7y.io/dragonfly/v2/pkg/dfpath"
	"d7y.io/dragonfly/v2/pkg/net/ip"
	"d7y.io/dragonfly/v2/pkg/objectstorage"
	"d7y.io/dragonfly/v2/pkg/rpc"
)

const (
	// gracefulStopTimeout specifies a time limit for
	// grpc server to complete a graceful shutdown.
	gracefulStopTimeout = 10 * time.Minute

	// assetsTargetPath is target path of embed assets.
	assetsTargetPath = "dist"
)

//go:embed dist/*
var assets embed.FS

type embedFileSystem struct {
	http.FileSystem
}

func (e embedFileSystem) Exists(prefix string, path string) bool {
	_, err := e.Open(path)
	if err != nil {
		return false
	}
	return true
}

func EmbedFolder(fsEmbed embed.FS, targetPath string) static.ServeFileSystem {
	fsys, err := fs.Sub(fsEmbed, targetPath)
	if err != nil {
		panic(err)
	}

	return embedFileSystem{
		FileSystem: http.FS(fsys),
	}
}

// Server is the manager server.
type Server struct {
	// Server configuration.
	config *config.Config

	// Job server.
	job *job.Job

	// Job rate limiter.
	jobRateLimiter ratelimiter.JobRateLimiter

	// GRPC server.
	grpcServer *grpc.Server

	// REST server.
	restServer *http.Server

	// Metrics server.
	metricsServer *http.Server
}

// New creates a new manager server.
func New(cfg *config.Config, d dfpath.Dfpath) (*Server, error) {
	s := &Server{config: cfg}

	// Initialize database.
	db, err := database.New(cfg)
	if err != nil {
		return nil, err
	}

	// Initialize enforcer.
	enforcer, err := rbac.NewEnforcer(db.DB)
	if err != nil {
		return nil, err
	}

	// Initialize cache.
	cache, err := cache.New(cfg)
	if err != nil {
		return nil, err
	}

	// Initialize searcher.
	searcher := searcher.New(d.PluginDir())

	// Initialize job.
	job, err := job.New(cfg, db.DB)
	if err != nil {
		return nil, err
	}
	s.job = job

	// Initialize object storage.
	var objectStorage objectstorage.ObjectStorage
	if cfg.ObjectStorage.Enable {
		objectStorage, err = objectstorage.New(
			cfg.ObjectStorage.Name,
			cfg.ObjectStorage.Region,
			cfg.ObjectStorage.Endpoint,
			cfg.ObjectStorage.AccessKey,
			cfg.ObjectStorage.SecretKey,
			objectstorage.WithS3ForcePathStyle(cfg.ObjectStorage.S3ForcePathStyle),
		)
		if err != nil {
			return nil, err
		}
	}

	// Initialize job rate limiter.
	s.jobRateLimiter, err = ratelimiter.NewJobRateLimiter(db)
	if err != nil {
		return nil, err
	}

	// Initialize REST server.
	restService := service.New(cfg, db, cache, job, enforcer, objectStorage)
	router, err := router.Init(cfg, d.LogDir(), restService, db, enforcer, s.jobRateLimiter, EmbedFolder(assets, assetsTargetPath))
	if err != nil {
		return nil, err
	}
	s.restServer = &http.Server{
		Addr:    cfg.Server.REST.Addr,
		Handler: router,
	}

	// Initialize roles and check roles.
	err = rbac.InitRBAC(enforcer, router, db.DB)
	if err != nil {
		return nil, err
	}

	// Initialize signing certificate and tls credentials of grpc server.
	var options []grpc.ServerOption
	if cfg.Server.GRPC.TLS != nil {
		// Initialize grpc server with tls.
		transportCredentials, err := rpc.NewServerCredentials(cfg.Server.GRPC.TLS.CACert, cfg.Server.GRPC.TLS.Cert, cfg.Server.GRPC.TLS.Key)
		if err != nil {
			logger.Errorf("failed to create server credentials: %v", err)
			return nil, err
		}

		options = append(options, grpc.Creds(transportCredentials))
	} else {
		// Initialize grpc server without tls.
		options = append(options, grpc.Creds(rpc.NewInsecureCredentials()))
	}

	// Initialize GRPC server.
	_, grpcServer, err := rpcserver.New(cfg, db, cache, searcher, objectStorage, options...)
	if err != nil {
		return nil, err
	}

	s.grpcServer = grpcServer

	// Initialize prometheus.
	if cfg.Metrics.Enable {
		s.metricsServer = metrics.New(&cfg.Metrics, grpcServer)
	}

	return s, nil
}

// Serve starts the manager server.
func (s *Server) Serve() error {
	// Started REST server.
	go func() {
		logger.Infof("started rest server at %s", s.restServer.Addr)
		if s.config.Server.REST.TLS != nil {
			if err := s.restServer.ListenAndServeTLS(s.config.Server.REST.TLS.Cert, s.config.Server.REST.TLS.Key); err != nil {
				if err == http.ErrServerClosed {
					return
				}
				logger.Fatalf("rest server closed unexpect: %v", err)
			}
		} else {
			if err := s.restServer.ListenAndServe(); err != nil {
				if err == http.ErrServerClosed {
					return
				}
				logger.Fatalf("rest server closed unexpect: %v", err)
			}
		}
	}()

	// Started metrics server.
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

	// Started job server.
	go func() {
		logger.Info("started job server")
		s.job.Serve()
	}()

	// Started job rate limiter server.
	go func() {
		logger.Infof("started job rate limiter server")
		s.jobRateLimiter.Serve()
	}()

	// Generate GRPC listener.
	ip, ok := ip.FormatIP(s.config.Server.GRPC.ListenIP.String())
	if !ok {
		return errors.New("format ip failed")
	}

	listener, err := net.Listen("tcp", fmt.Sprintf("%s:%d", ip, s.config.Server.GRPC.Port.Start))
	if err != nil {
		logger.Fatalf("net listener failed to start: %v", err)
	}
	defer listener.Close()

	// Started GRPC server.
	logger.Infof("started grpc server at %s://%s", listener.Addr().Network(), listener.Addr().String())
	if err := s.grpcServer.Serve(listener); err != nil {
		logger.Errorf("stoped grpc server: %+v", err)
		return err
	}

	return nil
}

// Stop stops the manager server.
func (s *Server) Stop() {
	// Stop REST server.
	if err := s.restServer.Shutdown(context.Background()); err != nil {
		logger.Errorf("rest server failed to stop: %+v", err)
	} else {
		logger.Info("rest server closed under request")
	}

	// Stop metrics server.
	if s.metricsServer != nil {
		if err := s.metricsServer.Shutdown(context.Background()); err != nil {
			logger.Errorf("metrics server failed to stop: %+v", err)
		} else {
			logger.Info("metrics server closed under request")
		}
	}

	// Stop job server.
	s.job.Stop()

	// Stop job rate limiter.
	s.jobRateLimiter.Stop()

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
