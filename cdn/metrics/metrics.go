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

package metrics

import (
	"context"
	"net"
	"net/http"

	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
	"gopkg.in/yaml.v3"

	"d7y.io/dragonfly/v2/cdn/config"
	"d7y.io/dragonfly/v2/internal/constants"
	logger "d7y.io/dragonfly/v2/internal/dflog"
)

// Variables declared for metrics.
var (
	DownloadCount = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: constants.MetricsNamespace,
		Subsystem: constants.CDNMetricsName,
		Name:      "download_total",
		Help:      "Counter of the number of the downloading.",
	})

	DownloadFailureCount = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: constants.MetricsNamespace,
		Subsystem: constants.CDNMetricsName,
		Name:      "download_failure_total",
		Help:      "Counter of the number of failed of the downloading.",
	})

	DownloadTraffic = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: constants.MetricsNamespace,
		Subsystem: constants.CDNMetricsName,
		Name:      "download_traffic",
		Help:      "Counter of the number of download traffic.",
	})

	ConcurrentDownloadGauge = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: constants.MetricsNamespace,
		Subsystem: constants.CDNMetricsName,
		Name:      "concurrent_download_total",
		Help:      "Gauger of the number of concurrent of the downloading.",
	})
)

type Server struct {
	config     *config.RestConfig
	httpServer *http.Server
}

func New(config *config.RestConfig, rpcServer *grpc.Server) (*Server, error) {
	// scheduler config values
	s, err := yaml.Marshal(config)
	if err != nil {
		return nil, errors.Wrap(err, "marshal metrics server config")
	}
	logger.Infof("metrics server config: \n%s", s)
	grpc_prometheus.Register(rpcServer)

	return &Server{
		config:     config,
		httpServer: &http.Server{},
	}, nil
}

// Handler returns an http handler for the blob server.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	return mux
}

// ListenAndServe is a blocking call which runs s.
func (s *Server) ListenAndServe(h http.Handler) error {
	l, err := net.Listen("tcp", s.config.Addr)
	if err != nil {
		return err
	}
	s.httpServer.Handler = h
	logger.Infof("====starting metrics server at %s====", s.config.Addr)
	err = s.httpServer.Serve(l)
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

func (s *Server) Shutdown(ctx context.Context) error {
	defer logger.Infof("====stopped metrics server====")
	return s.httpServer.Shutdown(ctx)
}
