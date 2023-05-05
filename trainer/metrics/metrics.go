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
	"net/http"

	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"

	"d7y.io/dragonfly/v2/pkg/types"
	"d7y.io/dragonfly/v2/trainer/config"
	"d7y.io/dragonfly/v2/version"
)

// Variables declared for metrics.
var (
	TrainCount = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: types.MetricsNamespace,
		Subsystem: types.TrainerMetricsName,
		Name:      "training_total",
		Help:      "Counter of the number of the training.",
	})

	TrainFailureCount = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: types.MetricsNamespace,
		Subsystem: types.TrainerMetricsName,
		Name:      "training_failure_total",
		Help:      "Counter of the number of failed of the training.",
	})

	VersionGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: types.MetricsNamespace,
		Subsystem: types.TrainerMetricsName,
		Name:      "version",
		Help:      "Version info of the service.",
	}, []string{"major", "minor", "git_version", "git_commit", "platform", "build_time", "go_version", "go_tags", "go_gcflags"})
)

func New(cfg *config.MetricsConfig, svr *grpc.Server) *http.Server {
	grpc_prometheus.Register(svr)

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())

	VersionGauge.WithLabelValues(version.Major, version.Minor, version.GitVersion, version.GitCommit, version.Platform, version.BuildTime, version.GoVersion, version.Gotags, version.Gogcflags).Set(1)
	return &http.Server{
		Addr:    cfg.Addr,
		Handler: mux,
	}
}
