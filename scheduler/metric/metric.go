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

package metric

import (
	"net/http"

	"d7y.io/dragonfly/v2/internal/constants"
	"d7y.io/dragonfly/v2/scheduler/config"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
)

// Variables declared for metrics.
var (
	RegisterPeerTaskCount = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: constants.MetricNamespace,
		Subsystem: constants.SchedulerMetricName,
		Name:      "schedule_total",
		Help:      "Counter of the number of the register peer task.",
	})

	RegisterPeerTaskFailureCount = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: constants.MetricNamespace,
		Subsystem: constants.SchedulerMetricName,
		Name:      "register_peer_task_failure_total",
		Help:      "Counter of the number of failed of the register peer task.",
	})

	P2PTraffic = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: constants.MetricNamespace,
		Subsystem: constants.SchedulerMetricName,
		Name:      "p2p_traffic",
		Help:      "Counter of the number of p2p traffic.",
	})

	TinyPeerTaskCounter = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: constants.MetricNamespace,
		Subsystem: constants.SchedulerMetricName,
		Name:      "tiny_peer_task_total",
		Help:      "Counter of the number of tiny peer task.",
	})

	SmallPeerTaskCounter = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: constants.MetricNamespace,
		Subsystem: constants.SchedulerMetricName,
		Name:      "small_peer_task_total",
		Help:      "Counter of the number of small peer task.",
	})

	NormalPeerTaskCounter = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: constants.MetricNamespace,
		Subsystem: constants.SchedulerMetricName,
		Name:      "normal_peer_task_total",
		Help:      "Counter of the number of normal peer task.",
	})

	ConcurrentScheduleGauge = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: constants.MetricNamespace,
		Subsystem: constants.SchedulerMetricName,
		Name:      "concurrent_schedule_total",
		Help:      "Gauger of the number of concurrent of the scheduling.",
	})
)

func New(cfg *config.RestConfig, grpcServer *grpc.Server) *http.Server {
	grpc_prometheus.Register(grpcServer)

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())

	return &http.Server{
		Addr:    cfg.Addr,
		Handler: mux,
	}
}
