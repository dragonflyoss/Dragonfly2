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
	"d7y.io/dragonfly/v2/scheduler/config"
)

var (
	// TrafficP2PType is p2p type for traffic metrics.
	TrafficP2PType = "p2p"

	// TrafficBackToSourceType is back-to-source type for traffic metrics.
	TrafficBackToSourceType = "back_to_source"

	// PeerHostTrafficUploadType is upload traffic type for peer host traffic metrics.
	PeerHostTrafficUploadType = "upload"

	// PeerHostTrafficDownloadType is download traffic type for peer host traffic metrics.
	PeerHostTrafficDownloadType = "download"

	// DownloadFailureBackToSourceType is back-to-source type for download failure count metrics.
	DownloadFailureBackToSourceType = "back_to_source"

	// DownloadFailureP2PType is p2p type for download failure count metrics.
	DownloadFailureP2PType = "p2p"
)

// Variables declared for metrics.
var (
	RegisterPeerTaskCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: types.MetricsNamespace,
		Subsystem: types.SchedulerMetricsName,
		Name:      "register_peer_task_total",
		Help:      "Counter of the number of the register peer task.",
	}, []string{"tag", "app"})

	RegisterPeerTaskFailureCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: types.MetricsNamespace,
		Subsystem: types.SchedulerMetricsName,
		Name:      "register_peer_task_failure_total",
		Help:      "Counter of the number of failed of the register peer task.",
	}, []string{"tag", "app"})

	DownloadCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: types.MetricsNamespace,
		Subsystem: types.SchedulerMetricsName,
		Name:      "download_total",
		Help:      "Counter of the number of the downloading.",
	}, []string{"tag", "app"})

	DownloadFailureCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: types.MetricsNamespace,
		Subsystem: types.SchedulerMetricsName,
		Name:      "download_failure_total",
		Help:      "Counter of the number of failed of the downloading.",
	}, []string{"tag", "app", "type"})

	StatTaskCount = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: types.MetricsNamespace,
		Subsystem: types.SchedulerMetricsName,
		Name:      "stat_task_total",
		Help:      "Counter of the number of the stat task.",
	})

	StatTaskFailureCount = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: types.MetricsNamespace,
		Subsystem: types.SchedulerMetricsName,
		Name:      "stat_task_failure_total",
		Help:      "Counter of the number of failed of the stat task.",
	})

	AnnounceCount = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: types.MetricsNamespace,
		Subsystem: types.SchedulerMetricsName,
		Name:      "announce_task_total",
		Help:      "Counter of the number of the announce task.",
	})

	AnnounceFailureCount = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: types.MetricsNamespace,
		Subsystem: types.SchedulerMetricsName,
		Name:      "announce_task_failure_total",
		Help:      "Counter of the number of failed of the announce task.",
	})

	LeaveTaskCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: types.MetricsNamespace,
		Subsystem: types.SchedulerMetricsName,
		Name:      "leave_task_total",
		Help:      "Counter of the number of the leaving task.",
	}, []string{"tag", "app"})

	LeaveTaskFailureCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: types.MetricsNamespace,
		Subsystem: types.SchedulerMetricsName,
		Name:      "leave_task_failure_total",
		Help:      "Counter of the number of failed of the leaving task.",
	}, []string{"tag", "app"})

	Traffic = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: types.MetricsNamespace,
		Subsystem: types.SchedulerMetricsName,
		Name:      "traffic",
		Help:      "Counter of the number of traffic.",
	}, []string{"tag", "app", "type"})

	PeerHostTraffic = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: types.MetricsNamespace,
		Subsystem: types.SchedulerMetricsName,
		Name:      "peer_host_traffic",
		Help:      "Counter of the number of per peer host traffic.",
	}, []string{"tag", "app", "traffic_type", "peer_host_id", "peer_host_ip"})

	PeerTaskCounter = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: types.MetricsNamespace,
		Subsystem: types.SchedulerMetricsName,
		Name:      "peer_task_total",
		Help:      "Counter of the number of peer task.",
	}, []string{"tag", "app", "type"})

	PeerTaskSourceErrorCounter = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: types.MetricsNamespace,
		Subsystem: types.SchedulerMetricsName,
		Name:      "peer_task_source_error_total",
		Help:      "Counter of the source error code number of peer task.",
	}, []string{"tag", "app", "protocol", "code"})

	PeerTaskDownloadDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: types.MetricsNamespace,
		Subsystem: types.SchedulerMetricsName,
		Name:      "peer_task_download_duration_milliseconds",
		Help:      "Histogram of the time each peer task downloading.",
		Buckets:   []float64{100, 200, 500, 1000, 1500, 2 * 1000, 3 * 1000, 5 * 1000, 10 * 1000, 20 * 1000, 60 * 1000, 120 * 1000, 300 * 1000},
	}, []string{"tag", "app"})

	ConcurrentScheduleGauge = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: types.MetricsNamespace,
		Subsystem: types.SchedulerMetricsName,
		Name:      "concurrent_schedule_total",
		Help:      "Gauge of the number of concurrent of the scheduling.",
	})
)

func New(cfg *config.MetricsConfig, svr *grpc.Server) *http.Server {
	grpc_prometheus.Register(svr)

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())

	return &http.Server{
		Addr:    cfg.Addr,
		Handler: mux,
	}
}
