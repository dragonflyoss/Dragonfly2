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

	"d7y.io/dragonfly/v2/internal/constants"
	"d7y.io/dragonfly/v2/scheduler/config"
)

var (
	// TrafficP2PType is p2p type for traffic metrics
	TrafficP2PType = "p2p"

	// TrafficBackToSourceType is back-to-source type for traffic metrics
	TrafficBackToSourceType = "back_to_source"

	// PeerHostTrafficUploadType is upload traffic type for peer host traffic metrics
	PeerHostTrafficUploadType = "upload"

	// PeerHostTrafficDownloadType is download traffic type for peer host traffic metrics
	PeerHostTrafficDownloadType = "download"
)

// Variables declared for metrics.
var (
	RegisterPeerTaskCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: constants.MetricsNamespace,
		Subsystem: constants.SchedulerMetricsName,
		Name:      "register_peer_task_total",
		Help:      "Counter of the number of the register peer task.",
	}, []string{"biz_tag"})

	RegisterPeerTaskFailureCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: constants.MetricsNamespace,
		Subsystem: constants.SchedulerMetricsName,
		Name:      "register_peer_task_failure_total",
		Help:      "Counter of the number of failed of the register peer task.",
	}, []string{"biz_tag"})

	DownloadCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: constants.MetricsNamespace,
		Subsystem: constants.SchedulerMetricsName,
		Name:      "download_total",
		Help:      "Counter of the number of the downloading.",
	}, []string{"biz_tag"})

	DownloadFailureCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: constants.MetricsNamespace,
		Subsystem: constants.SchedulerMetricsName,
		Name:      "download_failure_total",
		Help:      "Counter of the number of failed of the downloading.",
	}, []string{"biz_tag"})

	LeaveTaskCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: constants.MetricsNamespace,
		Subsystem: constants.SchedulerMetricsName,
		Name:      "leave_task_total",
		Help:      "Counter of the number of the leaving task.",
	}, []string{"biz_tag"})

	LeaveTaskFailureCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: constants.MetricsNamespace,
		Subsystem: constants.SchedulerMetricsName,
		Name:      "leave_task_failure_total",
		Help:      "Counter of the number of failed of the leaving task.",
	}, []string{"biz_tag"})

	Traffic = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: constants.MetricsNamespace,
		Subsystem: constants.SchedulerMetricsName,
		Name:      "traffic",
		Help:      "Counter of the number of traffic.",
	}, []string{"biz_tag", "type"})

	PeerHostTraffic = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: constants.MetricsNamespace,
		Subsystem: constants.SchedulerMetricsName,
		Name:      "peer_host_traffic",
		Help:      "Counter of the number of per peer host traffic.",
	}, []string{"biz_tag", "traffic_type", "peer_host_uuid", "peer_host_ip"})

	PeerTaskCounter = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: constants.MetricsNamespace,
		Subsystem: constants.SchedulerMetricsName,
		Name:      "peer_task_total",
		Help:      "Counter of the number of peer task.",
	}, []string{"biz_tag", "type"})

	PeerTaskDownloadDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: constants.MetricsNamespace,
		Subsystem: constants.SchedulerMetricsName,
		Name:      "peer_task_download_duration_milliseconds",
		Help:      "Histogram of the time each peer task downloading.",
		Buckets:   []float64{100, 200, 500, 1000, 1500, 2 * 1000, 3 * 1000, 5 * 1000, 10 * 1000, 20 * 1000, 60 * 1000, 120 * 1000, 300 * 1000},
	}, []string{"biz_tag"})

	ConcurrentScheduleGauge = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: constants.MetricsNamespace,
		Subsystem: constants.SchedulerMetricsName,
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
