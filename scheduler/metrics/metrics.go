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
	"d7y.io/dragonfly/v2/version"
)

var (
	// HostTrafficUploadType is upload traffic type for host traffic metrics.
	HostTrafficUploadType = "upload"

	// HostTrafficDownloadType is download traffic type for host traffic metrics.
	HostTrafficDownloadType = "download"
)

// Variables declared for metrics.
var (
	AnnouncePeerCount = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: types.MetricsNamespace,
		Subsystem: types.SchedulerMetricsName,
		Name:      "announce_peer_total",
		Help:      "Counter of the number of the announcing peer.",
	})

	AnnouncePeerFailureCount = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: types.MetricsNamespace,
		Subsystem: types.SchedulerMetricsName,
		Name:      "announce_peer_failure_total",
		Help:      "Counter of the number of failed of the announcing peer.",
	})

	StatPeerCount = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: types.MetricsNamespace,
		Subsystem: types.SchedulerMetricsName,
		Name:      "stat_peer_total",
		Help:      "Counter of the number of the stat peer.",
	})

	StatPeerFailureCount = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: types.MetricsNamespace,
		Subsystem: types.SchedulerMetricsName,
		Name:      "stat_peer_failure_total",
		Help:      "Counter of the number of failed of the stat peer.",
	})

	LeavePeerCount = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: types.MetricsNamespace,
		Subsystem: types.SchedulerMetricsName,
		Name:      "leave_peer_total",
		Help:      "Counter of the number of the leaving peer.",
	})

	LeavePeerFailureCount = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: types.MetricsNamespace,
		Subsystem: types.SchedulerMetricsName,
		Name:      "leave_peer_failure_total",
		Help:      "Counter of the number of failed of the leaving peer.",
	})

	ExchangePeerCount = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: types.MetricsNamespace,
		Subsystem: types.SchedulerMetricsName,
		Name:      "exchange_peer_total",
		Help:      "Counter of the number of the exchanging peer.",
	})

	ExchangePeerFailureCount = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: types.MetricsNamespace,
		Subsystem: types.SchedulerMetricsName,
		Name:      "exchange_peer_failure_total",
		Help:      "Counter of the number of failed of the exchanging peer.",
	})

	RegisterPeerCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: types.MetricsNamespace,
		Subsystem: types.SchedulerMetricsName,
		Name:      "register_peer_total",
		Help:      "Counter of the number of the register peer.",
	}, []string{"priority", "task_type", "task_tag", "task_app", "host_type"})

	RegisterPeerFailureCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: types.MetricsNamespace,
		Subsystem: types.SchedulerMetricsName,
		Name:      "register_peer_failure_total",
		Help:      "Counter of the number of failed of the register peer.",
	}, []string{"priority", "task_type", "task_tag", "task_app", "host_type"})

	DownloadPeerStartedCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: types.MetricsNamespace,
		Subsystem: types.SchedulerMetricsName,
		Name:      "download_peer_started_total",
		Help:      "Counter of the number of the download peer started.",
	}, []string{"priority", "task_type", "task_tag", "task_app", "host_type"})

	DownloadPeerStartedFailureCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: types.MetricsNamespace,
		Subsystem: types.SchedulerMetricsName,
		Name:      "download_peer_started_failure_total",
		Help:      "Counter of the number of failed of the download peer started.",
	}, []string{"priority", "task_type", "task_tag", "task_app", "host_type"})

	DownloadPeerBackToSourceStartedCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: types.MetricsNamespace,
		Subsystem: types.SchedulerMetricsName,
		Name:      "download_peer_back_to_source_started_total",
		Help:      "Counter of the number of the download peer back-to-source started.",
	}, []string{"priority", "task_type", "task_tag", "task_app", "host_type"})

	DownloadPeerBackToSourceStartedFailureCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: types.MetricsNamespace,
		Subsystem: types.SchedulerMetricsName,
		Name:      "download_peer_back_to_source_started_failure_total",
		Help:      "Counter of the number of failed of the download peer back-to-source started.",
	}, []string{"priority", "task_type", "task_tag", "task_app", "host_type"})

	DownloadPeerCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: types.MetricsNamespace,
		Subsystem: types.SchedulerMetricsName,
		Name:      "download_peer_finished_total",
		Help:      "Counter of the number of the download peer.",
	}, []string{"priority", "task_type", "task_tag", "task_app", "host_type"})

	DownloadPeerFailureCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: types.MetricsNamespace,
		Subsystem: types.SchedulerMetricsName,
		Name:      "download_peer_finished_failure_total",
		Help:      "Counter of the number of failed of the download peer.",
	}, []string{"priority", "task_type", "task_tag", "task_app", "host_type"})

	DownloadPeerBackToSourceFailureCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: types.MetricsNamespace,
		Subsystem: types.SchedulerMetricsName,
		Name:      "download_peer_back_to_source_finished_failure_total",
		Help:      "Counter of the number of failed of the download peer back-to-source.",
	}, []string{"priority", "task_type", "task_tag", "task_app", "host_type"})

	DownloadPieceCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: types.MetricsNamespace,
		Subsystem: types.SchedulerMetricsName,
		Name:      "download_piece_finished_total",
		Help:      "Counter of the number of the download piece.",
	}, []string{"traffic_type", "task_type", "task_tag", "task_app", "host_type"})

	DownloadPieceFailureCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: types.MetricsNamespace,
		Subsystem: types.SchedulerMetricsName,
		Name:      "download_piece_finished_failure_total",
		Help:      "Counter of the number of failed of the download piece.",
	}, []string{"traffic_type", "task_type", "task_tag", "task_app", "host_type"})

	DownloadPieceBackToSourceFailureCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: types.MetricsNamespace,
		Subsystem: types.SchedulerMetricsName,
		Name:      "download_piece_back_to_source_finished_failure_total",
		Help:      "Counter of the number of failed of the download piece back-to-source.",
	}, []string{"traffic_type", "task_type", "task_tag", "task_app", "host_type"})

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

	AnnounceHostCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: types.MetricsNamespace,
		Subsystem: types.SchedulerMetricsName,
		Name:      "announce_host_total",
		Help:      "Counter of the number of the announce host.",
	}, []string{"os", "platform", "platform_family", "platform_version",
		"kernel_version", "git_version", "git_commit", "go_version", "build_platform"})

	AnnounceHostFailureCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: types.MetricsNamespace,
		Subsystem: types.SchedulerMetricsName,
		Name:      "announce_host_failure_total",
		Help:      "Counter of the number of failed of the announce host.",
	}, []string{"os", "platform", "platform_family", "platform_version",
		"kernel_version", "git_version", "git_commit", "go_version", "build_platform"})

	LeaveHostCount = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: types.MetricsNamespace,
		Subsystem: types.SchedulerMetricsName,
		Name:      "leave_host_total",
		Help:      "Counter of the number of the leaving host.",
	})

	LeaveHostFailureCount = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: types.MetricsNamespace,
		Subsystem: types.SchedulerMetricsName,
		Name:      "leave_host_failure_total",
		Help:      "Counter of the number of failed of the leaving host.",
	})

	SyncProbesCount = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: types.MetricsNamespace,
		Subsystem: types.SchedulerMetricsName,
		Name:      "sync_probes_total",
		Help:      "Counter of the number of the synchronizing probes.",
	})

	SyncProbesFailureCount = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: types.MetricsNamespace,
		Subsystem: types.SchedulerMetricsName,
		Name:      "sync_probes_failure_total",
		Help:      "Counter of the number of failed of the synchronizing probes.",
	})

	Traffic = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: types.MetricsNamespace,
		Subsystem: types.SchedulerMetricsName,
		Name:      "traffic",
		Help:      "Counter of the number of traffic.",
	}, []string{"type", "task_type", "task_tag", "task_app", "host_type"})

	HostTraffic = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: types.MetricsNamespace,
		Subsystem: types.SchedulerMetricsName,
		Name:      "host_traffic",
		Help:      "Counter of the number of per host traffic.",
	}, []string{"type", "task_type", "task_tag", "task_app", "host_type", "host_id", "host_ip", "host_name"})

	DownloadPeerDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: types.MetricsNamespace,
		Subsystem: types.SchedulerMetricsName,
		Name:      "download_peer_duration_milliseconds",
		Help:      "Histogram of the time each peer downloading.",
		Buckets:   []float64{100, 200, 500, 1000, 1500, 2 * 1000, 3 * 1000, 5 * 1000, 10 * 1000, 20 * 1000, 60 * 1000, 120 * 1000, 300 * 1000},
	}, []string{"priority", "task_type", "task_tag", "task_app", "host_type"})

	ConcurrentScheduleGauge = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: types.MetricsNamespace,
		Subsystem: types.SchedulerMetricsName,
		Name:      "concurrent_schedule_total",
		Help:      "Gauge of the number of concurrent of the scheduling.",
	})

	VersionGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: types.MetricsNamespace,
		Subsystem: types.SchedulerMetricsName,
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
