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

	// DownloadFailureBackToSourceType is back-to-source type for download failure count metrics.
	DownloadFailureBackToSourceType = "back_to_source"

	// DownloadFailureP2PType is p2p type for download failure count metrics.
	DownloadFailureP2PType = "p2p"
)

// Variables declared for metrics.
var (
	AnnouncePeerCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: types.MetricsNamespace,
		Subsystem: types.SchedulerMetricsName,
		Name:      "announce_peer_total",
		Help:      "Counter of the number of the announcing peer.",
	}, []string{"tag", "app"})

	AnnouncePeerFailureCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: types.MetricsNamespace,
		Subsystem: types.SchedulerMetricsName,
		Name:      "announce_peer_failure_total",
		Help:      "Counter of the number of failed of the announcing peer.",
	}, []string{"tag", "app"})

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
		Help:      "Counter of the number of the leaving peer.",
	})

	ExchangePeerFailureCount = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: types.MetricsNamespace,
		Subsystem: types.SchedulerMetricsName,
		Name:      "exchange_peer_failure_total",
		Help:      "Counter of the number of failed of the leaving peer.",
	})

	RegisterTaskCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: types.MetricsNamespace,
		Subsystem: types.SchedulerMetricsName,
		Name:      "register_task_total",
		Help:      "Counter of the number of the register task.",
	}, []string{"tag", "app"})

	RegisterTaskFailureCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: types.MetricsNamespace,
		Subsystem: types.SchedulerMetricsName,
		Name:      "register_task_failure_total",
		Help:      "Counter of the number of failed of the register task.",
	}, []string{"tag", "app"})

	DownloadTaskCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: types.MetricsNamespace,
		Subsystem: types.SchedulerMetricsName,
		Name:      "download_task_total",
		Help:      "Counter of the number of the task downloading.",
	}, []string{"tag", "app", "host_type"})

	DownloadTaskFailureCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: types.MetricsNamespace,
		Subsystem: types.SchedulerMetricsName,
		Name:      "download_task_failure_total",
		Help:      "Counter of the number of failed of the task downloading.",
	}, []string{"tag", "app", "type", "code", "host_type"})

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

	AnnounceTaskCount = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: types.MetricsNamespace,
		Subsystem: types.SchedulerMetricsName,
		Name:      "announce_task_total",
		Help:      "Counter of the number of the announce task.",
	})

	AnnounceTaskFailureCount = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: types.MetricsNamespace,
		Subsystem: types.SchedulerMetricsName,
		Name:      "announce_task_failure_total",
		Help:      "Counter of the number of failed of the announce task.",
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

	LeaveTaskCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: types.MetricsNamespace,
		Subsystem: types.SchedulerMetricsName,
		Name:      "leave_task_total",
		Help:      "Counter of the number of the leaving task.",
	}, []string{"tag", "app", "host_type"})

	LeaveTaskFailureCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: types.MetricsNamespace,
		Subsystem: types.SchedulerMetricsName,
		Name:      "leave_task_failure_total",
		Help:      "Counter of the number of failed of the leaving task.",
	}, []string{"tag", "app", "host_type"})

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

	Traffic = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: types.MetricsNamespace,
		Subsystem: types.SchedulerMetricsName,
		Name:      "traffic",
		Help:      "Counter of the number of traffic.",
	}, []string{"task_tag", "task_app", "type"})

	HostTraffic = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: types.MetricsNamespace,
		Subsystem: types.SchedulerMetricsName,
		Name:      "host_traffic",
		Help:      "Counter of the number of per host traffic.",
	}, []string{"task_tag", "task_app", "type", "host_id", "host_ip"})

	DownloadTaskDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: types.MetricsNamespace,
		Subsystem: types.SchedulerMetricsName,
		Name:      "download_task_duration_milliseconds",
		Help:      "Histogram of the time each task downloading.",
		Buckets:   []float64{100, 200, 500, 1000, 1500, 2 * 1000, 3 * 1000, 5 * 1000, 10 * 1000, 20 * 1000, 60 * 1000, 120 * 1000, 300 * 1000},
	}, []string{"tag", "app", "host_type"})

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
