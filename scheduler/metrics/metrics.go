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
	}, []string{"priority", "task_type", "host_type"})

	RegisterPeerFailureCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: types.MetricsNamespace,
		Subsystem: types.SchedulerMetricsName,
		Name:      "register_peer_failure_total",
		Help:      "Counter of the number of failed of the register peer.",
	}, []string{"priority", "task_type", "host_type"})

	DownloadPeerStartedCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: types.MetricsNamespace,
		Subsystem: types.SchedulerMetricsName,
		Name:      "download_peer_started_total",
		Help:      "Counter of the number of the download peer started.",
	}, []string{"priority", "task_type", "host_type"})

	DownloadPeerStartedFailureCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: types.MetricsNamespace,
		Subsystem: types.SchedulerMetricsName,
		Name:      "download_peer_started_failure_total",
		Help:      "Counter of the number of failed of the download peer started.",
	}, []string{"priority", "task_type", "host_type"})

	DownloadPeerBackToSourceStartedCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: types.MetricsNamespace,
		Subsystem: types.SchedulerMetricsName,
		Name:      "download_peer_back_to_source_started_total",
		Help:      "Counter of the number of the download peer back-to-source started.",
	}, []string{"priority", "task_type", "host_type"})

	DownloadPeerBackToSourceStartedFailureCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: types.MetricsNamespace,
		Subsystem: types.SchedulerMetricsName,
		Name:      "download_peer_back_to_source_started_failure_total",
		Help:      "Counter of the number of failed of the download peer back-to-source started.",
	}, []string{"priority", "task_type", "host_type"})

	DownloadPeerCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: types.MetricsNamespace,
		Subsystem: types.SchedulerMetricsName,
		Name:      "download_peer_finished_total",
		Help:      "Counter of the number of the download peer.",
	}, []string{"priority", "task_type", "host_type"})

	DownloadPeerFailureCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: types.MetricsNamespace,
		Subsystem: types.SchedulerMetricsName,
		Name:      "download_peer_finished_failure_total",
		Help:      "Counter of the number of failed of the download peer.",
	}, []string{"priority", "task_type", "host_type"})

	DownloadPeerBackToSourceFailureCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: types.MetricsNamespace,
		Subsystem: types.SchedulerMetricsName,
		Name:      "download_peer_back_to_source_finished_failure_total",
		Help:      "Counter of the number of failed of the download peer back-to-source.",
	}, []string{"priority", "task_type", "host_type"})

	DownloadPieceCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: types.MetricsNamespace,
		Subsystem: types.SchedulerMetricsName,
		Name:      "download_piece_finished_total",
		Help:      "Counter of the number of the download piece.",
	}, []string{"traffic_type", "task_type", "host_type"})

	DownloadPieceFailureCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: types.MetricsNamespace,
		Subsystem: types.SchedulerMetricsName,
		Name:      "download_piece_finished_failure_total",
		Help:      "Counter of the number of failed of the download piece.",
	}, []string{"traffic_type", "task_type", "host_type"})

	DownloadPieceBackToSourceFailureCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: types.MetricsNamespace,
		Subsystem: types.SchedulerMetricsName,
		Name:      "download_piece_back_to_source_finished_failure_total",
		Help:      "Counter of the number of failed of the download piece back-to-source.",
	}, []string{"traffic_type", "task_type", "host_type"})

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

	LeaveTaskCount = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: types.MetricsNamespace,
		Subsystem: types.SchedulerMetricsName,
		Name:      "leave_task_total",
		Help:      "Counter of the number of the leaving task.",
	})

	LeaveTaskFailureCount = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: types.MetricsNamespace,
		Subsystem: types.SchedulerMetricsName,
		Name:      "leave_task_failure_total",
		Help:      "Counter of the number of failed of the leaving task.",
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
	}, []string{"type", "task_type", "host_type"})

	HostTraffic = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: types.MetricsNamespace,
		Subsystem: types.SchedulerMetricsName,
		Name:      "host_traffic",
		Help:      "Counter of the number of per host traffic.",
	}, []string{"type", "task_type", "host_type", "host_id", "host_ip", "host_name"})

	DownloadPeerDuration = promauto.NewSummaryVec(prometheus.SummaryOpts{
		Namespace:  types.MetricsNamespace,
		Subsystem:  types.SchedulerMetricsName,
		Name:       "download_peer_duration_milliseconds",
		Help:       "Summary of the time each peer downloading.",
		Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.95: 0.005, 0.99: 0.001},
	}, []string{"task_size_level"})

	ConcurrentScheduleGauge = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: types.MetricsNamespace,
		Subsystem: types.SchedulerMetricsName,
		Name:      "concurrent_schedule_total",
		Help:      "Gauge of the number of concurrent of the scheduling.",
	})

	ScheduleDuration = promauto.NewSummary(prometheus.SummaryOpts{
		Namespace:  types.MetricsNamespace,
		Subsystem:  types.SchedulerMetricsName,
		Name:       "schedule_duration_milliseconds",
		Help:       "Summary of the time each scheduling.",
		Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.95: 0.005, 0.99: 0.001},
	})

	AnnouncePersistentCachePeerCount = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: types.MetricsNamespace,
		Subsystem: types.SchedulerMetricsName,
		Name:      "announce_persistent_cache_peer_total",
		Help:      "Counter of the number of the announcing cache peer.",
	})

	AnnouncePersistentCachePeerFailureCount = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: types.MetricsNamespace,
		Subsystem: types.SchedulerMetricsName,
		Name:      "announce_persistent_cache_peer_failure_total",
		Help:      "Counter of the number of failed of the announcing cache peer.",
	})

	StatPersistentCachePeerCount = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: types.MetricsNamespace,
		Subsystem: types.SchedulerMetricsName,
		Name:      "stat_persistent_cache_peer_total",
		Help:      "Counter of the number of the stat cache peer.",
	})

	StatPersistentCachePeerFailureCount = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: types.MetricsNamespace,
		Subsystem: types.SchedulerMetricsName,
		Name:      "stat_persistent_cache_peer_failure_total",
		Help:      "Counter of the number of failed of the stat cache peer.",
	})

	DeletePersistentCachePeerCount = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: types.MetricsNamespace,
		Subsystem: types.SchedulerMetricsName,
		Name:      "delete_persistent_cache_peer_total",
		Help:      "Counter of the number of the deleting cache peer.",
	})

	DeletePersistentCachePeerFailureCount = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: types.MetricsNamespace,
		Subsystem: types.SchedulerMetricsName,
		Name:      "delete_persistent_cache_peer_failure_total",
		Help:      "Counter of the number of failed of the deleting cache peer.",
	})

	UploadPersistentCacheTaskStartedCount = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: types.MetricsNamespace,
		Subsystem: types.SchedulerMetricsName,
		Name:      "upload_persistent_cache_task_started_total",
		Help:      "Counter of the number of the started uploading cache peer.",
	})

	UploadPersistentCacheTaskStartedFailureCount = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: types.MetricsNamespace,
		Subsystem: types.SchedulerMetricsName,
		Name:      "upload_persistent_cache_task_started_failure_total",
		Help:      "Counter of the number of failed of the started uploading cache peer.",
	})

	UploadPersistentCacheTaskFinishedCount = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: types.MetricsNamespace,
		Subsystem: types.SchedulerMetricsName,
		Name:      "upload_persistent_cache_task_finished_total",
		Help:      "Counter of the number of the finished uploading cache peer.",
	})

	UploadPersistentCacheTaskFinishedFailureCount = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: types.MetricsNamespace,
		Subsystem: types.SchedulerMetricsName,
		Name:      "upload_persistent_cache_task_finished_failure_total",
		Help:      "Counter of the number of failed of the finished uploading cache peer.",
	})

	UploadPersistentCacheTaskFailedCount = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: types.MetricsNamespace,
		Subsystem: types.SchedulerMetricsName,
		Name:      "upload_persistent_cache_task_failed_total",
		Help:      "Counter of the number of the failed uploading cache peer.",
	})

	UploadPersistentCacheTaskFailedFailureCount = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: types.MetricsNamespace,
		Subsystem: types.SchedulerMetricsName,
		Name:      "upload_cache_peer_failed_failure_total",
		Help:      "Counter of the number of failed of the failed uploading cache peer.",
	})

	StatPersistentCacheTaskCount = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: types.MetricsNamespace,
		Subsystem: types.SchedulerMetricsName,
		Name:      "stat_persistent_cache_task_total",
		Help:      "Counter of the number of the stat cache task.",
	})

	StatPersistentCacheTaskFailureCount = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: types.MetricsNamespace,
		Subsystem: types.SchedulerMetricsName,
		Name:      "stat_persistent_cache_task_failure_total",
		Help:      "Counter of the number of failed of the stat cache task.",
	})

	DeletePersistentCacheTaskCount = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: types.MetricsNamespace,
		Subsystem: types.SchedulerMetricsName,
		Name:      "delete_persistent_cache_task_total",
		Help:      "Counter of the number of the delete cache task.",
	})

	DeletePersistentCacheTaskFailureCount = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: types.MetricsNamespace,
		Subsystem: types.SchedulerMetricsName,
		Name:      "delete_persistent_cache_task_failure_total",
		Help:      "Counter of the number of failed of the delete cache task.",
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

// TaskSizeLevel is the level of the task size.
type TaskSizeLevel int

// String returns the string representation of the TaskSizeLevel.
func (t TaskSizeLevel) String() string {
	switch t {
	case TaskSizeLevel0:
		return "0"
	case TaskSizeLevel1:
		return "1"
	case TaskSizeLevel2:
		return "2"
	case TaskSizeLevel3:
		return "3"
	case TaskSizeLevel4:
		return "4"
	case TaskSizeLevel5:
		return "5"
	case TaskSizeLevel6:
		return "6"
	case TaskSizeLevel7:
		return "7"
	case TaskSizeLevel8:
		return "8"
	case TaskSizeLevel9:
		return "9"
	case TaskSizeLevel10:
		return "10"
	case TaskSizeLevel11:
		return "11"
	case TaskSizeLevel12:
		return "12"
	case TaskSizeLevel13:
		return "13"
	case TaskSizeLevel14:
		return "14"
	case TaskSizeLevel15:
		return "15"
	case TaskSizeLevel16:
		return "16"
	case TaskSizeLevel17:
		return "17"
	case TaskSizeLevel18:
		return "18"
	case TaskSizeLevel19:
		return "19"
	case TaskSizeLevel20:
		return "20"
	default:
		return "0"
	}
}

const (
	// TaskSizeLevel0 represents unknown size.
	TaskSizeLevel0 TaskSizeLevel = iota

	// TaskSizeLevel1 represents size range is from 0 to 1M.
	TaskSizeLevel1

	// TaskSizeLevel2 represents size range is from 1M to 4M.
	TaskSizeLevel2

	// TaskSizeLevel3 represents size range is from 4M to 8M.
	TaskSizeLevel3

	// TaskSizeLevel4 represents size range is from 8M to 16M.
	TaskSizeLevel4

	// TaskSizeLevel5 represents size range is from 16M to 32M.
	TaskSizeLevel5

	// TaskSizeLevel6 represents size range is from 32M to 64M.
	TaskSizeLevel6

	// TaskSizeLevel7 represents size range is from 64M to 128M.
	TaskSizeLevel7

	// TaskSizeLevel8 represents size range is from 128M to 256M.
	TaskSizeLevel8

	// TaskSizeLevel9 represents size range is from 256M to 512M.
	TaskSizeLevel9

	// TaskSizeLevel10 represents size range is from 512M to 1G.
	TaskSizeLevel10

	// TaskSizeLevel11 represents size range is from 1G to 4G.
	TaskSizeLevel11

	// TaskSizeLevel12 represents size range is from 4G to 8G.
	TaskSizeLevel12

	// TaskSizeLevel13 represents size range is from 8G to 16G.
	TaskSizeLevel13

	// TaskSizeLevel14 represents size range is from 16G to 32G.
	TaskSizeLevel14

	// TaskSizeLevel15 represents size range is from 32G to 64G.
	TaskSizeLevel15

	// TaskSizeLevel16 represents size range is from 64G to 128G.
	TaskSizeLevel16

	// TaskSizeLevel17 represents size range is from 128G to 256G.
	TaskSizeLevel17

	// TaskSizeLevel18 represents size range is from 256G to 512G.
	TaskSizeLevel18

	// TaskSizeLevel19 represents size range is from 512G to 1T.
	TaskSizeLevel19

	// TaskSizeLevel20 represents size is greater than 1T.
	TaskSizeLevel20
)

const (
	Size1MB   = 1024 * 1024
	Size4MB   = 4 * Size1MB
	Size8MB   = 8 * Size1MB
	Size16MB  = 16 * Size1MB
	Size32MB  = 32 * Size1MB
	Size64MB  = 64 * Size1MB
	Size128MB = 128 * Size1MB
	Size256MB = 256 * Size1MB
	Size512MB = 512 * Size1MB

	Size1GB   = 1024 * Size1MB
	Size4GB   = 4 * Size1GB
	Size8GB   = 8 * Size1GB
	Size16GB  = 16 * Size1GB
	Size32GB  = 32 * Size1GB
	Size64GB  = 64 * Size1GB
	Size128GB = 128 * Size1GB
	Size256GB = 256 * Size1GB
	Size512GB = 512 * Size1GB

	Size1TB = 1024 * Size1GB
)

// CalculateSizeLevel calculates the size level according to the size.
func CalculateSizeLevel(size int64) TaskSizeLevel {
	if size <= 0 {
		return TaskSizeLevel0
	} else if size < Size1MB {
		return TaskSizeLevel1
	} else if size < Size4MB {
		return TaskSizeLevel2
	} else if size < Size8MB {
		return TaskSizeLevel3
	} else if size < Size16MB {
		return TaskSizeLevel4
	} else if size < Size32MB {
		return TaskSizeLevel5
	} else if size < Size64MB {
		return TaskSizeLevel6
	} else if size < Size128MB {
		return TaskSizeLevel7
	} else if size < Size256MB {
		return TaskSizeLevel8
	} else if size < Size512MB {
		return TaskSizeLevel9
	} else if size < Size1GB {
		return TaskSizeLevel10
	} else if size < Size4GB {
		return TaskSizeLevel11
	} else if size < Size8GB {
		return TaskSizeLevel12
	} else if size < Size16GB {
		return TaskSizeLevel13
	} else if size < Size32GB {
		return TaskSizeLevel14
	} else if size < Size64GB {
		return TaskSizeLevel15
	} else if size < Size128GB {
		return TaskSizeLevel16
	} else if size < Size256GB {
		return TaskSizeLevel17
	} else if size < Size512GB {
		return TaskSizeLevel18
	} else if size < Size1TB {
		return TaskSizeLevel19
	} else {
		return TaskSizeLevel20
	}
}
