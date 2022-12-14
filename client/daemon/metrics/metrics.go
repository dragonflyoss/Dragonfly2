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

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"d7y.io/dragonfly/v2/pkg/types"
	"d7y.io/dragonfly/v2/version"
)

const (
	// Failed download task type is P2P
	FailTypeP2P = "p2p"

	// Failed download task type is source
	FailTypeBackSource = "source"

	// Failed download task type is init, indecates not yet register to scheduler
	FailTypeInit = "init"

	// SeedPeerDownload type is p2p
	SeedPeerDownloadTypeP2P = "p2p"

	// SeedPeerDownload type is back-to-source
	SeedPeerDownloadTypeBackToSource = "back_to_source"
)

// Variables declared for metrics.
var (
	ProxyRequestCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: types.MetricsNamespace,
		Subsystem: types.DfdaemonMetricsName,
		Name:      "proxy_request_total",
		Help:      "Counter of the total proxy request.",
	}, []string{"method"})

	ProxyRequestViaDragonflyCount = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: types.MetricsNamespace,
		Subsystem: types.DfdaemonMetricsName,
		Name:      "proxy_request_via_dragonfly_total",
		Help:      "Counter of the total proxy request via Dragonfly.",
	})

	ProxyRequestNotViaDragonflyCount = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: types.MetricsNamespace,
		Subsystem: types.DfdaemonMetricsName,
		Name:      "proxy_request_not_via_dragonfly_total",
		Help:      "Counter of the total proxy request not via Dragonfly.",
	})

	ProxyRequestRunningCount = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: types.MetricsNamespace,
		Subsystem: types.DfdaemonMetricsName,
		Name:      "proxy_request_running_total",
		Help:      "Current running count of proxy request.",
	}, []string{"method"})

	ProxyRequestBytesCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: types.MetricsNamespace,
		Subsystem: types.DfdaemonMetricsName,
		Name:      "proxy_request_bytes_total",
		Help:      "Counter of the total byte of all proxy request.",
	}, []string{"method"})

	PeerTaskCount = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: types.MetricsNamespace,
		Subsystem: types.DfdaemonMetricsName,
		Name:      "peer_task_total",
		Help:      "Counter of the total peer tasks.",
	})

	PeerTaskFailedCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: types.MetricsNamespace,
		Subsystem: types.DfdaemonMetricsName,
		Name:      "peer_task_failed_total",
		Help:      "Counter of the total failed peer tasks.",
	}, []string{"type"})

	PieceTaskCount = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: types.MetricsNamespace,
		Subsystem: types.DfdaemonMetricsName,
		Name:      "piece_task_total",
		Help:      "Counter of the total failed piece tasks.",
	})

	PieceTaskFailedCount = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: types.MetricsNamespace,
		Subsystem: types.DfdaemonMetricsName,
		Name:      "piece_task_failed_total",
		Help:      "Counter of the total failed piece tasks.",
	})

	FileTaskCount = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: types.MetricsNamespace,
		Subsystem: types.DfdaemonMetricsName,
		Name:      "file_task_total",
		Help:      "Counter of the total file tasks.",
	})

	StreamTaskCount = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: types.MetricsNamespace,
		Subsystem: types.DfdaemonMetricsName,
		Name:      "stream_task_total",
		Help:      "Counter of the total stream tasks.",
	})

	SeedPeerDownloadCount = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: types.MetricsNamespace,
		Subsystem: types.DfdaemonMetricsName,
		Name:      "seed_peer_download_total",
		Help:      "Counter of the number of the seed peer downloading.",
	})

	SeedPeerDownloadFailureCount = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: types.MetricsNamespace,
		Subsystem: types.DfdaemonMetricsName,
		Name:      "seed_peer_download_failure_total",
		Help:      "Counter of the number of failed of the seed peer downloading.",
	})

	SeedPeerDownloadTraffic = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: types.MetricsNamespace,
		Subsystem: types.DfdaemonMetricsName,
		Name:      "seed_peer_download_traffic",
		Help:      "Counter of the number of seed peer download traffic.",
	}, []string{"type"})

	SeedPeerConcurrentDownloadGauge = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: types.MetricsNamespace,
		Subsystem: types.DfdaemonMetricsName,
		Name:      "seed_peer_concurrent_download_total",
		Help:      "Gauger of the number of concurrent of the seed peer downloading.",
	})

	PeerTaskCacheHitCount = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: types.MetricsNamespace,
		Subsystem: types.DfdaemonMetricsName,
		Name:      "peer_task_cache_hit_total",
		Help:      "Counter of the total cache hit peer tasks.",
	})

	PrefetchTaskCount = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: types.MetricsNamespace,
		Subsystem: types.DfdaemonMetricsName,
		Name:      "prefetch_task_total",
		Help:      "Counter of the total prefetched tasks.",
	})

	VersionGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: types.MetricsNamespace,
		Subsystem: types.DfdaemonMetricsName,
		Name:      "version",
		Help:      "Version info of the service.",
	}, []string{"major", "minor", "git_version", "git_commit", "platform", "build_time", "go_version", "go_tags", "go_gcflags"})
)

func New(addr string) *http.Server {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())

	VersionGauge.WithLabelValues(version.Major, version.Minor, version.GitVersion, version.GitCommit, version.Platform, version.BuildTime, version.GoVersion, version.Gotags, version.Gogcflags).Set(1)
	return &http.Server{
		Addr:    addr,
		Handler: mux,
	}
}
