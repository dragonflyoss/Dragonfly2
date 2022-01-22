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

	"d7y.io/dragonfly/v2/internal/constants"
)

var (
	ProxyRequestCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: constants.MetricsNamespace,
		Subsystem: constants.DfdaemonMetricsName,
		Name:      "proxy_request_total",
		Help:      "Counter of the total proxy request.",
	}, []string{"method"})

	ProxyRequestViaDragonflyCount = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: constants.MetricsNamespace,
		Subsystem: constants.DfdaemonMetricsName,
		Name:      "proxy_request_via_dragonfly_total",
		Help:      "Counter of the total proxy request via Dragonfly.",
	})

	ProxyRequestNotViaDragonflyCount = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: constants.MetricsNamespace,
		Subsystem: constants.DfdaemonMetricsName,
		Name:      "proxy_request_not_via_dragonfly_total",
		Help:      "Counter of the total proxy request not via Dragonfly.",
	})

	ProxyRequestRunningCount = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: constants.MetricsNamespace,
		Subsystem: constants.DfdaemonMetricsName,
		Name:      "proxy_request_running_total",
		Help:      "Current running count of proxy request.",
	}, []string{"method"})

	ProxyRequestBytesCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: constants.MetricsNamespace,
		Subsystem: constants.DfdaemonMetricsName,
		Name:      "proxy_request_bytes_total",
		Help:      "Counter of the total byte of all proxy request.",
	}, []string{"method"})

	PeerTaskCount = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: constants.MetricsNamespace,
		Subsystem: constants.DfdaemonMetricsName,
		Name:      "peer_task_total",
		Help:      "Counter of the total peer tasks.",
	})

	FileTaskCount = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: constants.MetricsNamespace,
		Subsystem: constants.DfdaemonMetricsName,
		Name:      "file_task_total",
		Help:      "Counter of the total file tasks.",
	})

	StreamTaskCount = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: constants.MetricsNamespace,
		Subsystem: constants.DfdaemonMetricsName,
		Name:      "stream_task_total",
		Help:      "Counter of the total stream tasks.",
	})

	PeerTaskFailedCount = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: constants.MetricsNamespace,
		Subsystem: constants.DfdaemonMetricsName,
		Name:      "peer_task_failed_total",
		Help:      "Counter of the total failed peer tasks.",
	})

	PeerTaskCacheHitCount = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: constants.MetricsNamespace,
		Subsystem: constants.DfdaemonMetricsName,
		Name:      "peer_task_cache_hit_total",
		Help:      "Counter of the total cache hit peer tasks.",
	})
)

func New(addr string) *http.Server {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())

	return &http.Server{
		Addr:    addr,
		Handler: mux,
	}
}
