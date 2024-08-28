/*
 *     Copyright 2024 The Dragonfly Authors
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

package source

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"d7y.io/dragonfly/v2/pkg/types"
)

var (
	transportLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: types.MetricsNamespace,
		Subsystem: types.DfdaemonMetricsName,
		Name:      "transport_latency",
		Help:      "The latency of transport request",
		Buckets:   prometheus.ExponentialBuckets(1, 2, 16),
	}, []string{"stage"})
)

func withTraceRoundTripper(next http.RoundTripper) http.RoundTripper {
	trace := promhttp.InstrumentTrace{
		GotConn: func(f float64) {
			transportLatency.With(prometheus.Labels{"stage": "GotConn"}).Observe(f)
		},
		PutIdleConn: func(f float64) {
			transportLatency.With(prometheus.Labels{"stage": "PutIdleConn"}).Observe(f)
		},
		GotFirstResponseByte: func(f float64) {
			transportLatency.With(prometheus.Labels{"stage": "GotFirstResponseByte"}).Observe(f)
		},
		Got100Continue: func(f float64) {
			transportLatency.With(prometheus.Labels{"stage": "Got100Continue"}).Observe(f)
		},
		DNSStart: func(f float64) {
			transportLatency.With(prometheus.Labels{"stage": "DNSStart"}).Observe(f)
		},
		DNSDone: func(f float64) {
			transportLatency.With(prometheus.Labels{"stage": "DNSDone"}).Observe(f)
		},
		ConnectStart: func(f float64) {
			transportLatency.With(prometheus.Labels{"stage": "ConnectStart"}).Observe(f)
		},
		ConnectDone: func(f float64) {
			transportLatency.With(prometheus.Labels{"stage": "ConnectDone"}).Observe(f)
		},
		TLSHandshakeStart: func(f float64) {
			transportLatency.With(prometheus.Labels{"stage": "TLSHandshakeStart"}).Observe(f)
		},
		TLSHandshakeDone: func(f float64) {
			transportLatency.With(prometheus.Labels{"stage": "TLSHandshakeDone"}).Observe(f)
		},
		WroteHeaders: func(f float64) {
			transportLatency.With(prometheus.Labels{"stage": "WroteHeaders"}).Observe(f)
		},
		Wait100Continue: func(f float64) {
			transportLatency.With(prometheus.Labels{"stage": "Wait100Continue"}).Observe(f)
		},
		WroteRequest: func(f float64) {
			transportLatency.With(prometheus.Labels{"stage": "WroteRequest"}).Observe(f)
		},
	}
	return promhttp.InstrumentRoundTripperTrace(&trace, next)
}
