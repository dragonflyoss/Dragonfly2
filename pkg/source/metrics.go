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
		// promhttp.InstrumentRoundTripperTrace unit is second, the buckets starts from 1 millisecond to 32.768 seconds
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 16),
	}, []string{"stage"})
)

func withTraceRoundTripper(next http.RoundTripper) http.RoundTripper {
	trace := promhttp.InstrumentTrace{
		GotConn: func(f float64) {
			transportLatency.WithLabelValues("GotConn").Observe(f)
		},
		PutIdleConn: func(f float64) {
			transportLatency.WithLabelValues("PutIdleConn").Observe(f)
		},
		GotFirstResponseByte: func(f float64) {
			transportLatency.WithLabelValues("GotFirstResponseByte").Observe(f)
		},
		Got100Continue: func(f float64) {
			transportLatency.WithLabelValues("Got100Continue").Observe(f)
		},
		DNSStart: func(f float64) {
			transportLatency.WithLabelValues("DNSStart").Observe(f)
		},
		DNSDone: func(f float64) {
			transportLatency.WithLabelValues("DNSDone").Observe(f)
		},
		ConnectStart: func(f float64) {
			transportLatency.WithLabelValues("ConnectStart").Observe(f)
		},
		ConnectDone: func(f float64) {
			transportLatency.WithLabelValues("ConnectDone").Observe(f)
		},
		TLSHandshakeStart: func(f float64) {
			transportLatency.WithLabelValues("TLSHandshakeStart").Observe(f)
		},
		TLSHandshakeDone: func(f float64) {
			transportLatency.WithLabelValues("TLSHandshakeDone").Observe(f)
		},
		WroteHeaders: func(f float64) {
			transportLatency.WithLabelValues("WroteHeaders").Observe(f)
		},
		Wait100Continue: func(f float64) {
			transportLatency.WithLabelValues("Wait100Continue").Observe(f)
		},
		WroteRequest: func(f float64) {
			transportLatency.WithLabelValues("WroteRequest").Observe(f)
		},
	}
	return promhttp.InstrumentRoundTripperTrace(&trace, next)
}
