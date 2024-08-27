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
	"d7y.io/dragonfly/v2/pkg/types"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"net/http"
)

var (
	gotFirstResponseByte = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: types.MetricsNamespace,
		Subsystem: types.DfdaemonMetricsName,
		Name:      "transport_latency_got_first_response_byte",
		Help:      "Total got first response byte latency of transport request",
		Buckets:   prometheus.ExponentialBuckets(1, 2, 16),
	})
)

func withTraceRoundTripper(next http.RoundTripper) http.RoundTripper {
	trace := promhttp.InstrumentTrace{
		GotConn:     nil,
		PutIdleConn: nil,
		GotFirstResponseByte: func(f float64) {
			gotFirstResponseByte.Observe(f)
		},
		Got100Continue:    nil,
		DNSStart:          nil,
		DNSDone:           nil,
		ConnectStart:      nil,
		ConnectDone:       nil,
		TLSHandshakeStart: nil,
		TLSHandshakeDone:  nil,
		WroteHeaders:      nil,
		Wait100Continue:   nil,
		WroteRequest:      nil,
	}
	return promhttp.InstrumentRoundTripperTrace(&trace, next)
}
