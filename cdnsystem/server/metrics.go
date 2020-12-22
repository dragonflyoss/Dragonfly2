package server

import (
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/config"
	"github.com/dragonflyoss/Dragonfly2/pkg/util/metricsutils"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// metrics defines some prometheus metrics for monitoring cdnNode.
type metrics struct {
	// server http related metrics
	requestCounter  *prometheus.CounterVec
	requestDuration *prometheus.HistogramVec
	requestSize     *prometheus.HistogramVec
	responseSize    *prometheus.HistogramVec
}

func newMetrics(register prometheus.Registerer) *metrics {
	return &metrics{
		requestCounter: metricsutils.NewCounter(config.SubsystemCdnSystem, "http_requests_total",
			"Counter of HTTP requests.", []string{"code", "handler"}, register,
		),
		requestDuration: metricsutils.NewHistogram(config.SubsystemCdnSystem, "http_request_duration_seconds",
			"Histogram of latencies for HTTP requests.", []string{"handler"},
			[]float64{.01, .02, .04, .1, .2, .4, 1, 2, 4, 8, 20, 60, 120}, register,
		),
		requestSize: metricsutils.NewHistogram(config.SubsystemCdnSystem, "http_request_size_bytes",
			"Histogram of request size for HTTP requests.", []string{"handler"},
			prometheus.ExponentialBuckets(100, 10, 8), register,
		),
		responseSize: metricsutils.NewHistogram(config.SubsystemCdnSystem, "http_response_size_bytes",
			"Histogram of response size for HTTP requests.", []string{"handler"},
			prometheus.ExponentialBuckets(100, 10, 8), register,
		),
	}
}

// instrumentHandler will update metrics for every http request.
func (m *metrics) instrumentHandler(handlerName string, handler http.HandlerFunc) http.HandlerFunc {
	return promhttp.InstrumentHandlerDuration(
		m.requestDuration.MustCurryWith(prometheus.Labels{"handler": handlerName}),
		promhttp.InstrumentHandlerCounter(
			m.requestCounter.MustCurryWith(prometheus.Labels{"handler": handlerName}),
			promhttp.InstrumentHandlerRequestSize(
				m.requestSize.MustCurryWith(prometheus.Labels{"handler": handlerName}),
				promhttp.InstrumentHandlerResponseSize(
					m.responseSize.MustCurryWith(prometheus.Labels{"handler": handlerName}),
					handler,
				),
			),
		),
	)
}