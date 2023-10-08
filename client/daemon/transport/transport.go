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

package transport

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/http/httputil"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/go-http-utils/headers"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/status"

	commonv1 "d7y.io/api/v2/pkg/apis/common/v1"
	errordetailsv1 "d7y.io/api/v2/pkg/apis/errordetails/v1"

	"d7y.io/dragonfly/v2/client/config"
	"d7y.io/dragonfly/v2/client/daemon/metrics"
	"d7y.io/dragonfly/v2/client/daemon/peer"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	nethttp "d7y.io/dragonfly/v2/pkg/net/http"
)

var _ *logger.SugaredLoggerOnWith // pin this package for no log code generation

var (
	// layerReg the regex to determine if it is an image download
	layerReg     = regexp.MustCompile("^.+/blobs/sha256.*$")
	traceContext = propagation.TraceContext{}
)

// transport implements RoundTripper for dragonfly.
// It uses http.fileTransport to serve requests that need to use dragonfly,
// and uses http.Transport to serve the other requests.
type transport struct {
	// baseRoundTripper is an implementation of RoundTripper that supports HTTP
	baseRoundTripper http.RoundTripper

	// shouldUseDragonfly is used to determine to download resources with or without dragonfly
	shouldUseDragonfly func(req *http.Request) bool

	// peerTaskManager is the peer task manager
	peerTaskManager peer.TaskManager

	// defaultFilter is used when http request without X-Dragonfly-Filter Header
	defaultFilter string

	// defaultTag is used when http request without X-Dragonfly-Tag Header
	defaultTag string

	// defaultTag is used when http request without X-Dragonfly-Tag Header
	defaultApplication string

	// defaultPriority is used when http request without X-Dragonfly-Priority Header
	defaultPriority commonv1.Priority

	// dumpHTTPContent indicates to dump http request header and response header
	dumpHTTPContent bool

	peerIDGenerator peer.IDGenerator
}

// Option is functional config for transport.
type Option func(rt *transport) *transport

// WithPeerIDGenerator sets the peerIDGenerator for transport
func WithPeerIDGenerator(peerIDGenerator peer.IDGenerator) Option {
	return func(rt *transport) *transport {
		rt.peerIDGenerator = peerIDGenerator
		return rt
	}
}

// WithPeerTaskManager sets the peerTaskManager for transport
func WithPeerTaskManager(peerTaskManager peer.TaskManager) Option {
	return func(rt *transport) *transport {
		rt.peerTaskManager = peerTaskManager
		return rt
	}
}

// WithTLS configures TLS config used for http transport.
func WithTLS(cfg *tls.Config) Option {
	return func(rt *transport) *transport {
		rt.baseRoundTripper = defaultHTTPTransport(cfg)
		return rt
	}
}

// WithCondition configures how to decide whether to use dragonfly or not.
func WithCondition(c func(r *http.Request) bool) Option {
	return func(rt *transport) *transport {
		rt.shouldUseDragonfly = c
		return rt
	}
}

// WithDefaultFilter sets default filter for http requests with X-Dragonfly-Filter Header
func WithDefaultFilter(f string) Option {
	return func(rt *transport) *transport {
		rt.defaultFilter = f
		return rt
	}
}

// WithDefaultTag sets default tag for http requests with X-Dragonfly-Tag Header
func WithDefaultTag(b string) Option {
	return func(rt *transport) *transport {
		rt.defaultTag = b
		return rt
	}
}

// WithDefaultApplication sets default Application for http requests with X-Dragonfly-Application Header
func WithDefaultApplication(b string) Option {
	return func(rt *transport) *transport {
		rt.defaultApplication = b
		return rt
	}
}

// WithDefaultPriority sets default Priority for http requests with X-Dragonfly-Priority Header
func WithDefaultPriority(p commonv1.Priority) Option {
	return func(rt *transport) *transport {
		rt.defaultPriority = p
		return rt
	}

}

func WithDumpHTTPContent(b bool) Option {
	return func(rt *transport) *transport {
		rt.dumpHTTPContent = b
		return rt
	}
}

var tracer trace.Tracer

func init() {
	tracer = otel.Tracer("dfget-transport")
}

// New constructs a new instance of a RoundTripper with additional options.
func New(options ...Option) (http.RoundTripper, error) {
	rt := &transport{
		baseRoundTripper:   defaultHTTPTransport(nil),
		shouldUseDragonfly: NeedUseDragonfly,
	}

	for _, opt := range options {
		opt(rt)
	}

	return rt, nil
}

// RoundTrip only process first redirect at present
func (rt *transport) RoundTrip(req *http.Request) (resp *http.Response, err error) {
	if rt.shouldUseDragonfly(req) {
		// delete the Accept-Encoding header to avoid returning the same cached
		// result for different requests
		req.Header.Del("Accept-Encoding")

		ctx := req.Context()
		if req.URL.Scheme == "https" {
			// for https, the trace info is in request header
			ctx = traceContext.Extract(req.Context(), propagation.HeaderCarrier(req.Header))
		}

		logger.Debugf("round trip with dragonfly: %s", req.URL.String())
		metrics.ProxyRequestViaDragonflyCount.Add(1)
		resp, err = rt.download(ctx, req)
	} else {
		logger.Debugf("round trip directly, method: %s, url: %s", req.Method, req.URL.String())
		req.Host = req.URL.Host
		req.Header.Set("Host", req.Host)
		metrics.ProxyRequestNotViaDragonflyCount.Add(1)
		resp, err = rt.baseRoundTripper.RoundTrip(req)
	}

	if err != nil {
		logger.With("method", req.Method, "url", req.URL.String()).Errorf("round trip error: %s", err)
		return resp, err
	}

	if resp.ContentLength > 0 {
		metrics.ProxyRequestBytesCount.WithLabelValues(req.Method).Add(float64(resp.ContentLength))
	}

	rt.processDumpHTTPContent(req, resp)
	return resp, err
}

// NeedUseDragonfly is the default value for shouldUseDragonfly, which downloads all
// images layers with dragonfly.
func NeedUseDragonfly(req *http.Request) bool {
	return req.Method == http.MethodGet && layerReg.MatchString(req.URL.Path)
}

// download uses dragonfly to download.
// the ctx has span info from transport, did not use the ctx from request
func (rt *transport) download(ctx context.Context, req *http.Request) (*http.Response, error) {
	url := req.URL.String()
	peerID := rt.peerIDGenerator.PeerID()

	ctx, span := tracer.Start(ctx, config.SpanTransport, trace.WithSpanKind(trace.SpanKindClient))
	defer span.End()
	logKV := []any{
		"peer", peerID, "component", "transport",
	}
	if span.SpanContext().TraceID().IsValid() {
		logKV = append(logKV, "trace", span.SpanContext().TraceID().String())
	}
	log := logger.With(logKV...)

	log.Infof("start download with url: %s", url)
	log.Debugf("request url: %s, with header: %#v", url, req.Header)

	// Init meta value
	meta := &commonv1.UrlMeta{Header: map[string]string{}}
	var rg *nethttp.Range

	// Set meta range's value
	if rangeHeader := req.Header.Get("Range"); len(rangeHeader) > 0 {
		rgs, err := nethttp.ParseRange(rangeHeader, math.MaxInt64)
		if err != nil {
			span.RecordError(err)
			return badRequest(req, err.Error())
		}
		if len(rgs) > 1 {
			// TODO support multiple range request
			return notImplemented(req, "multiple range is not supported")
		} else if len(rgs) == 0 {
			return requestedRangeNotSatisfiable(req, "zero range is not supported")
		}
		rg = &rgs[0]
		// range in dragonfly is without "bytes="
		meta.Range = strings.TrimPrefix(rangeHeader, "bytes=")
	}

	// Pick header's parameters
	filter := nethttp.PickHeader(req.Header, config.HeaderDragonflyFilter, rt.defaultFilter)
	tag := nethttp.PickHeader(req.Header, config.HeaderDragonflyTag, rt.defaultTag)
	application := nethttp.PickHeader(req.Header, config.HeaderDragonflyApplication, rt.defaultApplication)
	var priority = rt.defaultPriority
	priorityString := nethttp.PickHeader(req.Header, config.HeaderDragonflyPriority, fmt.Sprintf("%d", rt.defaultPriority))
	priorityInt, err := strconv.ParseInt(priorityString, 10, 32)
	if err == nil {
		priority = commonv1.Priority(priorityInt)
	}

	// Delete hop-by-hop headers
	delHopHeaders(req.Header)

	meta.Header = nethttp.HeaderToMap(req.Header)
	meta.Tag = tag
	meta.Filter = filter
	meta.Application = application
	meta.Priority = priority

	body, attr, err := rt.peerTaskManager.StartStreamTask(
		ctx,
		&peer.StreamTaskRequest{
			URL:     url,
			URLMeta: meta,
			Range:   rg,
			PeerID:  peerID,
		},
	)
	if err != nil {
		log.Errorf("start stream task error: %v", err)
		// check underlay status code
		if st, ok := status.FromError(err); ok {
			for _, detail := range st.Details() {
				switch d := detail.(type) {
				case *errordetailsv1.SourceError:
					hdr := nethttp.MapToHeader(attr)
					for k, v := range d.Metadata.Header {
						hdr.Set(k, v)
					}
					resp := &http.Response{
						StatusCode: int(d.Metadata.StatusCode),
						Body:       io.NopCloser(bytes.NewBufferString(d.Metadata.Status)),
						Header:     hdr,
						Proto:      req.Proto,
						ProtoMajor: req.ProtoMajor,
						ProtoMinor: req.ProtoMinor,
					}
					log.Errorf("underlay response code: %d", d.Metadata.StatusCode)
					return resp, nil
				}
			}
		}
		// add more info for debugging
		if attr != nil {
			err = fmt.Errorf("task: %s\npeer: %s\nerror: %s",
				attr[config.HeaderDragonflyTask], attr[config.HeaderDragonflyPeer], err)
		}
		return nil, err
	}

	hdr := nethttp.MapToHeader(attr)
	log.Infof("download stream attribute: %v", hdr)

	var contentLength int64 = -1
	if l, ok := attr[headers.ContentLength]; ok {
		if i, e := strconv.ParseInt(l, 10, 64); e == nil {
			contentLength = i
		}
	}

	var status int
	if meta.Range == "" {
		status = http.StatusOK
	} else {
		status = http.StatusPartialContent
		if hdr.Get(headers.ContentRange) == "" && contentLength > 0 {
			value := fmt.Sprintf("bytes %d-%d/%d", rg.Start, rg.Start+contentLength-1, rg.Start+contentLength)
			hdr.Set(headers.ContentRange, value)
		}
	}
	resp := &http.Response{
		StatusCode:    status,
		Body:          body,
		Header:        hdr,
		ContentLength: contentLength,

		Proto:      req.Proto,
		ProtoMajor: req.ProtoMajor,
		ProtoMinor: req.ProtoMinor,
	}
	return resp, nil
}

func (rt *transport) processDumpHTTPContent(req *http.Request, resp *http.Response) {
	if !rt.dumpHTTPContent {
		return
	}
	if out, e := httputil.DumpRequest(req, false); e == nil {
		logger.Debugf("dump request in transport: %s", string(out))
	} else {
		logger.Errorf("dump request in transport error: %s", e)
	}
	if resp == nil {
		return
	}
	if out, e := httputil.DumpResponse(resp, false); e == nil {
		logger.Debugf("dump response in transport: %s", string(out))
	} else {
		logger.Errorf("dump response in transport error: %s", e)
	}
}

func defaultHTTPTransport(cfg *tls.Config) *http.Transport {
	if cfg == nil {
		cfg = &tls.Config{InsecureSkipVerify: true}
	}

	return &http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   10 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		TLSClientConfig:       cfg,
	}
}

// Hop-by-hop headers. These are removed when sent to the backend.
// As of RFC 7230, hop-by-hop headers are required to appear in the
// Connection header field. These are the headers defined by the
// obsoleted RFC 2616 (section 13.5.1) and are used for backward
// compatibility.
// copy from net/http/httputil/reverseproxy.go

// dragonfly need generate task id with header, need to remove some other headers
var hopHeaders = []string{
	"Connection",
	"Proxy-Connection", // non-standard but still sent by libcurl and rejected by e.g. google
	"Keep-Alive",
	"Proxy-Authenticate",
	"Proxy-Authorization",
	"Te",      // canonicalized version of "TE"
	"Trailer", // not Trailers per URL above; https://www.rfc-editor.org/errata_search.php?eid=4522
	"Transfer-Encoding",
	"Upgrade",

	// remove by dragonfly
	// "Accept", Accept header should not be removed, issue: https://github.com/dragonflyoss/Dragonfly2/issues/1290
	"User-Agent",
	"X-Forwarded-For",
}

// delHopHeaders delete hop-by-hop headers.
func delHopHeaders(header http.Header) {
	for _, h := range hopHeaders {
		header.Del(h)
	}
	// remove correlation with trace header
	for _, h := range traceContext.Fields() {
		header.Del(h)
	}
}

func compositeErrorHTTPResponse(req *http.Request, status int, body string) (*http.Response, error) {
	resp := &http.Response{
		StatusCode:    status,
		Body:          io.NopCloser(bytes.NewBufferString(body)),
		ContentLength: int64(len(body)),

		Proto:      req.Proto,
		ProtoMajor: req.ProtoMajor,
		ProtoMinor: req.ProtoMinor,
	}
	return resp, nil
}

func badRequest(req *http.Request, body string) (*http.Response, error) {
	return compositeErrorHTTPResponse(req, http.StatusBadRequest, body)
}

func notImplemented(req *http.Request, body string) (*http.Response, error) {
	return compositeErrorHTTPResponse(req, http.StatusNotImplemented, body)
}

func requestedRangeNotSatisfiable(req *http.Request, body string) (*http.Response, error) {
	return compositeErrorHTTPResponse(req, http.StatusRequestedRangeNotSatisfiable, body)
}
