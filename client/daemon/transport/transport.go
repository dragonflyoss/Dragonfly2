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
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"regexp"
	"time"

	"d7y.io/dragonfly/v2/client/clientutil"
	"d7y.io/dragonfly/v2/client/config"
	"d7y.io/dragonfly/v2/client/daemon/peer"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	"d7y.io/dragonfly/v2/pkg/rpc/scheduler"
)

var _ *logger.SugaredLoggerOnWith // pin this package for no log code generation

var (
	// layerReg the regex to determine if it is an image download
	layerReg = regexp.MustCompile("^.+/blobs/sha256.*$")
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

	// peerHost is the peer host info
	peerHost *scheduler.PeerHost

	// defaultFilter is used when http request without X-Dragonfly-Filter Header
	defaultFilter string

	// defaultBiz is used when http request without X-Dragonfly-Biz Header
	defaultBiz string
}

// Option is functional config for transport.
type Option func(rt *transport) *transport

// WithPeerHost sets the peerHost for transport
func WithPeerHost(peerHost *scheduler.PeerHost) Option {
	return func(rt *transport) *transport {
		rt.peerHost = peerHost
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

// WithDefaultBiz sets default biz for http requests with X-Dragonfly-Biz Header
func WithDefaultBiz(b string) Option {
	return func(rt *transport) *transport {
		rt.defaultBiz = b
		return rt
	}
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
// fix resource release
func (rt *transport) RoundTrip(req *http.Request) (*http.Response, error) {
	if rt.shouldUseDragonfly(req) {
		// delete the Accept-Encoding header to avoid returning the same cached
		// result for different requests
		req.Header.Del("Accept-Encoding")
		logger.Debugf("round trip with dragonfly: %s", req.URL.String())
		return rt.download(req)
	}
	logger.Debugf("round trip directly: %s %s", req.Method, req.URL.String())
	req.Host = req.URL.Host
	req.Header.Set("Host", req.Host)

	return rt.baseRoundTripper.RoundTrip(req)
}

// NeedUseDragonfly is the default value for shouldUseDragonfly, which downloads all
// images layers with dragonfly.
func NeedUseDragonfly(req *http.Request) bool {
	return req.Method == http.MethodGet && layerReg.MatchString(req.URL.Path)
}

// download uses dragonfly to download.
func (rt *transport) download(req *http.Request) (*http.Response, error) {
	url := req.URL.String()
	peerID := clientutil.GenPeerID(rt.peerHost)
	log := logger.With("peer", peerID, "component", "transport")
	log.Infof("start download with url: %s", url)

	// Init meta value
	meta := &base.UrlMeta{Header: map[string]string{}}

	// Set meta range's value
	if rg := req.Header.Get("Range"); len(rg) > 0 {
		meta.Digest = ""
		meta.Range = rg
	}

	// Pick header's parameters
	filter := pickHeader(req.Header, config.HeaderDragonflyFilter, rt.defaultFilter)
	biz := pickHeader(req.Header, config.HeaderDragonflyBiz, rt.defaultBiz)

	// Delete hop-by-hop headers
	delHopHeaders(req.Header)

	meta.Header = headerToMap(req.Header)

	body, attr, err := rt.peerTaskManager.StartStreamPeerTask(
		req.Context(),
		&scheduler.PeerTaskRequest{
			Url:         url,
			Filter:      filter,
			BizId:       biz,
			UrlMeta:     meta,
			PeerId:      peerID,
			PeerHost:    rt.peerHost,
			HostLoad:    nil,
			IsMigrating: false,
		},
	)
	if err != nil {
		log.Errorf("download fail: %v", err)
		// add more info for debugging
		if attr != nil {
			err = fmt.Errorf("task: %s\npeer: %s\nerror: %s",
				attr[config.HeaderDragonflyTask], attr[config.HeaderDragonflyPeer], err)
		}
		return nil, err
	}

	hdr := mapToHeader(attr)
	log.Infof("download stream attribute: %v", hdr)

	resp := &http.Response{
		StatusCode: 200,
		Body:       body,
		Header:     hdr,
	}
	return resp, nil
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
}

// headerToMap coverts request headers to map[string]string.
func headerToMap(header http.Header) map[string]string {
	m := make(map[string]string)
	for k, v := range header {
		// TODO only use first value currently
		m[k] = v[0]
	}
	return m
}

// mapToHeader coverts map[string]string to request headers.
func mapToHeader(m map[string]string) http.Header {
	var h = http.Header{}
	for k, v := range m {
		h.Set(k, v)
	}
	return h
}

// delHopHeaders delete hop-by-hop headers.
func delHopHeaders(header http.Header) {
	for _, h := range hopHeaders {
		header.Del(h)
	}
}

// pickHeader pick header with key.
func pickHeader(header http.Header, key, defaultValue string) string {
	v := header.Get(key)
	if v != "" {
		header.Del(key)
		return v
	}

	return defaultValue
}
