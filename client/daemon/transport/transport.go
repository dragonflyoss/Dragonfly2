/*
 * Copyright The Dragonfly Authors.
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
	"io/ioutil"
	"net"
	"net/http"
	"regexp"
	"time"

	"d7y.io/dragonfly/v2/client/clientutil"
	"d7y.io/dragonfly/v2/client/daemon/peer"
	logger "d7y.io/dragonfly/v2/pkg/dflog"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	"d7y.io/dragonfly/v2/pkg/rpc/scheduler"
)

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
	peerTaskManager peer.PeerTaskManager

	// peerHost is the peer host info
	peerHost *scheduler.PeerHost
}

// Option is functional config for transport.
type Option func(rt *transport) *transport

// WithHTTPSHosts sets the rules for hijacking https requests
func WithPeerHost(peerHost *scheduler.PeerHost) Option {
	return func(rt *transport) *transport {
		rt.peerHost = peerHost
		return rt
	}
}

// WithHTTPSHosts sets the rules for hijacking https requests
func WithPeerTaskManager(peerTaskManager peer.PeerTaskManager) Option {
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
		if res, err := rt.download(req); err == nil {
			return res, err
		}
	}
	logger.Debugf("round trip directly: %s %s", req.Method, req.URL.String())
	req.Host = req.URL.Host
	req.Header.Set("Host", req.Host)

	res, err := rt.baseRoundTripper.RoundTrip(req)

	fmt.Printf("Response: %+v\n\n", res)
	fmt.Printf("Response Body: %#v\n\n", res.StatusCode)
	fmt.Println("------------------------------------")
	return res, err
}

// needUseGetter is the default value for shouldUseDragonfly, which downloads all
// images layers with dragonfly.
func NeedUseDragonfly(req *http.Request) bool {
	return req.Method == http.MethodGet && layerReg.MatchString(req.URL.Path)
}

// download uses dragonfly to download.
func (rt *transport) download(req *http.Request) (*http.Response, error) {
	url := req.URL.String()
	logger.Infof("start download with url: %s", url)

	var meta *base.UrlMeta
	if rg := req.Header.Get("Range"); len(rg) > 0 {
		meta = &base.UrlMeta{
			Md5:   "",
			Range: rg,
		}
	}

	r, attr, err := rt.peerTaskManager.StartStreamPeerTask(
		req.Context(),
		&scheduler.PeerTaskRequest{
			Url: url,
			// FIXME(jim): read filter from config or from request header
			Filter:      "",
			BizId:       "d7s/proxy",
			UrlMata:     meta,
			PeerId:      clientutil.GenPeerID(rt.peerHost),
			PeerHost:    rt.peerHost,
			HostLoad:    nil,
			IsMigrating: false,
		},
	)
	if err != nil {
		logger.Errorf("download fail: %v", err)
		return nil, err
	}
	var hdr = http.Header{}
	for k, v := range attr {
		hdr.Set(k, v)
	}

	resp := &http.Response{
		StatusCode: 200,
		Body:       ioutil.NopCloser(r),
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
