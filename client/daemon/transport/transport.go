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
	"io/ioutil"
	"net"
	"net/http"
	"regexp"
	"time"

	"github.com/dragonflyoss/Dragonfly2/client/daemon/peer"
	logger "github.com/dragonflyoss/Dragonfly2/pkg/dflog"
	"github.com/dragonflyoss/Dragonfly2/pkg/rpc/scheduler"
)

var (
	// layerReg the regex to determine if it is an image download
	layerReg = regexp.MustCompile("^.+/blobs/sha256.*$")
)

// DFRoundTripper implements RoundTripper for dfget.
// It uses http.fileTransport to serve requests that need to use dfget,
// and uses http.Transport to serve the other requests.
type DFRoundTripper struct {
	// Round is an implementation of RoundTripper that supports HTTP
	Round *http.Transport

	// ShouldUseDfget is used to determine the use of dfget to download resources
	ShouldUseDfget func(req *http.Request) bool

	// peerTaskManager is the peer task manager
	peerTaskManager peer.PeerTaskManager

	// peerHost is the peer host info
	peerHost *scheduler.PeerHost
}

// Option is functional config for DFRoundTripper.
type DFRoundTripperOption func(rt *DFRoundTripper) *DFRoundTripper

// WithHTTPSHosts sets the rules for hijacking https requests
func WithPeerHost(peerHost *scheduler.PeerHost) DFRoundTripperOption {
	return func(rt *DFRoundTripper) *DFRoundTripper {
		rt.peerHost = peerHost
		return rt
	}
}

// WithHTTPSHosts sets the rules for hijacking https requests
func WithPeerTaskManager(peerTaskManager peer.PeerTaskManager) DFRoundTripperOption {
	return func(rt *DFRoundTripper) *DFRoundTripper {
		rt.peerTaskManager = peerTaskManager
		return rt
	}
}

// WithTLS configures TLS config used for http transport.
func WithTLS(cfg *tls.Config) DFRoundTripperOption {
	return func(rt *DFRoundTripper) *DFRoundTripper {
		rt.Round = defaultHTTPTransport(cfg)
		return rt
	}
}

// WithCondition configures how to decide whether to use dfget or not.
func WithCondition(c func(r *http.Request) bool) DFRoundTripperOption {
	return func(rt *DFRoundTripper) *DFRoundTripper {
		rt.ShouldUseDfget = c
		return rt
	}
}

// New returns the default DFRoundTripper.
func NewDFRoundTripper(options ...DFRoundTripperOption) (*DFRoundTripper, error) {
	return NewDFRoundTripperWithOptions(options...)
}

// NewDFRoundTripperWithOptions constructs a new instance of a DFRoundTripper with additional options.
func NewDFRoundTripperWithOptions(options ...DFRoundTripperOption) (*DFRoundTripper, error) {
	rt := &DFRoundTripper{
		Round:          defaultHTTPTransport(nil),
		ShouldUseDfget: NeedUseGetter,
	}

	for _, opt := range options {
		opt(rt)
	}

	return rt, nil
}

// RoundTrip only process first redirect at present
// fix resource release
func (roundTripper *DFRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	if roundTripper.ShouldUseDfget(req) {
		// delete the Accept-Encoding header to avoid returning the same cached
		// result for different requests
		req.Header.Del("Accept-Encoding")
		logger.Debugf("round trip with dfget: %s", req.URL.String())
		if res, err := roundTripper.download(req); err == nil {
			return res, err
		}
	}
	logger.Debugf("round trip directly: %s %s", req.Method, req.URL.String())
	req.Host = req.URL.Host
	req.Header.Set("Host", req.Host)
	res, err := roundTripper.Round.RoundTrip(req)
	return res, err
}

// download uses dfget to download.
func (roundTripper *DFRoundTripper) download(req *http.Request) (*http.Response, error) {
	urlString := req.URL.String()
	logger.Infof("start download url: %s", urlString)

	r, _, err := roundTripper.peerTaskManager.StartStreamPeerTask(
		req.Context(),
		&scheduler.PeerTaskRequest{
			Url:   urlString,
			BizId: "d7s/dfget",
			// TODO
			PeerId:   "peerId",
			PeerHost: roundTripper.peerHost,
		},
	)
	if err != nil {
		logger.Errorf("download fail: %v", err)
		return nil, err
	}

	resp := &http.Response{
		StatusCode: 200,
		Body:       ioutil.NopCloser(r),
	}
	return resp, nil
}

// needUseGetter is the default value for ShouldUseDfget, which downloads all
// images layers with dfget.
func NeedUseGetter(req *http.Request) bool {
	return req.Method == http.MethodGet && layerReg.MatchString(req.URL.Path)
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
