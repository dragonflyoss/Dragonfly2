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

package proxy

import (
	"crypto/tls"
	"io/ioutil"
	"net"
	"net/http"
	"regexp"
	"time"

	"github.com/pkg/errors"

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
	Round           *http.Transport
	ShouldUseDfget  func(req *http.Request) bool
	peerTaskManager peer.PeerTaskManager
}

// Option is functional config for DFRoundTripper.
type DFRoundTripperOption func(rt *DFRoundTripper) error

// WithTLS configures TLS config used for http transport.
func WithTLS(cfg *tls.Config) DFRoundTripperOption {
	return func(rt *DFRoundTripper) error {
		rt.Round = defaultHTTPTransport(cfg)
		return nil
	}
}

// WithCondition configures how to decide whether to use dfget or not.
func WithCondition(c func(r *http.Request) bool) DFRoundTripperOption {
	return func(rt *DFRoundTripper) error {
		rt.ShouldUseDfget = c
		return nil
	}
}

// New returns the default DFRoundTripper.
func NewTransport(peerTaskManager peer.PeerTaskManager, opts ...DFRoundTripperOption) (*DFRoundTripper, error) {
	rt := &DFRoundTripper{
		Round:           defaultHTTPTransport(nil),
		ShouldUseDfget:  NeedUseGetter,
		peerTaskManager: peerTaskManager,
	}

	for _, opt := range opts {
		if err := opt(rt); err != nil {
			return nil, errors.Wrap(err, "apply options")
		}
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
		if res, err := roundTripper.download(req, req.URL.String()); err == nil {
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
func (roundTripper *DFRoundTripper) download(req *http.Request, urlString string) (*http.Response, error) {
	url := req.URL.String()
	logger.Infof("start download url:%s", url)

	r, _, err := roundTripper.peerTaskManager.StartStreamPeerTask(
		req.Context(),
		&scheduler.PeerTaskRequest{
			Url:   url,
			BizId: "d7s/dfget",
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
