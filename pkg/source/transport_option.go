/*
 *     Copyright 2022 The Dragonfly Authors
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
	"crypto/tls"
	"net"
	"net/http"
	"net/url"
	"os"
	"time"

	"gopkg.in/yaml.v3"

	logger "d7y.io/dragonfly/v2/internal/dflog"
)

var ProxyEnv = "D7Y_SOURCE_PROXY"

type transportOption struct {
	Proxy                 string        `yaml:"proxy"`
	DialTimeout           time.Duration `yaml:"dialTimeout"`
	KeepAlive             time.Duration `yaml:"keepAlive"`
	MaxIdleConns          int           `yaml:"maxIdleConns"`
	MaxIdleConnsPerHost   int           `yaml:"maxIdleConnsPerHost"`
	MaxConnsPerHost       int           `yaml:"maxConnsPerHost"`
	IdleConnTimeout       time.Duration `yaml:"idleConnTimeout"`
	ResponseHeaderTimeout time.Duration `yaml:"responseHeaderTimeout"`
	TLSHandshakeTimeout   time.Duration `yaml:"tlsHandshakeTimeout"`
	ExpectContinueTimeout time.Duration `yaml:"expectContinueTimeout"`
	InsecureSkipVerify    bool          `yaml:"insecureSkipVerify"`
	EnableTrace           bool          `yaml:"enableTrace"`
}

func CreateTransportWithOption(optionYaml []byte) (http.RoundTripper, error) {
	opt := &transportOption{}
	err := yaml.Unmarshal(optionYaml, opt)
	if err != nil {
		return nil, err
	}

	var roundTripper http.RoundTripper

	transport := DefaultTransport()
	roundTripper = transport

	if len(opt.Proxy) > 0 {
		proxy, err := url.Parse(opt.Proxy)
		if err != nil {
			logger.Errorf("proxy parse error: %s\n", err)
			return nil, err
		}
		logger.Debugf("update transport upstream proxy: %s", opt.Proxy)
		transport.Proxy = http.ProxyURL(proxy)
	}

	if opt.IdleConnTimeout > 0 {
		transport.IdleConnTimeout = opt.IdleConnTimeout
		logger.Debugf("update transport idle conn timeout: %s", opt.IdleConnTimeout)
	}
	if opt.DialTimeout > 0 && opt.KeepAlive > 0 {
		transport.DialContext = (&net.Dialer{
			Timeout:   opt.DialTimeout,
			KeepAlive: opt.KeepAlive,
			DualStack: true,
		}).DialContext
		logger.Debugf("update transport dial timeout: %s, keep alive: %s", opt.DialTimeout, opt.KeepAlive)
	}
	if opt.MaxIdleConns > 0 {
		transport.MaxIdleConns = opt.MaxIdleConns
		logger.Debugf("update transport max idle conns: %d", opt.MaxIdleConns)
	}
	if opt.MaxIdleConnsPerHost > 0 {
		transport.MaxIdleConnsPerHost = opt.MaxIdleConnsPerHost
		logger.Debugf("update transport max idle conns per host: %d", opt.MaxIdleConnsPerHost)
	}
	if opt.MaxConnsPerHost > 0 {
		transport.MaxConnsPerHost = opt.MaxConnsPerHost
		logger.Debugf("update transport max conns per host: %d", opt.MaxConnsPerHost)
	}
	if opt.ExpectContinueTimeout > 0 {
		transport.ExpectContinueTimeout = opt.ExpectContinueTimeout
		logger.Debugf("update transport expect continue timeout: %s", opt.ExpectContinueTimeout)
	}
	if opt.ResponseHeaderTimeout > 0 {
		transport.ResponseHeaderTimeout = opt.ResponseHeaderTimeout
		logger.Debugf("update transport response header timeout: %s", opt.ResponseHeaderTimeout)
	}
	if opt.TLSHandshakeTimeout > 0 {
		transport.TLSHandshakeTimeout = opt.TLSHandshakeTimeout
		logger.Debugf("update transport tls handshake timeout: %s", opt.TLSHandshakeTimeout)
	}
	if opt.InsecureSkipVerify {
		transport.TLSClientConfig.InsecureSkipVerify = opt.InsecureSkipVerify
		logger.Debugf("update transport skip insecure verify")
	}
	if opt.EnableTrace {
		roundTripper = withTraceRoundTripper(transport)
		logger.Debugf("update transport with trace")
	}
	return roundTripper, nil
}

func DefaultTransport() *http.Transport {
	var (
		proxy *url.URL
		err   error
	)
	if proxyEnv := os.Getenv(ProxyEnv); len(proxyEnv) > 0 {
		proxy, err = url.Parse(proxyEnv)
		if err != nil {
			logger.Errorf("proxy parse error: %s\n", err)
		}
	}

	transport := &http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
			DualStack: true,
		}).DialContext,
		IdleConnTimeout:       90 * time.Second,
		ResponseHeaderTimeout: 30 * time.Second,
		ExpectContinueTimeout: 10 * time.Second,
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true,
		},
	}

	if proxy != nil {
		transport.Proxy = http.ProxyURL(proxy)
	}
	return transport
}

func ParseToHTTPClient(optionYaml []byte) (*http.Client, error) {
	transport, err := CreateTransportWithOption(optionYaml)
	if err != nil {
		return nil, err
	}

	return &http.Client{
		Transport: transport,
	}, nil
}
