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
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

var ProxyEnv = "D7Y_SOURCE_PROXY"

type transportOption struct {
	Proxy                 string        `yaml:"proxy"`
	DialTimeout           time.Duration `yaml:"dialTimeout"`
	KeepAlive             time.Duration `yaml:"keepAlive"`
	MaxIdleConns          int           `yaml:"maxIdleConns"`
	IdleConnTimeout       time.Duration `yaml:"idleConnTimeout"`
	ResponseHeaderTimeout time.Duration `yaml:"responseHeaderTimeout"`
	TLSHandshakeTimeout   time.Duration `yaml:"tlsHandshakeTimeout"`
	ExpectContinueTimeout time.Duration `yaml:"expectContinueTimeout"`
	InsecureSkipVerify    bool          `yaml:"insecureSkipVerify"`
}

func UpdateTransportOption(transport *http.Transport, optionYaml []byte) error {
	opt := &transportOption{}
	err := yaml.Unmarshal(optionYaml, opt)
	if err != nil {
		return err
	}

	if len(opt.Proxy) > 0 {
		proxy, err := url.Parse(opt.Proxy)
		if err != nil {
			fmt.Printf("proxy parse error: %s\n", err)
			return err
		}
		transport.Proxy = http.ProxyURL(proxy)
	}

	if opt.IdleConnTimeout > 0 {
		transport.IdleConnTimeout = opt.IdleConnTimeout
	}
	if opt.DialTimeout > 0 && opt.KeepAlive > 0 {
		transport.DialContext = (&net.Dialer{
			Timeout:   opt.DialTimeout,
			KeepAlive: opt.KeepAlive,
			DualStack: true,
		}).DialContext
	}
	if opt.MaxIdleConns > 0 {
		transport.MaxIdleConns = opt.MaxIdleConns
	}
	if opt.ExpectContinueTimeout > 0 {
		transport.ExpectContinueTimeout = opt.ExpectContinueTimeout
	}
	if opt.ResponseHeaderTimeout > 0 {
		transport.ResponseHeaderTimeout = opt.ResponseHeaderTimeout
	}
	if opt.TLSHandshakeTimeout > 0 {
		transport.TLSHandshakeTimeout = opt.TLSHandshakeTimeout
	}
	if opt.InsecureSkipVerify {
		transport.TLSClientConfig.InsecureSkipVerify = opt.InsecureSkipVerify
	}
	return nil
}

func DefaultTransport() *http.Transport {
	var (
		proxy *url.URL
		err   error
	)
	if proxyEnv := os.Getenv(ProxyEnv); len(proxyEnv) > 0 {
		proxy, err = url.Parse(proxyEnv)
		if err != nil {
			fmt.Printf("proxy parse error: %s\n", err)
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
	transport := DefaultTransport()
	err := UpdateTransportOption(transport, optionYaml)
	if err != nil {
		return nil, err
	}

	return &http.Client{
		Transport: transport,
	}, nil
}
