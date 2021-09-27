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

package dns

import (
	"context"
	"net"
	"strings"
	"time"

	dflog "d7y.io/dragonfly/v2/internal/dflog"
)

const (
	defaultPingCount = 3
	defaultTimeout   = 3 * time.Second
)

type SearchOption struct {
	// SearchDomains like domain in /etc/resolv.conf
	SearchDomains []string `mapstructure:"searchDomains" yaml:"searchDomains"`
	// Timeout is the timeout for searching
	Timeout time.Duration `mapstructure:"timeout" yaml:"timeout"`
	// PingCount is the count for latency testing
	PingCount int `mapstructure:"pingCount" yaml:"pingCount"`
}

type pingResolver struct {
	defaultResolver netResolver
	port            string
	opt             *SearchOption
}

type pingResult struct {
	host    string
	latency time.Duration
	err     error
}

func newPingResolver(defaultResolver netResolver, opt *SearchOption, port string) *pingResolver {
	if opt.PingCount == 0 {
		opt.PingCount = defaultPingCount
	}
	if opt.Timeout == 0 {
		opt.Timeout = defaultTimeout
	}
	return &pingResolver{
		defaultResolver: defaultResolver,
		port:            port,
		opt:             opt,
	}
}

func (p *pingResolver) LookupHost(ctx context.Context, host string) ([]string, error) {
	addrs, err0 := p.defaultResolver.LookupHost(ctx, host)

	// local search found, return
	if len(addrs) > 0 {
		return addrs, err0
	}

	// already fqdn, return
	if strings.HasSuffix(host, ".") {
		return addrs, err0
	}

	for _, s := range p.opt.SearchDomains {
		add, err := p.defaultResolver.LookupHost(ctx, host+"."+s)
		if err == nil {
			addrs = append(addrs, add...)
		} else {
			dflog.Warnf("lookup host %s with search domain %s failed: %s", host, s, err)
		}
	}

	if len(addrs) == 0 {
		if err0 != nil {
			return nil, err0
		}
		return nil, &net.DNSError{Err: "no such host", Name: host, IsNotFound: true}
	}

	// try to find fastest
	if fastest, ok := p.findFastestHost(addrs); ok {
		return []string{fastest}, nil
	}

	return addrs, nil
}

func (p *pingResolver) LookupSRV(ctx context.Context, service, proto, name string) (cname string, addrs []*net.SRV, err error) {
	// TODO support search domain LookupSRV
	return p.defaultResolver.LookupSRV(ctx, service, proto, name)
}

func (p *pingResolver) LookupTXT(ctx context.Context, name string) (txts []string, err error) {
	// TODO support search domain LookupTXT
	return p.defaultResolver.LookupTXT(ctx, name)
}

func (p *pingResolver) findFastestHost(hosts []string) (string, bool) {
	result, done := make(chan string), make(chan bool)
	for _, host := range hosts {
		go func(host string) {
			for i := 0; i < p.opt.PingCount; i++ {
				r := p.ping(host, p.port)
				if r.err != nil {
					dflog.Errorf("tcp ping error: %s, host: %s", r.err, host)
					return
				}
				dflog.Debugf("host %s, tcp latency: %s", host, r.latency)
			}
			select {
			case result <- host:
			case <-done:
			}
		}(host)
	}

	select {
	case <-time.After(p.opt.Timeout):
		return "", false
	case fastest := <-result:
		close(done)
		dflog.Debugf("fastest host %s in %#v", fastest, hosts)
		return fastest, true
	}
}

func (p *pingResolver) ping(host, port string) *pingResult {
	start := time.Now()
	conn, err := net.DialTimeout("tcp", host+":"+port, time.Second)
	end := time.Now()
	if err != nil {
		return &pingResult{
			host:    host,
			latency: end.Sub(start),
			err:     err,
		}
	}
	defer conn.Close()
	return &pingResult{
		host:    host,
		latency: end.Sub(start),
	}
}
