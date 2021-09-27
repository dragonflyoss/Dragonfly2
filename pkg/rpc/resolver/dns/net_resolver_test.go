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
	"fmt"
	"net"
	"testing"

	"github.com/golang/mock/gomock"
	testifyassert "github.com/stretchr/testify/assert"
)

func Test_pingResolver(t *testing.T) {
	assert := testifyassert.New(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	dr := NewMocknetResolver(ctrl)

	var dnsResolveData map[string][]string
	dr.EXPECT().LookupSRV(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(
		func(ctx context.Context, service, proto, name string) (string, []*net.SRV, error) {
			return "", nil, nil
		})
	dr.EXPECT().LookupTXT(gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(
		func(ctx context.Context, name string) ([]string, error) {
			return nil, nil
		})
	dr.EXPECT().LookupHost(gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(
		func(ctx context.Context, host string) ([]string, error) {
			if ip, ok := dnsResolveData[host]; ok {
				return ip, nil
			}
			return nil, &net.DNSError{Err: "no such host", Name: host, IsNotFound: true}
		})

	ln, err := net.Listen("tcp", ":0")
	assert.Nil(err)
	tcpAddr, ok := ln.Addr().(*net.TCPAddr)
	defer ln.Close()
	assert.True(ok)

	var testCases = []struct {
		name          string
		searchDomains []string
		resolveHosts  map[string][]string
		lookupHost    string
		lookupResult  []string
		expectError   bool
	}{
		{
			name: "lookup short domain ok",
			resolveHosts: map[string][]string{
				"scheduler-0": {"127.0.0.1"},
			},
			searchDomains: nil,
			lookupHost:    "scheduler-0",
			lookupResult:  []string{"127.0.0.1"},
			expectError:   false,
		},
		{
			name: "lookup with search domain ok",
			resolveHosts: map[string][]string{
				"scheduler-0.local": {"127.0.0.1"},
			},
			searchDomains: []string{"local"},
			lookupHost:    "scheduler-0",
			lookupResult:  []string{"127.0.0.1"},
			expectError:   false,
		},
		{
			name:          "lookup failed",
			resolveHosts:  map[string][]string{},
			searchDomains: []string{"d7y.io"},
			lookupHost:    "scheduler-0",
			expectError:   true,
		},
		{
			name: "lookup with fqdn failed",
			resolveHosts: map[string][]string{
				"scheduler-0.local": {"127.0.0.1"},
			},
			searchDomains: []string{"local"},
			lookupHost:    "scheduler-0.",
			expectError:   true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			dnsResolveData = tc.resolveHosts
			pr := newPingResolver(dr, &SearchOption{
				SearchDomains: tc.searchDomains,
				PingCount:     1,
			}, fmt.Sprintf("%d", tcpAddr.Port))
			lookupResult, err := pr.LookupHost(context.Background(), tc.lookupHost)
			if tc.expectError {
				assert.NotNil(err)
				return
			}
			assert.Nil(err)
			assert.Equal(tc.lookupResult, lookupResult)
		})
	}
}
