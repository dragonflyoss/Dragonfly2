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

package daemon

import (
	"net"
	"testing"

	"github.com/stretchr/testify/assert"

	"d7y.io/dragonfly/v2/pkg/dfnet"
	"d7y.io/dragonfly/v2/pkg/rpc/manager"
)

func TestDaemonSchedulersToAvailableNetAddrs(t *testing.T) {
	l, err := net.Listen("tcp", ":3000")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	tests := []struct {
		name       string
		schedulers []*manager.Scheduler
		expect     func(t *testing.T, addrs []dfnet.NetAddr)
	}{
		{
			name: "available ip",
			schedulers: []*manager.Scheduler{
				{
					Ip:                 "127.0.0.1",
					Port:               int32(3000),
					SchedulerClusterId: 1,
				},
				{
					Ip:                 "127.0.0.1",
					Port:               int32(3001),
					SchedulerClusterId: 1,
				},
			},
			expect: func(t *testing.T, addrs []dfnet.NetAddr) {
				assert := assert.New(t)
				assert.EqualValues(addrs, []dfnet.NetAddr{{Type: dfnet.TCP, Addr: "127.0.0.1:3000"}})
			},
		},
		{
			name: "available host",
			schedulers: []*manager.Scheduler{
				{
					Ip:                 "foo",
					HostName:           "localhost",
					Port:               int32(3000),
					SchedulerClusterId: 1,
				},
				{
					Ip:                 "foo",
					HostName:           "localhost",
					Port:               int32(3001),
					SchedulerClusterId: 1,
				},
			},
			expect: func(t *testing.T, addrs []dfnet.NetAddr) {
				assert := assert.New(t)
				assert.EqualValues(addrs, []dfnet.NetAddr{{Type: dfnet.TCP, Addr: "localhost:3000"}})
			},
		},
		{
			name: "available ip and host",
			schedulers: []*manager.Scheduler{
				{
					Ip:                 "foo",
					HostName:           "localhost",
					Port:               int32(3000),
					SchedulerClusterId: 1,
				},
				{
					Ip:                 "foo",
					HostName:           "localhost",
					Port:               int32(3001),
					SchedulerClusterId: 1,
				},
				{
					Ip:                 "127.0.0.1",
					HostName:           "foo",
					Port:               int32(3001),
					SchedulerClusterId: 1,
				},
				{
					Ip:                 "127.0.0.1",
					HostName:           "foo",
					Port:               int32(3000),
					SchedulerClusterId: 1,
				},
				{
					Ip:                 "127.0.0.1",
					HostName:           "foo",
					Port:               int32(3001),
					SchedulerClusterId: 1,
				},
			},
			expect: func(t *testing.T, addrs []dfnet.NetAddr) {
				assert := assert.New(t)
				assert.EqualValues(addrs, []dfnet.NetAddr{
					{Type: dfnet.TCP, Addr: "localhost:3000"},
					{Type: dfnet.TCP, Addr: "127.0.0.1:3000"},
				})
			},
		},
		{
			name: "unreachable",
			schedulers: []*manager.Scheduler{
				{
					Ip:                 "foo",
					HostName:           "localhost",
					Port:               int32(3001),
					SchedulerClusterId: 1,
				},
				{
					Ip:                 "127.0.0.1",
					HostName:           "foo",
					Port:               int32(3001),
					SchedulerClusterId: 1,
				},
			},
			expect: func(t *testing.T, addrs []dfnet.NetAddr) {
				assert := assert.New(t)
				assert.EqualValues(addrs, []dfnet.NetAddr{})
			},
		},
		{
			name:       "empty schedulers",
			schedulers: []*manager.Scheduler{},
			expect: func(t *testing.T, addrs []dfnet.NetAddr) {
				assert := assert.New(t)
				assert.EqualValues(addrs, []dfnet.NetAddr{})
			},
		},
		{
			name: "available ip with different scheduler cluster",
			schedulers: []*manager.Scheduler{
				{
					Ip:                 "127.0.0.1",
					HostName:           "foo",
					Port:               int32(3000),
					SchedulerClusterId: 1,
				},
				{
					Ip:                 "127.0.0.1",
					HostName:           "foo",
					Port:               int32(3000),
					SchedulerClusterId: 1,
				},
				{
					Ip:                 "127.0.0.1",
					HostName:           "foo",
					Port:               int32(3001),
					SchedulerClusterId: 2,
				},
			},
			expect: func(t *testing.T, addrs []dfnet.NetAddr) {
				assert := assert.New(t)
				assert.EqualValues(addrs, []dfnet.NetAddr{
					{Type: dfnet.TCP, Addr: "127.0.0.1:3000"},
					{Type: dfnet.TCP, Addr: "127.0.0.1:3000"},
				})
			},
		},
		{
			name: "available host with different scheduler cluster",
			schedulers: []*manager.Scheduler{
				{
					Ip:                 "127.0.0.1",
					HostName:           "foo",
					Port:               int32(3001),
					SchedulerClusterId: 1,
				},
				{
					Ip:                 "foo",
					HostName:           "localhost",
					Port:               int32(3000),
					SchedulerClusterId: 2,
				},
				{
					Ip:                 "foo",
					HostName:           "localhost",
					Port:               int32(3000),
					SchedulerClusterId: 2,
				},
			},
			expect: func(t *testing.T, addrs []dfnet.NetAddr) {
				assert := assert.New(t)
				assert.EqualValues(addrs, []dfnet.NetAddr{
					{Type: dfnet.TCP, Addr: "localhost:3000"},
					{Type: dfnet.TCP, Addr: "localhost:3000"},
				})
			},
		},
		{
			name: "available host and ip with different scheduler cluster",
			schedulers: []*manager.Scheduler{
				{
					Ip:                 "foo",
					HostName:           "localhost",
					Port:               int32(3000),
					SchedulerClusterId: 2,
				},
				{
					Ip:                 "127.0.0.1",
					HostName:           "foo",
					Port:               int32(3001),
					SchedulerClusterId: 1,
				},
				{
					Ip:                 "127.0.0.1",
					HostName:           "goo",
					Port:               int32(3000),
					SchedulerClusterId: 2,
				},
				{
					Ip:                 "127.0.0.1",
					HostName:           "foo",
					Port:               int32(3000),
					SchedulerClusterId: 1,
				},
			},
			expect: func(t *testing.T, addrs []dfnet.NetAddr) {
				assert := assert.New(t)
				assert.EqualValues(addrs, []dfnet.NetAddr{
					{Type: dfnet.TCP, Addr: "localhost:3000"},
					{Type: dfnet.TCP, Addr: "127.0.0.1:3000"},
				})
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tc.expect(t, schedulersToAvailableNetAddrs(tc.schedulers))
		})
	}
}
