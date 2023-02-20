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

package config

import (
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/resolver"

	"d7y.io/dragonfly/v2/pkg/dfnet"
)

func TestDynconfigGetResolveSchedulerAddrs_LocalSourceType(t *testing.T) {
	l, err := net.Listen("tcp", ":3000")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	tests := []struct {
		name   string
		config *DaemonOption
		newcfg *DaemonOption
		expect func(t *testing.T, dynconfig Dynconfig, config, newcfg *DaemonOption)
	}{
		{
			name: "get scheduler addrs",
			config: &DaemonOption{
				Scheduler: SchedulerOption{
					NetAddrs: []dfnet.NetAddr{
						{
							Addr: "127.0.0.1:3000",
						},
					},
				},
			},
			newcfg: nil,
			expect: func(t *testing.T, dynconfig Dynconfig, config, _newcfg *DaemonOption) {
				assert := assert.New(t)
				result, err := dynconfig.GetResolveSchedulerAddrs()
				assert.NoError(err)
				assert.EqualValues(result, []resolver.Address{{ServerName: "127.0.0.1", Addr: "127.0.0.1:3000"}})
			},
		},
		{
			name: "scheduler addrs can not reachable",
			config: &DaemonOption{
				Scheduler: SchedulerOption{
					NetAddrs: []dfnet.NetAddr{
						{
							Addr: "127.0.0.1:3003",
						},
					},
				},
			},
			newcfg: nil,
			expect: func(t *testing.T, dynconfig Dynconfig, config, _newcfg *DaemonOption) {
				assert := assert.New(t)
				_, err := dynconfig.GetResolveSchedulerAddrs()
				assert.EqualError(err, "can not found available scheduler addresses")
			},
		},
		{
			name: "config has duplicate scheduler addrs",
			config: &DaemonOption{
				Scheduler: SchedulerOption{
					NetAddrs: []dfnet.NetAddr{
						{
							Addr: "127.0.0.1:3000",
						},
						{
							Addr: "127.0.0.1:3000",
						},
					},
				},
			},
			newcfg: nil,
			expect: func(t *testing.T, dynconfig Dynconfig, config, _newcfg *DaemonOption) {
				assert := assert.New(t)
				result, err := dynconfig.GetResolveSchedulerAddrs()
				assert.NoError(err)
				assert.EqualValues(result, []resolver.Address{{ServerName: "127.0.0.1", Addr: "127.0.0.1:3000"}})
			},
		},
		{
			name: "get scheduler addrs after update config",
			config: &DaemonOption{
				Scheduler: SchedulerOption{
					NetAddrs: []dfnet.NetAddr{
						{
							Addr: "127.0.0.1:3003",
						},
					},
				},
			},
			newcfg: &DaemonOption{
				Scheduler: SchedulerOption{
					NetAddrs: []dfnet.NetAddr{
						{
							Addr: "127.0.0.1:3000",
						},
					},
				},
			},
			expect: func(t *testing.T, dynconfig Dynconfig, config, newcfg *DaemonOption) {
				assert := assert.New(t)
				_, err := dynconfig.GetResolveSchedulerAddrs()
				assert.EqualError(err, "can not found available scheduler addresses")

				dynconfig.SetConfig(newcfg)
				result, err := dynconfig.GetResolveSchedulerAddrs()
				assert.NoError(err)
				assert.EqualValues(result, []resolver.Address{{ServerName: "127.0.0.1", Addr: "127.0.0.1:3000"}})
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dynconfig, err := NewDynconfig(LocalSourceType, tc.config)
			if err != nil {
				t.Fatal(err)
			}

			tc.expect(t, dynconfig, tc.config, tc.newcfg)
		})
	}
}
