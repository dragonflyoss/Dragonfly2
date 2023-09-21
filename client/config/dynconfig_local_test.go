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
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/resolver"

	"d7y.io/dragonfly/v2/pkg/dfnet"
)

func TestDynconfigLocal_GetResolveSchedulerAddrs(t *testing.T) {
	grpcServer := grpc.NewServer()
	healthpb.RegisterHealthServer(grpcServer, health.NewServer())
	l, err := net.Listen("tcp", ":3000")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	go func() {
		if err := grpcServer.Serve(l); err != nil {
			panic(err)
		}
	}()
	defer grpcServer.Stop()

	tests := []struct {
		name   string
		config *DaemonOption
		expect func(t *testing.T, dynconfig Dynconfig, config *DaemonOption)
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
			expect: func(t *testing.T, dynconfig Dynconfig, config *DaemonOption) {
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
			expect: func(t *testing.T, dynconfig Dynconfig, config *DaemonOption) {
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
			expect: func(t *testing.T, dynconfig Dynconfig, config *DaemonOption) {
				assert := assert.New(t)
				result, err := dynconfig.GetResolveSchedulerAddrs()
				assert.NoError(err)
				assert.EqualValues(result, []resolver.Address{{ServerName: "127.0.0.1", Addr: "127.0.0.1:3000"}})
			},
		},
		{
			name: "get scheduler addrs with OnNotify",
			config: &DaemonOption{
				Scheduler: SchedulerOption{
					NetAddrs: []dfnet.NetAddr{
						{
							Addr: "127.0.0.1:3003",
						},
					},
				},
			},
			expect: func(t *testing.T, dynconfig Dynconfig, config *DaemonOption) {
				assert := assert.New(t)
				_, err := dynconfig.GetResolveSchedulerAddrs()
				assert.EqualError(err, "can not found available scheduler addresses")

				dynconfig.OnNotify(&DaemonOption{
					Scheduler: SchedulerOption{
						NetAddrs: []dfnet.NetAddr{
							{
								Addr: "127.0.0.1:3000",
							},
						},
					},
				})
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

			tc.expect(t, dynconfig, tc.config)
		})
	}
}

func TestDynconfigLocal_Notify(t *testing.T) {
	grpcServer := grpc.NewServer()
	healthpb.RegisterHealthServer(grpcServer, health.NewServer())
	l, err := net.Listen("tcp", ":3000")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	tests := []struct {
		name   string
		config *DaemonOption
	}{
		{
			name: "normal",
			config: &DaemonOption{
				Scheduler: SchedulerOption{
					NetAddrs: []dfnet.NetAddr{
						{
							Addr: "127.0.0.1:3000",
						},
					},
				},
			},
		},
		{
			name: "missing port in address",
			config: &DaemonOption{
				Scheduler: SchedulerOption{
					NetAddrs: []dfnet.NetAddr{
						{
							Addr: "127.0.0.1",
						},
					},
				},
			},
		},
		{
			name: "port value is illegal",
			config: &DaemonOption{
				Scheduler: SchedulerOption{
					NetAddrs: []dfnet.NetAddr{
						{
							Addr: "127.0.0.1:",
						},
					},
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dynconfig, err := NewDynconfig(LocalSourceType, tc.config)
			if err != nil {
				t.Fatal(err)
			}

			errNotify := dynconfig.Notify()
			assert := assert.New(t)
			assert.NoError(errNotify)
		})
	}
}
