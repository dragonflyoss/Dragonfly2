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

package basic

import (
	"testing"

	"d7y.io/dragonfly/v2/scheduler/supervisor"
)

func Test_getDistance(t *testing.T) {
	type args struct {
		dst *supervisor.Peer
		src *supervisor.Peer
	}
	host := supervisor.NewClientHost("uuid", "ip", "hostname", 0, 0, "securityDomain",
		"china|beijing|daxing", "idc",
		"netTopology",
		0)
	tests := []struct {
		name string
		args args
		want float64
	}{
		{
			name: "same host",
			args: args{
				dst: supervisor.NewPeer("", supervisor.NewTask("task1", "", nil), host),
				src: supervisor.NewPeer("", supervisor.NewTask("task2", "", nil), host),
			},
			want: 1,
		}, {
			name: "different security",
			args: args{
				dst: supervisor.NewPeer("", supervisor.NewTask("task1", "", nil), supervisor.NewClientHost("uuid", "ip", "hostname", 0, 0, "securityDomain1",
					"location", "idc",
					"netTopology",
					0)),
				src: supervisor.NewPeer("", supervisor.NewTask("task2", "", nil), supervisor.NewClientHost("uuid", "ip", "hostname", 0, 0, "securityDomain2",
					"location", "idc",
					"netTopology",
					0)),
			},
			want: 0.0,
		}, {
			name: "same security, different idc, same location",
			args: args{
				dst: supervisor.NewPeer("", supervisor.NewTask("task1", "", nil), supervisor.NewClientHost("uuid", "ip", "hostname", 0, 0, "securityDomain",
					"china|beijing|chaoyang", "idc1",
					"netTopology",
					0)),
				src: supervisor.NewPeer("", supervisor.NewTask("task2", "", nil), supervisor.NewClientHost("uuid", "ip", "hostname", 0, 0, "securityDomain",
					"china|beijing|chaoyang", "idc2",
					"netTopology",
					0)),
			},
			want: 0.9,
		}, {
			name: "same security, different location, same idc",
			args: args{
				dst: supervisor.NewPeer("", supervisor.NewTask("task1", "", nil), supervisor.NewClientHost("uuid", "ip", "hostname", 0, 0, "securityDomain",
					"china|beijing|daxing", "idc",
					"netTopology",
					0)),
				src: supervisor.NewPeer("", supervisor.NewTask("task2", "", nil), supervisor.NewClientHost("uuid", "ip", "hostname", 0, 0, "securityDomain",
					"china|beijing|chaoyang", "idc",
					"netTopology",
					0)),
			},
			want: 0.95,
		}, {
			name: "same security, different location, same idc, different netTopology",
			args: args{
				dst: supervisor.NewPeer("", supervisor.NewTask("task1", "", nil), supervisor.NewClientHost("uuid", "ip", "hostname", 0, 0, "securityDomain",
					"china|beijing|daxing", "idc",
					"AAA|bb|cc",
					0)),
				src: supervisor.NewPeer("", supervisor.NewTask("task2", "", nil), supervisor.NewClientHost("uuid", "ip", "hostname", 0, 0, "securityDomain",
					"china|beijing|chaoyang", "idc",
					"AAA|bb|dd",
					0)),
			},
			want: 0.6928571428571428,
		}, {
			name: "same security, different location, same idc, different netTopology 2",
			args: args{
				dst: supervisor.NewPeer("", supervisor.NewTask("task1", "", nil), supervisor.NewClientHost("uuid", "ip", "hostname", 0, 0, "securityDomain",
					"china|beijing|daxing", "idc",
					"AAA|bb|cc",
					0)),
				src: supervisor.NewPeer("", supervisor.NewTask("task2", "", nil), supervisor.NewClientHost("uuid", "ip", "hostname", 0, 0, "securityDomain",
					"china|beijing|chaoyang", "idc",
					"BBB|bb|dd",
					0)),
			},
			want: 0.5,
		}, {
			name: "same security, different location, same idc, different netTopology 3",
			args: args{
				dst: supervisor.NewPeer("", supervisor.NewTask("task1", "", nil), supervisor.NewClientHost("uuid", "ip", "hostname", 0, 0, "securityDomain",
					"china|beijing|daxing", "idc",
					"AAA|bb|cc",
					0)),
				src: supervisor.NewPeer("", supervisor.NewTask("task2", "", nil), supervisor.NewClientHost("uuid", "ip", "hostname", 0, 0, "securityDomain",
					"china|beijing|chaoyang", "idc",
					"AAA|bbB|dd",
					0)),
			},
			want: 0.5642857142857143,
		}, {
			name: "same security, different location, empty idc",
			args: args{
				dst: supervisor.NewPeer("", supervisor.NewTask("task1", "", nil), supervisor.NewClientHost("uuid", "ip", "hostname", 0, 0, "securityDomain",
					"china|beijing|daxing", "",
					"netTopology",
					0)),
				src: supervisor.NewPeer("", supervisor.NewTask("task2", "", nil), supervisor.NewClientHost("uuid", "ip", "hostname", 0, 0, "securityDomain",
					"china|beijing|chaoyang", "",
					"netTopology",
					0)),
			},
			want: 0.6714285714285714,
		}, {
			name: "same security, different location, empty idc 2",
			args: args{
				dst: supervisor.NewPeer("", supervisor.NewTask("task1", "", nil), supervisor.NewClientHost("uuid", "ip", "hostname", 0, 0, "securityDomain",
					"china|beijing|daxing", "",
					"netTopology",
					0)),
				src: supervisor.NewPeer("", supervisor.NewTask("task2", "", nil), supervisor.NewClientHost("uuid", "ip", "hostname", 0, 0, "securityDomain",
					"us|new|chaoyang", "",
					"netTopology",
					0)),
			},
			want: 0.5,
		}, {
			name: "same security, different location, empty idc 3",
			args: args{
				dst: supervisor.NewPeer("", supervisor.NewTask("task1", "", nil), supervisor.NewClientHost("uuid", "ip", "hostname", 0, 0, "securityDomain",
					"china|beijing|daxing", "",
					"netTopology",
					0)),
				src: supervisor.NewPeer("", supervisor.NewTask("task2", "", nil), supervisor.NewClientHost("uuid", "ip", "hostname", 0, 0, "securityDomain",
					"china|shanghai|pudong", "",
					"netTopology",
					0)),
			},
			want: 0.5571428571428572,
		}, {
			name: "same security, different location, empty idc 3",
			args: args{
				dst: supervisor.NewPeer("", supervisor.NewTask("task1", "", nil), supervisor.NewClientHost("uuid", "ip", "hostname", 0, 0, "securityDomain",
					"china|beijing|daxing", "",
					"netTopology",
					0)),
				src: supervisor.NewPeer("", supervisor.NewTask("task2", "", nil), supervisor.NewClientHost("uuid", "ip", "hostname", 0, 0, "securityDomain",
					"china|shanghai|pudong", "",
					"netTopology",
					0)),
			},
			want: 0.5571428571428572,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := getDistance(tt.args.dst, tt.args.src); got != tt.want {
				t.Errorf("getDistance() = %v, want %v", got, tt.want)
			}
		})
	}
}
