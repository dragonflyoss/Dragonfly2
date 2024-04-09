/*
 *     Copyright 2024 The Dragonfly Authors
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

package pex

import (
	"net"
	"testing"

	"github.com/stretchr/testify/assert"

	managerv1 "d7y.io/api/v2/pkg/apis/manager/v1"
)

func TestSeedPeerMemberLister(t *testing.T) {
	testCases := []struct {
		name    string
		peers   []*managerv1.SeedPeer
		members []*InitialMember
	}{
		{
			name: "",
			peers: []*managerv1.SeedPeer{
				{
					Ip: "127.0.0.1",
				},
			},
			members: []*InitialMember{
				{
					Addr: net.ParseIP("127.0.0.1"),
					Port: defaultGossipPort,
				},
			},
		},
		{
			name: "",
			peers: []*managerv1.SeedPeer{
				{
					Ip: "127.0.0.11",
				},
			},
			members: []*InitialMember{
				{
					Addr: net.ParseIP("127.0.0.11"),
					Port: defaultGossipPort,
				},
			},
		},
		{
			name: "",
			peers: []*managerv1.SeedPeer{
				{
					Ip: "127.0.0.1",
				},
				{
					Ip: "127.0.0.2",
				},
				{
					Ip: "127.0.0.3",
				},
			},
			members: []*InitialMember{
				{
					Addr: net.ParseIP("127.0.0.1"),
					Port: defaultGossipPort,
				},
				{
					Addr: net.ParseIP("127.0.0.2"),
					Port: defaultGossipPort,
				},
				{
					Addr: net.ParseIP("127.0.0.3"),
					Port: defaultGossipPort,
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert := assert.New(t)
			lister := NewSeedPeerMemberLister(func() ([]*managerv1.SeedPeer, error) {
				return tc.peers, nil
			})
			assert.NotNil(lister)
			members, err := lister.List()
			assert.Nil(err)
			assert.Equal(tc.members, members)
		})
	}
}
