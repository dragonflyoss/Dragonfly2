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

	managerv1 "d7y.io/api/v2/pkg/apis/manager/v1"
)

const defaultGossipPort = 7946

type seedPeerMemberLister struct {
	getSeedPeers func() ([]*managerv1.SeedPeer, error)
}

func (s *seedPeerMemberLister) List() ([]*InitialMember, error) {
	seedPeers, err := s.getSeedPeers()
	if err != nil {
		return nil, err
	}
	var member []*InitialMember
	for _, peer := range seedPeers {
		member = append(member,
			&InitialMember{
				Addr: net.ParseIP(peer.Ip),
				Port: defaultGossipPort,
			})
	}
	return member, nil
}

func NewSeedPeerMemberLister(getSeedPeers func() ([]*managerv1.SeedPeer, error)) InitialMemberLister {
	return &seedPeerMemberLister{
		getSeedPeers: getSeedPeers,
	}
}

type staticPeerMemberLister struct {
	members []*InitialMember
}

func (s *staticPeerMemberLister) List() ([]*InitialMember, error) {
	return s.members, nil
}

func NewStaticPeerMemberLister(members []*InitialMember) InitialMemberLister {
	return &staticPeerMemberLister{
		members: members,
	}
}
