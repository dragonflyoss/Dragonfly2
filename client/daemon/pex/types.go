/*
 *     Copyright 2023 The Dragonfly Authors
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
	dfdaemonv1 "d7y.io/api/v2/pkg/apis/dfdaemon/v1"
	schedulerv1 "d7y.io/api/v2/pkg/apis/scheduler/v1"
)

type InitialMember = schedulerv1.PeerPacket_DestPeer

type PeerExchanger interface {
	Serve() error
	Stop() error

	PeerExchangeMember() PeerExchangeMember
	PeerExchangeSynchronizer() PeerExchangeSynchronizer
	PeerSearcher() PeerSearcher
}

type PeerExchangeMember interface {
	FindMember(ip string) (*MemberMeta, error)
	Register(ip string, sr PeerMetadataSendReceiveCloser) error
	UnRegister(ip string)
}

type PeerExchangeSynchronizer interface {
	Sync(nodeMeta *MemberMeta, peer *dfdaemonv1.PeerMetadata)
}

type PeerSearcher interface {
	FindPeersByTask(task string) ([]*schedulerv1.PeerPacket_DestPeer, bool)
}

type InitialMemberLister interface {
	List() ([]*InitialMember, error)
}

type PeerMetadataSendReceiver interface {
	Send(*dfdaemonv1.PeerMetadata) error
	Recv() (*dfdaemonv1.PeerMetadata, error)
}

type PeerMetadataSendReceiveCloser interface {
	PeerMetadataSendReceiver
	Close() error
}

type peerMetadataSendReceiveCloser struct {
	real  PeerMetadataSendReceiver
	close func() error
}

func (p *peerMetadataSendReceiveCloser) Send(metadata *dfdaemonv1.PeerMetadata) error {
	return p.real.Send(metadata)
}

func (p *peerMetadataSendReceiveCloser) Recv() (*dfdaemonv1.PeerMetadata, error) {
	return p.real.Recv()
}

func (p *peerMetadataSendReceiveCloser) Close() error {
	return p.close()
}

func NewPeerMetadataSendReceiveCloser(p PeerMetadataSendReceiver, close func() error) PeerMetadataSendReceiveCloser {
	return &peerMetadataSendReceiveCloser{
		real:  p,
		close: close,
	}
}
