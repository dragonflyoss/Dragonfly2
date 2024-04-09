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
	"github.com/hashicorp/memberlist"

	dfdaemonv1 "d7y.io/api/v2/pkg/apis/dfdaemon/v1"
)

type InitialMember = memberlist.Node

type PeerExchangeServer interface {
	Serve(localMember *MemberMeta) error
	Stop() error

	PeerSearchBroadcaster() PeerSearchBroadcaster
	PeerExchangeRPC() PeerExchangeRPC
}

type PeerExchangeMember interface {
	FindMember(hostID string) (*MemberMeta, error)
	Register(member *MemberMeta, sr PeerMetadataSendReceiveCloser) error
	UnRegister(member *MemberMeta)
	UnRegisterByHostID(hostID string)
}

type PeerExchangeSynchronizer interface {
	Sync(nodeMeta *MemberMeta, peer *dfdaemonv1.PeerExchangeData)
}

type PeerExchangeRPC interface {
	PeerExchange(exchangeServer dfdaemonv1.Daemon_PeerExchangeServer) error
}

type DestPeer struct {
	*MemberMeta
	PeerID string
}

type SearchPeerResultType int

const (
	SearchPeerResultTypeLocal = iota
	SearchPeerResultTypeReplica
	SearchPeerResultTypeRemote
	SearchPeerResultTypeNotFound
)

type SearchPeerResult struct {
	Type  SearchPeerResultType
	Peers []*DestPeer
}

type ReclaimFunc func(task, peer string) error

type PeerSearchBroadcaster interface {
	SearchPeer(task string) SearchPeerResult
	BroadcastPeer(data *dfdaemonv1.PeerMetadata)
	BroadcastPeers(data *dfdaemonv1.PeerExchangeData)
}

type InitialMemberLister interface {
	List() ([]*InitialMember, error)
}

type PeerMetadataSendReceiver interface {
	Send(*dfdaemonv1.PeerExchangeData) error
	Recv() (*dfdaemonv1.PeerExchangeData, error)
}

type PeerMetadataSendReceiveCloser interface {
	PeerMetadataSendReceiver
	Close() error
}

type peerMetadataSendReceiveCloser struct {
	real  PeerMetadataSendReceiver
	close func() error
}

func (p *peerMetadataSendReceiveCloser) Send(metadata *dfdaemonv1.PeerExchangeData) error {
	return p.real.Send(metadata)
}

func (p *peerMetadataSendReceiveCloser) Recv() (*dfdaemonv1.PeerExchangeData, error) {
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
