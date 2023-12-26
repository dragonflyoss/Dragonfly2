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
	"fmt"
	"time"

	"github.com/hashicorp/memberlist"
	"github.com/pkg/errors"
	"google.golang.org/grpc"

	dfdaemonv1 "d7y.io/api/v2/pkg/apis/dfdaemon/v1"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/net/ip"
)

type peerExchange struct {
	config        *peerExchangeConfig
	memberConfig  *memberlist.Config
	localMember   *MemberMeta
	memberlist    *memberlist.Memberlist
	memberManager *peerExchangeMemberManager
	lister        InitialMemberLister
	stopCh        chan struct{}
}

type peerExchangeConfig struct {
	initialRetryInterval time.Duration
	reSyncRetryInterval  time.Duration
}

func WithName(name string) func(*memberlist.Config, *peerExchangeConfig) {
	return func(memberConfig *memberlist.Config, pexConfig *peerExchangeConfig) {
		memberConfig.Name = name
	}
}

func WithBindAddr(bindAddr string) func(*memberlist.Config, *peerExchangeConfig) {
	return func(memberConfig *memberlist.Config, pexConfig *peerExchangeConfig) {
		memberConfig.BindAddr = bindAddr
	}
}

func WithBindPort(bindPort int) func(*memberlist.Config, *peerExchangeConfig) {
	return func(memberConfig *memberlist.Config, pexConfig *peerExchangeConfig) {
		memberConfig.BindPort = bindPort
	}
}

func WithAdvertiseAddr(advertiseAddr string) func(*memberlist.Config, *peerExchangeConfig) {
	return func(memberConfig *memberlist.Config, pexConfig *peerExchangeConfig) {
		memberConfig.AdvertiseAddr = advertiseAddr
	}
}

func WithAdvertisePort(advertisePort int) func(*memberlist.Config, *peerExchangeConfig) {
	return func(memberConfig *memberlist.Config, pexConfig *peerExchangeConfig) {
		memberConfig.AdvertisePort = advertisePort
	}
}

func WithInitialRetryInterval(interval time.Duration) func(*memberlist.Config, *peerExchangeConfig) {
	return func(memberConfig *memberlist.Config, pexConfig *peerExchangeConfig) {
		pexConfig.initialRetryInterval = interval
	}
}

func WithReSyncInterval(interval time.Duration) func(*memberlist.Config, *peerExchangeConfig) {
	return func(memberConfig *memberlist.Config, pexConfig *peerExchangeConfig) {
		pexConfig.reSyncRetryInterval = interval
	}
}

func NewPeerExchange(lister InitialMemberLister,
	grpcDialTimeout time.Duration,
	grpcDialOptions []grpc.DialOption, opts ...func(*memberlist.Config, *peerExchangeConfig)) (PeerExchangeServer, error) {
	memberManager := newPeerExchangeMemberManager(grpcDialTimeout, grpcDialOptions)

	memberConfig := memberlist.DefaultLANConfig()
	memberConfig.Events = memberManager

	pexConfig := &peerExchangeConfig{
		initialRetryInterval: 10 * time.Second,
		reSyncRetryInterval:  time.Minute,
	}

	for _, opt := range opts {
		opt(memberConfig, pexConfig)
	}

	if memberConfig.AdvertiseAddr == "" {
		// TODO support ipv6
		memberConfig.AdvertiseAddr = ip.IPv4.String()
	}

	pex := &peerExchange{
		config:        pexConfig,
		memberConfig:  memberConfig,
		memberManager: memberManager,
		lister:        lister,
		stopCh:        make(chan struct{}),
	}
	return pex, nil
}

func (p *peerExchange) PeerExchangeMember() PeerExchangeMember {
	return p.memberManager.memberPool
}

func (p *peerExchange) PeerExchangeSynchronizer() PeerExchangeSynchronizer {
	return p.memberManager.peerPool
}

func (p *peerExchange) PeerExchangeRPC() PeerExchangeRPC {
	return p
}

func (p *peerExchange) PeerSearchBroadcaster() PeerSearchBroadcaster {
	return p
}

func (p *peerExchange) FindPeersByTask(task string) ([]*DestPeer, bool) {
	return p.memberManager.peerPool.Find(task)
}

func (p *peerExchange) BroadcastPeer(data *dfdaemonv1.PeerMetadata) {
	p.memberManager.broadcast(&dfdaemonv1.PeerExchangeData{
		PeerMetadatas: []*dfdaemonv1.PeerMetadata{data},
	})
}

func (p *peerExchange) BroadcastPeers(data *dfdaemonv1.PeerExchangeData) {
	p.memberManager.broadcast(data)
}

func (p *peerExchange) Serve(localMember *MemberMeta) error {
	p.memberConfig.Delegate = newPeerExchangeDelegate(localMember)
	p.localMember = localMember
	p.memberManager.localMember = localMember

	member, err := memberlist.Create(p.memberConfig)
	if err != nil {
		return errors.WithMessage(err, "failed to create memberlist")
	}

	// update member list
	p.memberlist = member
	p.memberManager.memberPool.members = member

	go p.memberManager.broadcastInBackground()
	go p.reSyncMember()

	for {
		if err := p.serve(); err == nil {
			break
		}
	}

	<-p.stopCh
	return nil
}

func (p *peerExchange) serve() error {
	var (
		members []*InitialMember
		err     error
	)

	for {
		members, err = p.lister.List()
		if err == nil {
			break
		}
		logger.Errorf("failed to list initial member: %s", err)
		time.Sleep(p.config.initialRetryInterval)
	}

	var ips []string
	for _, member := range members {
		ips = append(ips, fmt.Sprintf("%s:%d", member.Addr.String(), member.Port))
	}

	_, err = p.memberlist.Join(ips)
	if err != nil {
		logger.Errorf("failed to join cluster: %s, error: %s", ips, err)
	}

	return err
}

func (p *peerExchange) Stop() error {
	close(p.stopCh)
	return p.memberlist.Leave(10 * time.Second)
}

func (p *peerExchange) reSyncMember() {
	for {
		time.Sleep(p.config.reSyncRetryInterval)
		var members []*MemberMeta
		for _, m := range p.memberlist.Members() {
			meta, err := ExtractNodeMeta(m)
			if err != nil {
				logger.Errorf("failed to extract metadata: %s", m)
				continue
			}
			// skip not alive
			if m.State != memberlist.StateAlive {
				continue
			}
			// skip local
			if p.memberManager.isLocal(meta) {
				continue
			}
			members = append(members, meta)
		}
		memberAddrs := p.memberManager.memberPool.MemberKeys()
		add, del := diffMembers(members, memberAddrs)
		for _, member := range add {
			logger.Infof("re-sync add member: %s", member.HostID)
			p.memberManager.syncNode(member)
		}
		for _, id := range del {
			logger.Infof("re-sync del member: %s", id)
			p.memberManager.memberPool.UnRegister(id)
		}
	}
}

func diffMembers(members []*MemberMeta, hostIDs []string) (add []*MemberMeta, del []string) {
	for _, member := range members {
		hostID := member.HostID
		toAdd := true

		for _, id := range hostIDs {
			if hostID == id {
				toAdd = false
			}
		}
		if toAdd {
			add = append(add, member)
		}
	}

	for _, id := range hostIDs {
		toDel := true
		for _, member := range members {
			if member.HostID == id {
				toDel = false
			}
		}
		if toDel {
			del = append(del, id)
		}
	}
	return
}
