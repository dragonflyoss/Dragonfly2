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
	"fmt"
	"math/rand"
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
	reclaim       ReclaimFunc
	lister        InitialMemberLister
	stopCh        chan struct{}
}

type peerExchangeConfig struct {
	initialRetryInterval time.Duration
	reSyncInterval       time.Duration
	replicaThreshold     int
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
		if interval > 0 {
			pexConfig.initialRetryInterval = interval
		}
	}
}

func WithReSyncInterval(interval time.Duration) func(*memberlist.Config, *peerExchangeConfig) {
	return func(memberConfig *memberlist.Config, pexConfig *peerExchangeConfig) {
		if interval > 0 {
			pexConfig.reSyncInterval = interval
		}
	}
}

func WithReplicaThreshold(threshold int) func(*memberlist.Config, *peerExchangeConfig) {
	return func(memberConfig *memberlist.Config, pexConfig *peerExchangeConfig) {
		if threshold > 0 {
			pexConfig.replicaThreshold = threshold
		}
	}
}

func NewPeerExchange(
	reclaim ReclaimFunc,
	lister InitialMemberLister,
	grpcDialTimeout time.Duration,
	grpcDialOptions []grpc.DialOption, opts ...func(*memberlist.Config, *peerExchangeConfig)) (PeerExchangeServer, error) {
	memberManager := newPeerExchangeMemberManager(grpcDialTimeout, grpcDialOptions)

	memberConfig := memberlist.DefaultLANConfig()
	memberConfig.Events = memberManager

	pexConfig := &peerExchangeConfig{
		initialRetryInterval: 10 * time.Second,
		reSyncInterval:       time.Minute,
		replicaThreshold:     2,
	}

	for _, opt := range opts {
		opt(memberConfig, pexConfig)
	}

	if memberConfig.AdvertiseAddr == "" {
		// TODO support ipv6
		memberConfig.AdvertiseAddr = ip.IPv4.String()
	}

	logger.Infof("peer exchange initial retry interval: %s", pexConfig.initialRetryInterval)
	logger.Infof("peer exchange re-sync interval: %s", pexConfig.reSyncInterval)
	logger.Infof("peer exchange replica threshold: %d", pexConfig.replicaThreshold)

	pex := &peerExchange{
		config:        pexConfig,
		memberConfig:  memberConfig,
		memberManager: memberManager,
		reclaim:       reclaim,
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

func (p *peerExchange) SearchPeer(task string) SearchPeerResult {
	searchPeerResult := p.memberManager.peerPool.Search(task)
	if p.config.replicaThreshold <= 0 || len(searchPeerResult.Peers) == 0 {
		return searchPeerResult
	}

	switch searchPeerResult.Type {
	case SearchPeerResultTypeLocal:
		// check replica threshold and reclaim local cache
		if len(searchPeerResult.Peers) > p.config.replicaThreshold {
			p.tryReclaim(task, searchPeerResult)
		}
	case SearchPeerResultTypeRemote:
		if len(searchPeerResult.Peers) < p.config.replicaThreshold {
			searchPeerResult.Type = SearchPeerResultTypeReplica
			p.memberManager.logger.Debugf("task %s redirect replica threshold not reached, try to make replica from other peers", task)
		}
	}
	return searchPeerResult
}

func (p *peerExchange) tryReclaim(task string, searchPeerResult SearchPeerResult) {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	// reclaim with 1% probability for shrink double reclaim with other members
	if r.Int31n(100) == 0 {
		peer := searchPeerResult.Peers[0].PeerID
		searchPeerResult.Type = SearchPeerResultTypeRemote
		p.memberManager.logger.Debugf("task %s replica threshold reached, try to reclaim local peer cache %s", task, peer)
		err := p.reclaim(task, peer)
		if err != nil {
			p.memberManager.logger.Warnf("task %s peer %s reclaim local cache error: %s", task, peer, err)
		}
	}
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
	localMember.isLocal = true
	p.memberConfig.Delegate = newPeerExchangeDelegate(localMember)
	p.localMember = localMember

	p.memberManager.localMember = localMember
	p.memberManager.memberPool.localMember = localMember
	p.memberManager.logger = logger.With("component", "peerExchangeCluster", "hostID", localMember.HostID)

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
		time.Sleep(p.config.reSyncInterval)
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
			logger.Infof("%s re-sync add member: %s", p.localMember.HostID, member.HostID)
			p.memberManager.syncNode(member)
		}
		for _, id := range del {
			logger.Infof("%s re-sync del member: %s", p.localMember.HostID, id)
			p.memberManager.memberPool.UnRegisterByHostID(id)
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
