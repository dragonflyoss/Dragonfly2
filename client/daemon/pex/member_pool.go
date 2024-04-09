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
	"errors"
	"sync"

	"github.com/hashicorp/memberlist"
	"golang.org/x/exp/maps"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"d7y.io/api/v2/pkg/apis/dfdaemon/v1"

	logger "d7y.io/dragonfly/v2/internal/dflog"
)

var (
	ErrAlreadyExists = errors.New("member is already exist")
	ErrNotFound      = errors.New("member not found")
)

func IsErrAlreadyExists(err error) bool {
	if err == nil {
		return false
	}

	if errors.Is(err, ErrAlreadyExists) {
		return true
	}

	return status.Code(err) == codes.AlreadyExists
}

type memberPool struct {
	localMember   *MemberMeta
	members       *memberlist.Memberlist
	peerPool      *peerPool
	lock          *sync.RWMutex
	sendReceivers map[string]PeerMetadataSendReceiveCloser
}

func newMemberPool(peerPool *peerPool) *memberPool {
	return &memberPool{
		lock:          &sync.RWMutex{},
		peerPool:      peerPool,
		sendReceivers: map[string]PeerMetadataSendReceiveCloser{},
	}
}

func (mp *memberPool) MemberKeys() []string {
	mp.lock.RLock()
	keys := maps.Keys(mp.sendReceivers)
	mp.lock.RUnlock()
	return keys
}

func (mp *memberPool) FindMember(hostID string) (*MemberMeta, error) {
	for _, member := range mp.members.Members() {
		memberMeta, err := ExtractNodeMeta(member)
		if err != nil {
			logger.Errorf("extract node meta error: %s", err)
			continue
		}
		if memberMeta.HostID == hostID {
			return memberMeta, nil
		}
	}
	return nil, ErrNotFound
}

func (mp *memberPool) Register(member *MemberMeta, sr PeerMetadataSendReceiveCloser) error {
	hostID := member.HostID

	mp.lock.Lock()
	defer mp.lock.Unlock()

	if _, ok := mp.sendReceivers[hostID]; ok {
		return ErrAlreadyExists
	}

	mp.sendReceivers[hostID] = sr
	logger.Infof("%s/%s registered", member.IP, hostID)
	return nil
}

func (mp *memberPool) UnRegister(member *MemberMeta) {
	hostID := member.HostID

	mp.lock.Lock()
	defer mp.lock.Unlock()

	defer mp.peerPool.clean(hostID)
	sr, ok := mp.sendReceivers[hostID]
	if !ok {
		return
	}

	delete(mp.sendReceivers, hostID)
	logger.Infof("%s/%s unregistered", member.IP, hostID)
	_ = sr.Close()
}

func (mp *memberPool) UnRegisterByHostID(hostID string) {
	mp.lock.Lock()
	defer mp.lock.Unlock()

	defer mp.peerPool.clean(hostID)
	sr, ok := mp.sendReceivers[hostID]
	if !ok {
		return
	}

	delete(mp.sendReceivers, hostID)
	logger.Infof("%s unregistered", hostID)
	_ = sr.Close()
}

func (mp *memberPool) IsRegistered(hostID string) bool {
	mp.lock.RLock()
	defer mp.lock.RUnlock()
	_, ok := mp.sendReceivers[hostID]
	return ok
}

func (mp *memberPool) broadcast(data *dfdaemon.PeerExchangeData) {
	mp.lock.Lock()
	defer mp.lock.Unlock()

	// sync local cache
	mp.peerPool.Sync(mp.localMember, data)

	for hostID, sr := range mp.sendReceivers {
		logger.Debugf("send peer metadata to host %s, peer count: %d", hostID, len(data.PeerMetadatas))
		err := sr.Send(data)
		if err != nil {
			logger.Errorf("send peer metadata to host %s error: %s, UnRegister member in background", hostID, err)
			go mp.UnRegisterByHostID(hostID)
		}
	}
}
