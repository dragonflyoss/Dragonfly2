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
	"errors"
	"sync"

	"github.com/hashicorp/memberlist"
	"golang.org/x/exp/maps"
)

var (
	ErrIsAlreadyExists = errors.New("member is already exist")
	ErrNotFound        = errors.New("member not found")
)

type memberPool struct {
	members       *memberlist.Memberlist
	lock          *sync.RWMutex
	sendReceivers map[string]PeerMetadataSendReceiveCloser
}

func newMemberPool() *memberPool {
	return &memberPool{
		lock:          &sync.RWMutex{},
		sendReceivers: map[string]PeerMetadataSendReceiveCloser{},
	}
}

func (mp *memberPool) MemberKeys() []string {
	mp.lock.RLock()
	keys := maps.Keys(mp.sendReceivers)
	mp.lock.RUnlock()
	return keys
}

func (mp *memberPool) FindMember(ip string) (*MemberMeta, error) {
	var (
		memberMeta *MemberMeta
		err        error
	)
	for _, member := range mp.members.Members() {
		if member.Addr.String() == ip {
			memberMeta, err = ExtractNodeMeta(member)
			if err != nil {
				return nil, err
			}
		}
	}
	if memberMeta == nil {
		return nil, ErrNotFound
	}

	return memberMeta, nil
}

func (mp *memberPool) Register(ip string, sr PeerMetadataSendReceiveCloser) error {
	mp.lock.Lock()
	defer mp.lock.Unlock()

	if _, ok := mp.sendReceivers[ip]; ok {
		return ErrIsAlreadyExists
	}

	mp.sendReceivers[ip] = sr
	return nil
}

func (mp *memberPool) UnRegister(ip string) {
	mp.lock.Lock()
	defer mp.lock.Unlock()
	sr, ok := mp.sendReceivers[ip]
	if !ok {
		return
	}

	delete(mp.sendReceivers, ip)
	_ = sr.Close()
}

func (mp *memberPool) IsRegistered(ip string) bool {
	mp.lock.RLock()
	defer mp.lock.RUnlock()
	_, ok := mp.sendReceivers[ip]
	return ok
}
