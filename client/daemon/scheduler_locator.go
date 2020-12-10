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

package daemon

import (
	"fmt"
	"sync"

	"github.com/dragonflyoss/Dragonfly2/pkg/grpc/scheduler"
	"google.golang.org/grpc"
)

// SchedulerLocator is used to locate all available schedulers
type SchedulerLocator interface {
	// Next return a SchedulerClient
	// when all=true, all SchedulerClient is returned, should call Refresh
	Next() (s scheduler.SchedulerClient, all bool, err error)
	// Refresh reload all scheduler
	Refresh(ph *scheduler.PeerHost) error
}

type staticSchedulerLocator struct {
	lock        sync.Locker
	addresses   []string
	next        int
	schedulers  []scheduler.SchedulerClient
	dialOptions []grpc.DialOption
}

func NewStaticSchedulerLocator(addresses []string, opts ...grpc.DialOption) (SchedulerLocator, error) {
	l := &staticSchedulerLocator{
		lock:        &sync.Mutex{},
		addresses:   addresses,
		next:        0,
		schedulers:  nil,
		dialOptions: opts,
	}
	return l, l.Refresh(nil)
}

func (l *staticSchedulerLocator) Next() (s scheduler.SchedulerClient, all bool, err error) {
	l.lock.Lock()
	defer l.lock.Unlock()
	if len(l.schedulers) == 0 {
		return nil, false, fmt.Errorf("no avaliable schedulers")
	}
	if l.next >= len(l.schedulers) {
		return nil, false, fmt.Errorf("all avaliable schedulers returned")
	}
	s = l.schedulers[l.next]
	l.next++
	if l.next == len(l.schedulers) {
		all = true
	}
	return
}

func (l *staticSchedulerLocator) Refresh(ph *scheduler.PeerHost) error {
	l.lock.Lock()
	defer l.lock.Unlock()
	var errs []error
	l.schedulers = nil
	l.next = 0
	for _, address := range l.addresses {
		conn, err := grpc.Dial(address, l.dialOptions...)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		l.schedulers = append(l.schedulers, scheduler.NewSchedulerClient(conn))
	}
	if len(l.schedulers) == 0 {
		return fmt.Errorf("no avaliable schedulers, refresh error: %#v", errs)
	}
	return nil
}
