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

package scheduler

import (
	"strings"

	"d7y.io/dragonfly/v2/scheduler/config"
	"d7y.io/dragonfly/v2/scheduler/supervise"
)

type Scheduler interface {
	// ScheduleChildren schedule children to a peer
	ScheduleChildren(peer *supervise.Peer) (children []*supervise.Peer)

	// ScheduleParent schedule a parent and candidates to a peer
	ScheduleParent(peer *supervise.Peer) (parent *supervise.Peer, candidateParents []*supervise.Peer, hasParent bool)
}

type BuildOptions struct {
	TaskManager supervise.TaskMgr
	PeerManager supervise.PeerMgr
}

var (
	m                = make(map[string]Builder)
	defaultScheduler = "basic"
)

func Register(b Builder) {
	m[strings.ToLower(b.Name())] = b
}

func Get(name string) Builder {
	if b, ok := m[strings.ToLower(name)]; ok {
		return b
	}
	return nil
}

func SetDefaultScheduler(scheduler string) {
	defaultScheduler = scheduler
}

func GetDefaultScheduler() string {
	return defaultScheduler
}

type Builder interface {
	Build(cfg *config.SchedulerConfig, opts *BuildOptions) (Scheduler, error)

	Name() string
}
