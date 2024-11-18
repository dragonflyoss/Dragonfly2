/*
 *     Copyright 2022 The Dragonfly Authors
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

//go:generate mockgen -destination mocks/announcer_mock.go -source announcer.go -package mocks

package announcer

import (
	"context"

	managerv2 "d7y.io/api/v2/pkg/apis/manager/v2"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	managerclient "d7y.io/dragonfly/v2/pkg/rpc/manager/client"
	"d7y.io/dragonfly/v2/scheduler/config"
)

// Announcer is the interface used for announce service.
type Announcer interface {
	// Started announcer server.
	Serve()

	// Stop announcer server.
	Stop()
}

// announcer provides announce function.
type announcer struct {
	config        *config.Config
	managerClient managerclient.V2
	done          chan struct{}
}

// Option is a functional option for configuring the announcer.
type Option func(s *announcer)

// New returns a new Announcer interface.
func New(cfg *config.Config, managerClient managerclient.V2, schedulerFeatures []string, options ...Option) (Announcer, error) {
	a := &announcer{
		config:        cfg,
		managerClient: managerClient,
		done:          make(chan struct{}),
	}

	for _, opt := range options {
		opt(a)
	}

	// Register to manager.
	if _, err := a.managerClient.UpdateScheduler(context.Background(), &managerv2.UpdateSchedulerRequest{
		SourceType:         managerv2.SourceType_SCHEDULER_SOURCE,
		Hostname:           a.config.Server.Host,
		Ip:                 a.config.Server.AdvertiseIP.String(),
		Port:               int32(a.config.Server.AdvertisePort),
		Idc:                &a.config.Host.IDC,
		Location:           &a.config.Host.Location,
		SchedulerClusterId: uint64(a.config.Manager.SchedulerClusterID),
		Features:           schedulerFeatures,
	}); err != nil {
		return nil, err
	}

	return a, nil
}

// Started announcer server.
func (a *announcer) Serve() {
	logger.Info("announce scheduler to manager")
	go a.announceToManager()
}

// Stop announcer server.
func (a *announcer) Stop() {
	close(a.done)
}

// announceSeedPeer announces peer information to manager.
func (a *announcer) announceToManager() {
	a.managerClient.KeepAlive(a.config.Manager.KeepAlive.Interval, &managerv2.KeepAliveRequest{
		SourceType: managerv2.SourceType_SCHEDULER_SOURCE,
		Hostname:   a.config.Server.Host,
		Ip:         a.config.Server.AdvertiseIP.String(),
		ClusterId:  uint64(a.config.Manager.SchedulerClusterID),
	}, a.done)
}
