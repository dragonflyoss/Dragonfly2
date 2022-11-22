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

	managerv1 "d7y.io/api/pkg/apis/manager/v1"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	managerclient "d7y.io/dragonfly/v2/pkg/rpc/manager/client"
	"d7y.io/dragonfly/v2/scheduler/config"
)

// Announcer is the interface used for announce service.
type Announcer interface {
	// Started announcer server.
	Serve() error

	// Stop announcer server.
	Stop() error
}

// announcer provides announce function.
type announcer struct {
	config        *config.Config
	managerClient managerclient.Client
	done          chan struct{}
}

// Option is a functional option for configuring the announcer.
type Option func(s *announcer)

// New returns a new Announcer interface.
func New(cfg *config.Config, managerClient managerclient.Client) (Announcer, error) {
	a := &announcer{
		config:        cfg,
		managerClient: managerClient,
		done:          make(chan struct{}),
	}

	// Register to manager.
	if _, err := a.managerClient.UpdateScheduler(context.Background(), &managerv1.UpdateSchedulerRequest{
		SourceType:         managerv1.SourceType_SCHEDULER_SOURCE,
		HostName:           a.config.Server.Host,
		Ip:                 a.config.Server.AdvertiseIP,
		Port:               int32(a.config.Server.Port),
		Idc:                a.config.Host.IDC,
		Location:           a.config.Host.Location,
		NetTopology:        a.config.Host.NetTopology,
		SchedulerClusterId: uint64(a.config.Manager.SchedulerClusterID),
	}); err != nil {
		return nil, err
	}

	return a, nil
}

// Started announcer server.
func (a *announcer) Serve() error {
	logger.Info("announce scheduler to manager")
	if err := a.announceToManager(); err != nil {
		return err
	}

	return nil
}

// Stop announcer server.
func (a *announcer) Stop() error {
	close(a.done)
	return nil
}

// announceSeedPeer announces peer information to manager.
func (a *announcer) announceToManager() error {
	// Start keepalive to manager.
	a.managerClient.KeepAlive(a.config.Manager.KeepAlive.Interval, &managerv1.KeepAliveRequest{
		SourceType: managerv1.SourceType_SCHEDULER_SOURCE,
		HostName:   a.config.Server.Host,
		Ip:         a.config.Server.AdvertiseIP,
		ClusterId:  uint64(a.config.Manager.SchedulerClusterID),
	}, a.done)

	return nil
}
