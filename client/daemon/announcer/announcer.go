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
	managerclient "d7y.io/dragonfly/v2/pkg/rpc/manager/client"
	schedulerclient "d7y.io/dragonfly/v2/pkg/rpc/scheduler/client"
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
	enableSeedPeer  bool
	schedulerClient schedulerclient.Client
	managerClient   managerclient.Client
}

// Option is a functional option for configuring the announcer.
type Option func(s *announcer)

// WithManagerClient sets the grpc client of manager.
func WithManagerClient(client managerclient.Client) Option {
	return func(a *announcer) {
		a.managerClient = client
	}
}

// WithEnableSeedPeer enables seed peer mode for peer.
func WithEnableSeedPeer(enable bool) Option {
	return func(a *announcer) {
		a.enableSeedPeer = enable
	}
}

// New returns a new Announcer interface.
func New(schedulerClient schedulerclient.Client, options ...Option) Announcer {
	a := &announcer{
		schedulerClient: schedulerClient,
		enableSeedPeer:  false,
	}

	for _, opt := range options {
		opt(a)
	}

	return a
}

// Started announcer server.
func (a *announcer) Serve() error {
	return nil
}

// Stop announcer server.
func (a *announcer) Stop() error {
	return nil
}
