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

//go:generate mockgen -destination mocks/dynconfig_mock.go -source dynconfig.go -package mocks

package config

import (
	"errors"
	"time"

	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/resolver"

	managerv1 "d7y.io/api/v2/pkg/apis/manager/v1"

	managerclient "d7y.io/dragonfly/v2/pkg/rpc/manager/client"
)

type SourceType string

const (
	// LocalSourceType represents read configuration from local file.
	LocalSourceType = "local"

	// ManagerSourceType represents pulling configuration from manager.
	ManagerSourceType = "manager"
)

// Watch dynconfig interval.
var watchInterval = 10 * time.Second

type DynconfigData struct {
	SeedPeers     []*managerv1.SeedPeer
	Schedulers    []*managerv1.Scheduler
	ObjectStorage *managerv1.ObjectStorage
}

type Dynconfig interface {
	// Get the dynamic seed peers config.
	GetSeedPeers() ([]*managerv1.SeedPeer, error)

	// Get the dynamic schedulers resolve addrs.
	GetResolveSchedulerAddrs() ([]resolver.Address, error)

	// Get the dynamic schedulers config.
	GetSchedulers() ([]*managerv1.Scheduler, error)

	// Get the dynamic schedulers cluster id.
	GetSchedulerClusterID() uint64

	// Get the dynamic object storage config.
	GetObjectStorage() (*managerv1.ObjectStorage, error)

	// Get the dynamic config.
	Get() (*DynconfigData, error)

	// Refresh refreshes dynconfig in cache.
	Refresh() error

	// Register allows an instance to register itself to listen/observe events.
	Register(Observer)

	// Deregister allows an instance to remove itself from the collection of observers/listeners.
	Deregister(Observer)

	// Notify publishes new events to listeners.
	Notify() error

	// OnNotify allows an event to be published to the dynconfig.
	// Used for listening changes of the local configuration.
	OnNotify(*DaemonOption)

	// Serve the dynconfig listening service.
	Serve() error

	// Stop the dynconfig listening service.
	Stop() error
}

type dynconfig struct {
	sourceType           SourceType
	managerClient        managerclient.V1
	cacheDir             string
	transportCredentials credentials.TransportCredentials
}

type Observer interface {
	// OnNotify allows an event to be published to interface implementations.
	OnNotify(*DynconfigData)
}

// DynconfigOption is a functional option for configuring the dynconfig.
type DynconfigOption func(d *dynconfig) error

// WithManagerClient set the manager client.
func WithManagerClient(c managerclient.V1) DynconfigOption {
	return func(d *dynconfig) error {
		d.managerClient = c
		return nil
	}
}

// WithCacheDir set the cache dir.
func WithCacheDir(dir string) DynconfigOption {
	return func(d *dynconfig) error {
		d.cacheDir = dir
		return nil
	}
}

// WithTransportCredentials returns a DialOption which configures a connection
// level security credentials (e.g., TLS/SSL).
func WithTransportCredentials(creds credentials.TransportCredentials) DynconfigOption {
	return func(d *dynconfig) error {
		d.transportCredentials = creds
		return nil
	}
}

// New returns a new dynconfig interface.
func NewDynconfig(sourceType SourceType, cfg *DaemonOption, options ...DynconfigOption) (Dynconfig, error) {
	d := &dynconfig{
		sourceType: sourceType,
	}

	for _, opt := range options {
		if err := opt(d); err != nil {
			return nil, err
		}
	}

	if err := d.validate(); err != nil {
		return nil, err
	}

	var (
		di  Dynconfig
		err error
	)
	switch sourceType {
	case ManagerSourceType:
		di, err = newDynconfigManager(cfg, d.managerClient, d.cacheDir, d.transportCredentials)
		if err != nil {
			return nil, err
		}
	case LocalSourceType:
		di, err = newDynconfigLocal(cfg, d.transportCredentials)
		if err != nil {
			return nil, err
		}
	default:
		return nil, errors.New("unknown source type")
	}

	return di, nil
}

// validate dynconfig parameters.
func (d *dynconfig) validate() error {
	if d.sourceType == ManagerSourceType {
		if d.managerClient == nil {
			return errors.New("manager dynconfig requires parameter ManagerClient")
		}

		if d.cacheDir == "" {
			return errors.New("manager dynconfig requires parameter CacheDir")
		}
	}

	return nil
}
