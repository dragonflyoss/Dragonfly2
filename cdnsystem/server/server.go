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

package server

import (
	"context"
	"fmt"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/config"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/daemon/mgr"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/daemon/mgr/gc"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/daemon/mgr/piece"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/daemon/mgr/task"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/source"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/store"
	"github.com/dragonflyoss/Dragonfly2/pkg/basic"
	"github.com/dragonflyoss/Dragonfly2/pkg/rpc"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"time"
)

type Server struct {
	Config  *config.Config
	TaskMgr mgr.SeedTaskMgr
	GCMgr   mgr.GCMgr
}

// New creates a brand new server instance.
func New(cfg *config.Config, register prometheus.Registerer) (*Server, error) {
	var err error

	storeMgr, err := store.NewManager(cfg)
	if err != nil {
		return nil, err
	}

	storeLocal, err := storeMgr.Get(store.LocalStorageDriver)
	if err != nil {
		return nil, err
	}

	sourceClient, err := source.NewSourceClient()
	if err != nil {
		return nil, err
	}

	publisher := piece.NewPublisher(100*time.Millisecond, 10)
	// cdn manager
	cdnMgr, err := mgr.GetCDNManager(cfg, storeLocal, sourceClient, register)
	if err != nil {
		return nil, err
	}
	// task manager
	taskMgr, err := task.NewManager(cfg, cdnMgr, publisher, sourceClient, register)
	if err != nil {
		return nil, err
	}
	// gc manager
	gcMgr, err := gc.NewManager(cfg, taskMgr, cdnMgr, register)
	if err != nil {
		return nil, err
	}

	return &Server{
		Config:  cfg,
		TaskMgr: taskMgr,
		GCMgr:   gcMgr,
	}, nil
}

// Start runs cdn server.
func (s *Server) Start() (err error) {
	defer func() {
		if err := recover(); err != nil {
			err = errors.New(fmt.Sprintf("%v", err))
		}
	}()
	lisAddr := basic.NetAddr{
		Type: basic.TCP,
		Addr: fmt.Sprintf(":%d", s.Config.ListenPort),
	}
	seedServer, err := NewCdnSeedServer(s.Config, s.TaskMgr)
	if err != nil {
		return errors.Wrap(err, "create seedServer fail")
	}
	err = rpc.StartServer(lisAddr, seedServer)
	if err != nil {
		return errors.Wrap(err, "start seedServer fail")
	}
	// start gc
	s.GCMgr.StartGC(context.Background())
	return nil
}
