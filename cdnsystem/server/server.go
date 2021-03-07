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
	_ "d7y.io/dragonfly/v2/cdnsystem/source/httpprotocol"
	logger "d7y.io/dragonfly/v2/pkg/dflog"
	_ "d7y.io/dragonfly/v2/pkg/rpc/cdnsystem/server"
)

import (
	"context"
	"d7y.io/dragonfly/v2/cdnsystem/config"
	"d7y.io/dragonfly/v2/cdnsystem/daemon/mgr"
	"d7y.io/dragonfly/v2/cdnsystem/daemon/mgr/cdn"
	"d7y.io/dragonfly/v2/cdnsystem/daemon/mgr/cdn/storage"
	"d7y.io/dragonfly/v2/cdnsystem/daemon/mgr/gc"
	"d7y.io/dragonfly/v2/cdnsystem/daemon/mgr/progress"
	"d7y.io/dragonfly/v2/cdnsystem/daemon/mgr/task"
	"d7y.io/dragonfly/v2/cdnsystem/server/service"
	"d7y.io/dragonfly/v2/cdnsystem/source"
	"d7y.io/dragonfly/v2/pkg/rpc"
	"fmt"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
)

type Server struct {
	Config  *config.Config
	TaskMgr mgr.SeedTaskMgr
	GCMgr   mgr.GCMgr
}

// New creates a brand new server instance.
func New(cfg *config.Config, register prometheus.Registerer) (*Server, error) {
	sb := storage.Get(cfg.StoragePattern, true)
	if sb == nil {
		return nil, fmt.Errorf("could not get storage for pattern: %s", cfg.StoragePattern)
	}
	logger.Debugf("storage pattern is %s", sb.Name())
	storageMgr, err := sb.Build()
	if err != nil {
		return nil, fmt.Errorf("failed to build storage: %v", err)
	}

	sourceClient, err := source.NewSourceClient(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create source client: %v", err)
	}
	// progress manager
	progressMgr, err := progress.NewManager(cfg, register)
	if err != nil {
		return nil, fmt.Errorf("failed to create progress manager: %v", err)
	}

	// cdn manager
	cdnMgr, err := cdn.NewManager(cfg, storageMgr, progressMgr, sourceClient, register)
	if err != nil {
		return nil, fmt.Errorf("failed to create cdn manager: %v", err)
	}

	// task manager
	taskMgr, err := task.NewManager(cfg, cdnMgr, progressMgr ,sourceClient, register)
	if err != nil {
		return nil, fmt.Errorf("failed to create task manager: %v", err)
	}
	storageMgr.SetTaskMgr(taskMgr)

	// gc manager
	gcMgr, err := gc.NewManager(cfg, taskMgr, cdnMgr, storageMgr, register)
	if err != nil {
		return nil, fmt.Errorf("failed to create gc manager: %v", err)
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
	seedServer, err := service.NewCdnSeedServer(s.Config, s.TaskMgr)
	if err != nil {
		return errors.Wrap(err, "create seedServer fail")
	}
	// start gc
	s.GCMgr.StartGC(context.Background())
	err = rpc.StartTcpServer(s.Config.ListenPort, s.Config.ListenPort, seedServer)
	if err != nil {
		return errors.Wrap(err, "failed to start tcp server")
	}
	return nil
}
