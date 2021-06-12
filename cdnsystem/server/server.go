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

// cdn server
package server

import (
	"context"
	"fmt"
	"runtime"

	"d7y.io/dragonfly/v2/cdnsystem/config"
	"d7y.io/dragonfly/v2/cdnsystem/daemon/cdn"
	"d7y.io/dragonfly/v2/cdnsystem/daemon/cdn/storage"
	"d7y.io/dragonfly/v2/cdnsystem/daemon/gc"
	"d7y.io/dragonfly/v2/cdnsystem/daemon/progress"
	"d7y.io/dragonfly/v2/cdnsystem/daemon/task"
	"d7y.io/dragonfly/v2/cdnsystem/plugins"
	"d7y.io/dragonfly/v2/cdnsystem/server/service"
	"d7y.io/dragonfly/v2/pkg/basic/dfnet"
	"d7y.io/dragonfly/v2/pkg/rpc"
	"d7y.io/dragonfly/v2/pkg/rpc/cdnsystem/server"
	"d7y.io/dragonfly/v2/pkg/rpc/manager"
	configServer "d7y.io/dragonfly/v2/pkg/rpc/manager/client"
	"d7y.io/dragonfly/v2/pkg/util/net/iputils"
	"d7y.io/dragonfly/v2/pkg/util/stringutils"
	"github.com/pkg/errors"
)

type Server struct {
	Config       *config.Config
	seedServer   server.SeederServer
	configServer configServer.ManagerClient
}

// New creates a brand new server instance.
func New(cfg *config.Config) (*Server, error) {
	if ok := storage.IsSupport(cfg.StorageMode); !ok {
		return nil, fmt.Errorf("os %s is not support storage mode %s", runtime.GOOS, cfg.StorageMode)
	}
	if err := plugins.Initialize(cfg.Plugins); err != nil {
		return nil, err
	}

	// progress manager
	progressMgr, err := progress.NewManager()
	if err != nil {
		return nil, errors.Wrapf(err, "failed to create progress manager")
	}

	// storage manager
	storageMgr, ok := storage.Get(cfg.StorageMode)
	if !ok {
		return nil, fmt.Errorf("can not find storage pattern %s", cfg.StorageMode)
	}
	// cdn manager
	cdnMgr, err := cdn.NewManager(cfg, storageMgr, progressMgr)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to create cdn manager")
	}
	// task manager
	taskMgr, err := task.NewManager(cfg, cdnMgr, progressMgr)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to create task manager")
	}
	storageMgr.Initialize(taskMgr)
	// gc manager
	if err != nil {
		return nil, errors.Wrapf(err, "failed to create gc manager")
	}

	cdnSeedServer, err := service.NewCdnSeedServer(cfg, taskMgr)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create seedServer")
	}
	var cfgServer configServer.ManagerClient
	if !stringutils.IsBlank(cfg.ConfigServer) {
		cfgServer, err = configServer.NewClient([]dfnet.NetAddr{{
			Type: dfnet.TCP,
			Addr: cfg.ConfigServer,
		}})
		if err != nil {
			return nil, errors.Wrap(err, "failed to create config server")
		}
	}
	return &Server{
		Config:       cfg,
		seedServer:   cdnSeedServer,
		configServer: cfgServer,
	}, nil
}

func (s *Server) Serve() (err error) {
	defer func() {
		if rec := recover(); rec != nil {
			err = errors.New(fmt.Sprintf("%v", rec))
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// start gc
	err = gc.StartGC(ctx)
	if err != nil {
		return err
	}
	if s.configServer != nil {
		s.configServer.KeepAlive(ctx, &manager.KeepAliveRequest{
			HostName: iputils.HostName,
			Type:     manager.ResourceType_Cdn,
		})
	}
	err = rpc.StartTCPServer(s.Config.ListenPort, s.Config.ListenPort, s.seedServer)
	if err != nil {
		return errors.Wrap(err, "failed to start tcp server")
	}
	return nil
}

func (s *Server) Stop() {
	rpc.StopServer()
}
