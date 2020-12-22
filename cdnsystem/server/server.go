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
	"fmt"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/config"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/daemon/mgr"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/daemon/mgr/gc"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/daemon/mgr/task"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/server/rpcpro/seed_server"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/source"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/store"
	"github.com/dragonflyoss/Dragonfly2/pkg/grpc/cdnsystem"
	"github.com/dragonflyoss/Dragonfly2/pkg/rate/ratelimiter"
	"github.com/dragonflyoss/Dragonfly2/version"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"net"
)

type Server struct {
	Config     *config.Config
	TaskMgr    mgr.SeedTaskMgr    // task管理
	GCMgr      mgr.GCMgr          // 垃圾回收
}

// New creates a brand new server instance.
func New(cfg *config.Config, register prometheus.Registerer) (*Server, error) {

	var err error
	version.NewBuildInfo("cdnnode", prometheus.DefaultRegisterer)

	storeMgr, err := store.NewManager(cfg)
	if err != nil {
		return nil, err
	}
	storeLocal, err := storeMgr.Get(store.LocalStorageDriver)

	sourceClient := source.NewSourceClient()

	rateLimiter := ratelimiter.NewRateLimiter(ratelimiter.TransRate(int64(cfg.MaxBandwidth-cfg.SystemReservedBandwidth)), 2)

	// cdn manager
	cdnMgr, err := mgr.GetCDNManager(cfg, storeLocal, sourceClient, rateLimiter, prometheus.DefaultRegisterer)
	if err != nil {
		return nil, err
	}
	// task manager
	taskMgr, err := task.NewManager(cfg, cdnMgr, sourceClient, prometheus.DefaultRegisterer)
	if err != nil {
		return nil, err
	}

	// gc manager
	gcMgr, err := gc.NewManager(cfg, taskMgr, cdnMgr, prometheus.DefaultRegisterer)
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
func (s *Server) Start() error {
	address := fmt.Sprintf("0.0.0.0:%d", s.Config.ListenPort)
	rpcServer := grpc.NewServer()
	cdnsystem.RegisterSeederServer(rpcServer, seed_server.NewCdnSeedServer(s.TaskMgr))
	lis, err := net.Listen("tcp", address)
	if err != nil {
		logrus.Errorf("failed to listen port %d: %v", s.Config.ListenPort, err)
		return err
	}
	return rpcServer.Serve(lis)
}
