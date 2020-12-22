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

package rpcpro

import (
	"fmt"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/config"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/daemon/mgr"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/server/rpcpro/seed_server"
	"github.com/dragonflyoss/Dragonfly2/pkg/grpc/cdnsystem"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"net"
)

// New creates a brand new server instance.
func NewRpcServer(cfg *config.Config, taskMgr mgr.SeedTaskMgr, gcMgr mgr.GCMgr) *RPCServer {
	return &RPCServer{
		Config:  cfg,
		TaskMgr: taskMgr,
		GCMgr:   gcMgr,
	}
}

type RPCServer struct {
	Config  *config.Config
	TaskMgr mgr.SeedTaskMgr // task管理
	GCMgr   mgr.GCMgr       // 垃圾回收
}

// Start runs cdn rpc server.
func (s *RPCServer) Start() (*grpc.Server, error) {
	address := fmt.Sprintf("0.0.0.0:%d", s.Config.ListenPort)
	rpcServer := grpc.NewServer()
	cdnsystem.RegisterSeederServer(rpcServer, seed_server.NewCdnSeedServer(s.TaskMgr))
	lis, err := net.Listen("tcp", address)
	if err != nil {
		logrus.Errorf("failed to listen port %d: %v", s.Config.ListenPort, err)
		return nil, err
	}
	if err := rpcServer.Serve(lis); err != nil {
		logrus.Errorf("failed to server %v", err)
	}
	return rpcServer, nil
}

func registerServers(s *grpc.Server, taskMgr mgr.SeedTaskMgr) {
	grpc.WithInsecure()
}
