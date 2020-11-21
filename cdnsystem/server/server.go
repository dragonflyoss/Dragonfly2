/*
 * Copyright The Dragonfly Authors.
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
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/sourceclient"
	"net"
	"net/http"
	"time"

	"github.com/dragonflyoss/Dragonfly2/cdnsystem/config"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/daemon/mgr"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/daemon/mgr/dfgettask"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/daemon/mgr/gc"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/daemon/mgr/peer"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/daemon/mgr/pieceerror"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/daemon/mgr/preheat"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/daemon/mgr/progress"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/daemon/mgr/scheduler"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/daemon/mgr/task"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/sourceclient/httpclient"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/store"
	"github.com/dragonflyoss/Dragonfly2/version"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
)

var dfgetLogger *logrus.Logger

// Server is supernode server struct.
type Server struct {
	Config        *config.Config
	PeerMgr       mgr.PeerMgr  // peer管理
	TaskMgr       mgr.TaskMgr  // task管理
	DfgetTaskMgr  mgr.DfgetTaskMgr
	ProgressMgr   mgr.ProgressMgr  // 进度管理
	GCMgr         mgr.GCMgr  // 垃圾回收
	PieceErrorMgr mgr.PieceErrorMgr  // 分片错误处理
	PreheatMgr    mgr.PreheatManager  // 预热管理

	sourceClient sourceclient.SourceClient
}

// New creates a brand new server instance.
func New(cfg *config.Config, logger *logrus.Logger, register prometheus.Registerer) (*Server, error) {
	var err error
	// register supernode build information
	version.NewBuildInfo("supernode", register)

	dfgetLogger = logger

	sm, err := store.NewManager(cfg)
	if err != nil {
		return nil, err
	}
	storeLocal, err := sm.Get(store.LocalStorageDriver)
	if err != nil {
		return nil, err
	}

	originClient := httpclient.NewHttpSourceClient()
	peerMgr, err := peer.NewManager(register)
	if err != nil {
		return nil, err
	}

	dfgetTaskMgr, err := dfgettask.NewManager(cfg, register)
	if err != nil {
		return nil, err
	}

	progressMgr, err := progress.NewManager(cfg)
	if err != nil {
		return nil, err
	}

	schedulerMgr, err := scheduler.NewManager(cfg, progressMgr)
	if err != nil {
		return nil, err
	}

	cdnMgr, err := mgr.GetCDNManager(cfg, storeLocal, progressMgr, originClient, register)
	if err != nil {
		return nil, err
	}

	taskMgr, err := task.NewManager(cfg, peerMgr, dfgetTaskMgr, progressMgr, cdnMgr,
		schedulerMgr, originClient, register)
	if err != nil {
		return nil, err
	}

	gcMgr, err := gc.NewManager(cfg, taskMgr, peerMgr, dfgetTaskMgr, progressMgr, cdnMgr, register)
	if err != nil {
		return nil, err
	}

	pieceErrorMgr, err := pieceerror.NewManager(cfg, gcMgr, cdnMgr)
	if err != nil {
		return nil, err
	}

	preheatMgr, err := preheat.NewManager(cfg)
	if err != nil {
		return nil, err
	}

	return &Server{
		Config:        cfg,
		PeerMgr:       peerMgr,
		TaskMgr:       taskMgr,
		DfgetTaskMgr:  dfgetTaskMgr,
		ProgressMgr:   progressMgr,
		GCMgr:         gcMgr,
		PieceErrorMgr: pieceErrorMgr,
		PreheatMgr:    preheatMgr,
		sourceClient:  originClient,
	}, nil
}

// Start runs supernode server.
func (s *Server) Start() error {
	router := createRouter(s)

	address := fmt.Sprintf("0.0.0.0:%d", s.Config.ListenPort)

	l, err := net.Listen("tcp", address)
	if err != nil {
		logrus.Errorf("failed to listen port %d: %v", s.Config.ListenPort, err)
		return err
	}

	// start to handle piece error
	s.PieceErrorMgr.StartHandleError(context.Background())
	s.GCMgr.StartGC(context.Background())

	server := &http.Server{
		Handler:           router,
		ReadTimeout:       time.Minute * 10,
		ReadHeaderTimeout: time.Minute * 10,
		IdleTimeout:       time.Minute * 10,
	}
	return server.Serve(l)
}
