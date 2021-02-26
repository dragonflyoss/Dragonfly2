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

package test

import (
	logger "d7y.io/dragonfly/v2/pkg/dflog"
	"d7y.io/dragonfly/v2/scheduler/mgr"
	"d7y.io/dragonfly/v2/scheduler/server"
	"d7y.io/dragonfly/v2/scheduler/test/common"
	"d7y.io/dragonfly/v2/scheduler/test/mock_cdn"
	"fmt"
	"github.com/go-echarts/statsview"
	"github.com/go-echarts/statsview/viewer"
	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"
	"time"
)

type SchedulerTestSuite struct {
	suite.Suite
	cdn *mock_cdn.MockCDN
	svr *server.Server
	ss  *server.SchedulerServer
}

func (suite *SchedulerTestSuite) SetupSuite() {
	logger.InitScheduler()
	logger.SetGcLogger(zap.NewNop().Sugar())
	logger.SetGrpcLogger(zap.NewNop().Sugar())

	suite.cdn= mock_cdn.NewMockCDN("localhost:8003", common.NewE2ELogger())
	suite.cdn.Start()
	time.Sleep(time.Second / 2)
	mgr.GetCDNManager().InitCDNClient()
	suite.svr = server.NewServer()
	suite.ss = suite.svr.GetServer()
	go suite.svr.Start()
	time.Sleep(time.Second / 2)
	go func() {
		// enable go pprof and statsview
		// port, _ := freeport.GetFreePort()
		port := 8888
		debugListen := fmt.Sprintf("localhost:%d", port)
		viewer.SetConfiguration(viewer.WithAddr(debugListen))
		logger.With("pprof", fmt.Sprintf("http://%s/debug/pprof", debugListen),
			"statsview", fmt.Sprintf("http://%s/debug/statsview", debugListen)).
			Infof("enable debug at http://%s", debugListen)
		if err := statsview.New().Start(); err != nil {
			logger.Warnf("serve go pprof error: %s", err)
		}
	}()
}

func (suite *SchedulerTestSuite) TearDownSuite() {
	suite.svr.Stop()
	if suite.cdn != nil {
		suite.cdn.Stop()
	}
}
