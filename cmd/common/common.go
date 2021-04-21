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

package common

import (
	"fmt"

	logger "d7y.io/dragonfly/v2/pkg/dflog"
	"d7y.io/dragonfly/v2/pkg/dflog/logcore"
	"d7y.io/dragonfly/v2/version"
	"github.com/go-echarts/statsview"
	"github.com/go-echarts/statsview/viewer"
	"github.com/phayes/freeport"
	"github.com/spf13/cobra"
	"go.uber.org/zap/zapcore"
)

func InitVerboseMode(verbose bool, pprofPort int) {
	if !verbose {
		return
	}

	logcore.SetCoreLevel(zapcore.DebugLevel)
	logcore.SetGrpcLevel(zapcore.DebugLevel)

	// enable go pprof and statsview
	go func() {
		if pprofPort == 0 {
			pprofPort, _ = freeport.GetFreePort()
		}

		debugAddr := fmt.Sprintf("localhost:%d", pprofPort)
		viewer.SetConfiguration(viewer.WithAddr(debugAddr))

		logger.With("pprof", fmt.Sprintf("http://%s/debug/pprof", debugAddr),
			"statsview", fmt.Sprintf("http://%s/debug/statsview", debugAddr)).
			Infof("enable pprof at %s", debugAddr)

		if err := statsview.New().Start(); err != nil {
			logger.Warnf("serve pprof error: %v", err)
		}
	}()
}

func AddCommonSubCmds(parent *cobra.Command) {
	parent.AddCommand(version.VersionCmd)
	parent.AddCommand(NewGenDocCommand(parent.Name()))
}
