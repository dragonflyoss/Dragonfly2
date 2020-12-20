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

package logger

import (
	"github.com/dragonflyoss/Dragonfly2/pkg/basic"
	"go.uber.org/zap"
	"testing"
)

func TestLogger(t *testing.T) {
	SetBizLogger(CreateLogger(basic.HomeDir+"/dragonfly/daemon.log", 10, 1, 1, false, false).Sugar())
	Infof("hello:%s", "world")

	SetStatSeedLogger(CreateLogger(basic.HomeDir+"/dragonfly/stat.log", 10, 1, 1, true, true))
	StatSeedLogger.Info("stat info", zap.String("hello", "world"))

	SetBizLogger(CreateLogger(basic.HomeDir+"/dragonfly/dfget.log", 10, -1, -1, false, false).Sugar())
	Infof("hello:%s", "world")
}
