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

package log

import (
	"path"

	"go.uber.org/zap/zapcore"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/internal/dflog/logcore"
	"d7y.io/dragonfly/v2/internal/dfpath"
)

// SetCoreLevel sets core log level, export internal SetCoreLevel for using dragonfly as library
func SetCoreLevel(level zapcore.Level) {
	logcore.SetCoreLevel(level)
}

// SetGrpcLevel sets grpc log level, export internal SetGrpcLevel for using dragonfly as library
func SetGrpcLevel(level zapcore.Level) {
	logcore.SetGrpcLevel(level)
}

// SetupDaemon sets daemon log config: path, console
func SetupDaemon(logDir string, console bool) error {
	if console {
		return nil
	}

	if logDir == "" {
		logDir = path.Join(dfpath.LogDir, "daemon")
	}

	coreLogger, err := logcore.CreateLogger(path.Join(logDir, logcore.CoreLogFileName), false, false)
	if err != nil {
		return err
	}
	logger.SetCoreLogger(coreLogger.Sugar())

	grpcLogger, err := logcore.CreateLogger(path.Join(logDir, logcore.GrpcLogFileName), false, false)
	if err != nil {
		return err
	}
	logger.SetGrpcLogger(grpcLogger.Sugar())

	gcLogger, err := logcore.CreateLogger(path.Join(logDir, logcore.GCLogFileName), false, false)
	if err != nil {
		return err
	}
	logger.SetGcLogger(gcLogger.Sugar())

	return nil
}
