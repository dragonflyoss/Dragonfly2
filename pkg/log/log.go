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
	"go.uber.org/zap/zapcore"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/dfpath"
)

// SetCoreLevel sets core log level, export internal SetCoreLevel for using dragonfly as library
func SetCoreLevel(level zapcore.Level) {
	logger.SetCoreLevel(level)
}

// SetGrpcLevel sets grpc log level, export internal SetGrpcLevel for using dragonfly as library
func SetGrpcLevel(level zapcore.Level) {
	logger.SetGrpcLevel(level)
}

// SetupDaemon sets daemon log config: path, console
func SetupDaemon(logDir string, verbose bool, console bool, rotateConfig logger.LogRotateConfig) error {
	var options []dfpath.Option
	if logDir != "" {
		options = append(options, dfpath.WithLogDir(logDir))
	}

	d, err := dfpath.New(options...)
	if err != nil {
		return err
	}

	return logger.InitDaemon(verbose, console, d.LogDir(), rotateConfig)
}
