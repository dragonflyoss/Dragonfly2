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

//go:generate mockgen -destination logger_mock.go -source logger.go -package gc

package gc

import (
	logger "d7y.io/dragonfly/v2/internal/dflog"
)

// Logger is the interface used in GC for logging.
type Logger interface {
	// Infof logs routine messages for GC.
	Infof(template string, args ...any)
	// Error logs error messages for GC.
	Errorf(template string, args ...any)
}

// gcLogger is default logger of dflog.
type gcLogger struct{}

// Infof logs routine messages for GC.
func (gl *gcLogger) Infof(template string, args ...any) {
	logger.CoreLogger.Infof(template, args)
}

// Error logs error messages for GC.
func (gl *gcLogger) Errorf(template string, args ...any) {
	logger.CoreLogger.Errorf(template, args)
}
