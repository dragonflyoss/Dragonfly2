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

package logcore

import (
	"path"

	"d7y.io/dragonfly/v2/pkg/basic"
	"d7y.io/dragonfly/v2/pkg/dflog"
)

func InitManager(console bool) error {
	if console {
		return nil
	}

	logDir := path.Join(basic.HomeDir, "logs/dragonfly")

	if coreLogger, err := CreateLogger(path.Join(logDir, CoreLogFileName), 300, 30, 0, false, false); err != nil {
		return err
	} else {
		logger.SetCoreLogger(coreLogger.Sugar())
	}

	if grpcLogger, err := CreateLogger(path.Join(logDir, GrpcLogFileName), 300, 30, 0, false, false); err != nil {
		return err
	} else {
		logger.SetGrpcLogger(grpcLogger.Sugar())
	}

	if gcLogger, err := CreateLogger(path.Join(logDir, "gc.log"), 300, 7, 0, false, false); err != nil {
		return err
	} else {
		logger.SetGcLogger(gcLogger.Sugar())
	}

	return nil
}

func InitScheduler(console bool) error {
	if console {
		return nil
	}

	logDir := path.Join(basic.HomeDir, "logs/dragonfly")

	if coreLogger, err := CreateLogger(path.Join(logDir, CoreLogFileName), 300, 30, 0, false, false); err != nil {
		return err
	} else {
		logger.SetCoreLogger(coreLogger.Sugar())
	}

	if grpcLogger, err := CreateLogger(path.Join(logDir, GrpcLogFileName), 300, 30, 0, false, false); err != nil {
		return err
	} else {
		logger.SetGrpcLogger(grpcLogger.Sugar())
	}

	if gcLogger, err := CreateLogger(path.Join(logDir, "gc.log"), 300, 7, 0, false, false); err != nil {
		return err
	} else {
		logger.SetGcLogger(gcLogger.Sugar())
	}

	if statPeerLogger, err := CreateLogger(path.Join(logDir, "stat/peer.log"), 300, 30, 0, true, true); err != nil {
		return err
	} else {
		logger.SetStatPeerLogger(statPeerLogger)
	}

	return nil
}

func InitCdnSystem(console bool) error {
	if console {
		return nil
	}

	logDir := path.Join(basic.HomeDir, "logs/dragonfly")

	if coreLogger, err := CreateLogger(path.Join(logDir, CoreLogFileName), 300, 30, 0, false, false); err != nil {
		return err
	} else {
		logger.SetCoreLogger(coreLogger.Sugar())
	}

	if grpcLogger, err := CreateLogger(path.Join(logDir, GrpcLogFileName), 300, 30, 0, false, false); err != nil {
		return err
	} else {
		logger.SetGrpcLogger(grpcLogger.Sugar())
	}

	if gcLogger, err := CreateLogger(path.Join(logDir, "gc.log"), 300, 7, 0, false, false); err != nil {
		return err
	} else {
		logger.SetGcLogger(gcLogger.Sugar())
	}

	if statSeedLogger, err := CreateLogger(path.Join(logDir, "stat/seed.log"), 300, 30, 0, true, true); err != nil {
		return err
	} else {
		logger.SetStatSeedLogger(statSeedLogger)
	}

	return nil
}

func InitDaemon(console bool) error {
	if console {
		return nil
	}

	if coreLogger, err := CreateLogger(path.Join(clientLogDir, CoreLogFileName), 100, 7, 14, false, false); err != nil {
		return err
	} else {
		logger.SetCoreLogger(coreLogger.Sugar())
	}

	if grpcLogger, err := CreateLogger(path.Join(clientLogDir, GrpcLogFileName), 100, 7, 14, false, false); err != nil {
		return err
	} else {
		logger.SetGrpcLogger(grpcLogger.Sugar())
	}

	if gcLogger, err := CreateLogger(path.Join(clientLogDir, "gc.log"), 100, 7, 14, false, false); err != nil {
		return err
	} else {
		logger.SetGcLogger(gcLogger.Sugar())
	}

	return nil
}

func InitDfget(console bool) error {
	if console {
		return nil
	}

	if dfgetLogger, err := CreateLogger(path.Join(clientLogDir, "dfget.log"), 300, -1, -1, false, false); err != nil {
		return err
	} else {
		log := dfgetLogger.Sugar()
		logger.SetCoreLogger(log)
		logger.SetGrpcLogger(log)
	}

	return nil
}
