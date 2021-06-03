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
	logger "d7y.io/dragonfly/v2/pkg/dflog"
)

func InitManager(console bool) error {
	if console {
		return nil
	}

	logDir := path.Join(basic.HomeDir, "logs/manager")

	coreLogger, err := CreateLogger(path.Join(logDir, CoreLogFileName), 300, 30, 0, false, false)
	if err != nil {
		return err
	}
	logger.SetCoreLogger(coreLogger.Sugar())

	grpcLogger, err := CreateLogger(path.Join(logDir, GrpcLogFileName), 300, 30, 0, false, false)
	if err != nil {
		return err
	}
	logger.SetGrpcLogger(grpcLogger.Sugar())

	gcLogger, err := CreateLogger(path.Join(logDir, "gc.log"), 300, 7, 0, false, false)
	if err != nil {
		return err
	}
	logger.SetGcLogger(gcLogger.Sugar())

	return nil
}

func InitScheduler(console bool) error {
	if console {
		return nil
	}

	logDir := path.Join(basic.HomeDir, "logs/scheduler")

	coreLogger, err := CreateLogger(path.Join(logDir, CoreLogFileName), 300, 30, 0, false, false)
	if err != nil {
		return err
	}
	logger.SetCoreLogger(coreLogger.Sugar())

	grpcLogger, err := CreateLogger(path.Join(logDir, GrpcLogFileName), 300, 30, 0, false, false)
	if err != nil {
		return err
	}
	logger.SetGrpcLogger(grpcLogger.Sugar())

	gcLogger, err := CreateLogger(path.Join(logDir, "gc.log"), 300, 7, 0, false, false)
	if err != nil {
		return err
	}
	logger.SetGcLogger(gcLogger.Sugar())

	statPeerLogger, err := CreateLogger(path.Join(logDir, "stat/peer.log"), 300, 30, 0, true, true)
	if err != nil {
		return err
	}
	logger.SetStatPeerLogger(statPeerLogger)

	return nil
}

func InitCdnSystem(console bool) error {
	if console {
		return nil
	}

	logDir := path.Join(basic.HomeDir, "logs/cdn")

	coreLogger, err := CreateLogger(path.Join(logDir, CoreLogFileName), 300, 30, 0, false, false)
	if err != nil {
		return err
	}
	logger.SetCoreLogger(coreLogger.Sugar())

	grpcLogger, err := CreateLogger(path.Join(logDir, GrpcLogFileName), 300, 30, 0, false, false)
	if err != nil {
		return err
	}
	logger.SetGrpcLogger(grpcLogger.Sugar())

	gcLogger, err := CreateLogger(path.Join(logDir, "gc.log"), 300, 7, 0, false, false)
	if err != nil {
		return err
	}
	logger.SetGcLogger(gcLogger.Sugar())

	statSeedLogger, err := CreateLogger(path.Join(logDir, "stat/seed.log"), 300, 30, 0, true, true)
	if err != nil {
		return err
	}
	logger.SetStatSeedLogger(statSeedLogger)

	downloaderLogger, err := CreateLogger(path.Join(logDir, "downloader.log"), 300, 7, 0, false, false)
	if err != nil {
		return err
	}
	logger.SetDownloadLogger(downloaderLogger)

	keepAliveLogger, err := CreateLogger(path.Join(logDir, "keepAlive.log"), 300, 7, 0, false, false)
	if err != nil {
		return err
	}
	logger.SetKeepAliveLogger(keepAliveLogger.Sugar())
	return nil
}

func InitDaemon(console bool) error {
	if console {
		return nil
	}

	logDir := path.Join(basic.HomeDir, "logs/daemon")

	coreLogger, err := CreateLogger(path.Join(logDir, CoreLogFileName), 100, 7, 14, false, false)
	if err != nil {
		return err
	}
	logger.SetCoreLogger(coreLogger.Sugar())

	grpcLogger, err := CreateLogger(path.Join(logDir, GrpcLogFileName), 100, 7, 14, false, false)
	if err != nil {
		return err
	}
	logger.SetGrpcLogger(grpcLogger.Sugar())

	gcLogger, err := CreateLogger(path.Join(logDir, GCLogFileName), 100, 7, 14, false, false)
	if err != nil {
		return err
	}
	logger.SetGcLogger(gcLogger.Sugar())

	return nil
}

func InitDfget(console bool) error {
	if console {
		return nil
	}

	logDir := path.Join(basic.HomeDir, "logs/dfget")

	coreLogger, err := CreateLogger(path.Join(logDir, CoreLogFileName), 100, 7, 14, false, false)
	if err != nil {
		return err
	}
	logger.SetCoreLogger(coreLogger.Sugar())

	grpcLogger, err := CreateLogger(path.Join(logDir, GrpcLogFileName), 100, 7, 14, false, false)
	if err != nil {
		return err
	}
	logger.SetGrpcLogger(grpcLogger.Sugar())

	return nil
}
