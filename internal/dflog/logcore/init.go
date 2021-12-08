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
	"path/filepath"

	logger "d7y.io/dragonfly/v2/internal/dflog"
)

func InitManager(console bool, dir string) error {
	if console {
		return nil
	}

	logDir := filepath.Join(dir, "manager")

	coreLogger, err := CreateLogger(path.Join(logDir, CoreLogFileName), false, false)
	if err != nil {
		return err
	}
	logger.SetCoreLogger(coreLogger.Sugar())

	grpcLogger, err := CreateLogger(path.Join(logDir, GrpcLogFileName), false, false)
	if err != nil {
		return err
	}
	logger.SetGrpcLogger(grpcLogger.Sugar())

	gcLogger, err := CreateLogger(path.Join(logDir, GCLogFileName), false, false)
	if err != nil {
		return err
	}
	logger.SetGcLogger(gcLogger.Sugar())

	jobLogger, err := CreateLogger(path.Join(logDir, JobLogFileName), false, false)
	if err != nil {
		return err
	}
	logger.SetJobLogger(jobLogger.Sugar())

	return nil
}

func InitScheduler(console bool, dir string) error {
	if console {
		return nil
	}

	logDir := filepath.Join(dir, "scheduler")

	coreLogger, err := CreateLogger(path.Join(logDir, CoreLogFileName), false, false)
	if err != nil {
		return err
	}
	logger.SetCoreLogger(coreLogger.Sugar())

	grpcLogger, err := CreateLogger(path.Join(logDir, GrpcLogFileName), false, false)
	if err != nil {
		return err
	}
	logger.SetGrpcLogger(grpcLogger.Sugar())

	gcLogger, err := CreateLogger(path.Join(logDir, GCLogFileName), false, false)
	if err != nil {
		return err
	}
	logger.SetGcLogger(gcLogger.Sugar())

	jobLogger, err := CreateLogger(path.Join(logDir, JobLogFileName), false, false)
	if err != nil {
		return err
	}
	logger.SetJobLogger(jobLogger.Sugar())

	statPeerLogger, err := CreateLogger(path.Join(logDir, StatPeerLogFileName), true, true)
	if err != nil {
		return err
	}
	logger.SetStatPeerLogger(statPeerLogger)

	return nil
}

func InitCdnSystem(console bool, dir string) error {
	if console {
		return nil
	}

	logDir := filepath.Join(dir, "cdn")

	coreLogger, err := CreateLogger(path.Join(logDir, CoreLogFileName), false, false)
	if err != nil {
		return err
	}
	logger.SetCoreLogger(coreLogger.Sugar())

	grpcLogger, err := CreateLogger(path.Join(logDir, GrpcLogFileName), false, false)
	if err != nil {
		return err
	}
	logger.SetGrpcLogger(grpcLogger.Sugar())

	gcLogger, err := CreateLogger(path.Join(logDir, GCLogFileName), false, false)
	if err != nil {
		return err
	}
	logger.SetGcLogger(gcLogger.Sugar())

	jobLogger, err := CreateLogger(path.Join(logDir, JobLogFileName), false, false)
	if err != nil {
		return err
	}
	logger.SetJobLogger(jobLogger.Sugar())

	statSeedLogger, err := CreateLogger(path.Join(logDir, StatSeedLogFileName), true, true)
	if err != nil {
		return err
	}
	logger.SetStatSeedLogger(statSeedLogger)

	downloaderLogger, err := CreateLogger(path.Join(logDir, DownloaderLogFileName), false, false)
	if err != nil {
		return err
	}
	logger.SetDownloadLogger(downloaderLogger)

	keepAliveLogger, err := CreateLogger(path.Join(logDir, KeepAliveLogFileName), false, false)
	if err != nil {
		return err
	}
	logger.SetKeepAliveLogger(keepAliveLogger.Sugar())
	return nil
}

func InitDaemon(console bool, dir string) error {
	if console {
		return nil
	}

	logDir := filepath.Join(dir, "daemon")

	coreLogger, err := CreateLogger(path.Join(logDir, CoreLogFileName), false, false)
	if err != nil {
		return err
	}
	logger.SetCoreLogger(coreLogger.Sugar())

	grpcLogger, err := CreateLogger(path.Join(logDir, GrpcLogFileName), false, false)
	if err != nil {
		return err
	}
	logger.SetGrpcLogger(grpcLogger.Sugar())

	gcLogger, err := CreateLogger(path.Join(logDir, GCLogFileName), false, false)
	if err != nil {
		return err
	}
	logger.SetGcLogger(gcLogger.Sugar())

	return nil
}

func InitDfget(console bool, dir string) error {
	if console {
		return nil
	}

	logDir := filepath.Join(dir, "dfget")

	coreLogger, err := CreateLogger(path.Join(logDir, CoreLogFileName), false, false)
	if err != nil {
		return err
	}
	logger.SetCoreLogger(coreLogger.Sugar())

	grpcLogger, err := CreateLogger(path.Join(logDir, GrpcLogFileName), false, false)
	if err != nil {
		return err
	}
	logger.SetGrpcLogger(grpcLogger.Sugar())

	return nil
}
