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
	"path"
	"path/filepath"

	"go.uber.org/zap"
)

type logInitMeta struct {
	fileName             string
	setSugaredLoggerFunc func(*zap.SugaredLogger)
	setLoggerFunc        func(log *zap.Logger)
}

func createLogger(console bool, meta []logInitMeta, logDir string) error {
	if console {
		startLoggerSignalHandler()
		return nil
	}
	// drop console level
	levels = nil

	for _, m := range meta {
		log, level, err := CreateLogger(path.Join(logDir, m.fileName), false, false)
		if err != nil {
			return err
		}
		if m.setSugaredLoggerFunc != nil {
			m.setSugaredLoggerFunc(log.Sugar())
		} else {
			m.setLoggerFunc(log)
		}

		levels = append(levels, level)
	}
	startLoggerSignalHandler()
	return nil
}

func InitManager(console bool, dir string) error {
	logDir := filepath.Join(dir, "manager")

	var meta = []logInitMeta{
		{
			fileName:             CoreLogFileName,
			setSugaredLoggerFunc: SetCoreLogger,
		},
		{
			fileName:             GrpcLogFileName,
			setSugaredLoggerFunc: SetGrpcLogger,
		},
		{
			fileName:             GCLogFileName,
			setSugaredLoggerFunc: SetGCLogger,
		},
		{
			fileName:             JobLogFileName,
			setSugaredLoggerFunc: SetJobLogger,
		},
	}

	return createLogger(console, meta, logDir)
}

func InitScheduler(console bool, dir string) error {
	logDir := filepath.Join(dir, "scheduler")

	var meta = []logInitMeta{
		{
			fileName:             CoreLogFileName,
			setSugaredLoggerFunc: SetCoreLogger,
		},
		{
			fileName:             GrpcLogFileName,
			setSugaredLoggerFunc: SetGrpcLogger,
		},
		{
			fileName:             GCLogFileName,
			setSugaredLoggerFunc: SetGCLogger,
		},
		{
			fileName:             JobLogFileName,
			setSugaredLoggerFunc: SetJobLogger,
		},
	}

	return createLogger(console, meta, logDir)
}

func InitCdnSystem(console bool, dir string) error {
	logDir := filepath.Join(dir, "cdn")
	var meta = []logInitMeta{
		{
			fileName:             CoreLogFileName,
			setSugaredLoggerFunc: SetCoreLogger,
		},
		{
			fileName:             GrpcLogFileName,
			setSugaredLoggerFunc: SetGrpcLogger,
		},
		{
			fileName:             GCLogFileName,
			setSugaredLoggerFunc: SetGCLogger,
		},
		{
			fileName:             StorageGCLogFileName,
			setSugaredLoggerFunc: SetStorageGCLogger,
		},
		{
			fileName:             JobLogFileName,
			setSugaredLoggerFunc: SetJobLogger,
		},
		{
			fileName:      StatSeedLogFileName,
			setLoggerFunc: SetStatSeedLogger,
		},
		{
			fileName:      DownloaderLogFileName,
			setLoggerFunc: SetDownloadLogger,
		},
		{
			fileName:             KeepAliveLogFileName,
			setSugaredLoggerFunc: SetKeepAliveLogger,
		},
	}

	return createLogger(console, meta, logDir)
}

func InitDaemon(console bool, dir string) error {
	logDir := filepath.Join(dir, "daemon")

	var meta = []logInitMeta{
		{
			fileName:             CoreLogFileName,
			setSugaredLoggerFunc: SetCoreLogger,
		},
		{
			fileName:             GrpcLogFileName,
			setSugaredLoggerFunc: SetGrpcLogger,
		},
		{
			fileName:             GCLogFileName,
			setSugaredLoggerFunc: SetGCLogger,
		},
	}

	return createLogger(console, meta, logDir)
}

func InitDfget(console bool, dir string) error {
	logDir := filepath.Join(dir, "dfget")

	var meta = []logInitMeta{
		{
			fileName:             CoreLogFileName,
			setSugaredLoggerFunc: SetCoreLogger,
		},
		{
			fileName:             GrpcLogFileName,
			setSugaredLoggerFunc: SetGrpcLogger,
		},
	}

	return createLogger(console, meta, logDir)
}
