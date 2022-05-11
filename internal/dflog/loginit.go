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

func InitManager(verbose, console bool, dir string) error {
	if console {
		return createConsoleLogger(verbose)
	}

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

	return createFileLogger(verbose, meta, logDir)
}

func InitScheduler(verbose, console bool, dir string) error {
	if console {
		return createConsoleLogger(verbose)
	}

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

	return createFileLogger(verbose, meta, logDir)
}

func InitCdnSystem(verbose, console bool, dir string) error {
	if console {
		return createConsoleLogger(verbose)
	}
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

	return createFileLogger(verbose, meta, logDir)
}

func InitDaemon(verbose, console bool, dir string) error {
	if console {
		return createConsoleLogger(verbose)
	}

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

	return createFileLogger(verbose, meta, logDir)
}

func InitDfget(verbose, console bool, dir string) error {
	if console {
		return createConsoleLogger(verbose)
	}

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

	return createFileLogger(verbose, meta, logDir)
}

func createConsoleLogger(verbose bool) error {
	levels = nil
	config := zap.NewDevelopmentConfig()
	config.Level = zap.NewAtomicLevelAt(zap.InfoLevel)
	if verbose {
		config.Level = zap.NewAtomicLevelAt(zap.DebugLevel)
	}
	log, err := config.Build(zap.AddCaller(), zap.AddStacktrace(zap.WarnLevel), zap.AddCallerSkip(1))
	if err == nil {
		sugar := log.Sugar()
		SetCoreLogger(sugar)
		SetGrpcLogger(sugar)
		SetGCLogger(sugar)
		SetStorageGCLogger(sugar)
		SetKeepAliveLogger(sugar)
		SetStatSeedLogger(log)
		SetDownloadLogger(log)
		SetJobLogger(sugar)
	}
	levels = append(levels, config.Level)
	startLoggerSignalHandler()
	return nil
}

func createFileLogger(verbose bool, meta []logInitMeta, logDir string) error {
	levels = nil

	for _, m := range meta {
		log, level, err := CreateLogger(path.Join(logDir, m.fileName), false, false, verbose)
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

func InitDfcache(console bool, dir string) error {
	logDir := filepath.Join(dir, "dfcache")

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

	return createFileLogger(console, meta, logDir)
}
