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
	"fmt"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"syscall"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	logger "d7y.io/dragonfly/v2/internal/dflog"
)

type logInitMeta struct {
	fileName             string
	setSugaredLoggerFunc func(*zap.SugaredLogger)
	setLoggerFunc        func(log *zap.Logger)
}

var (
	levels []zap.AtomicLevel
	level  = zapcore.InfoLevel
)

func startLoggerSignalHandler() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGUSR1)

	go func() {
		for {
			select {
			case <-signals:
				level -= 1
				if level < zapcore.DebugLevel {
					level = zapcore.FatalLevel
				}

				// use fmt.Printf print change log level event when log level is great then info level
				fmt.Printf("change log level to %s\n", level.String())
				logger.Infof("change log level to %s\n", level.String())
				for _, l := range levels {
					l.SetLevel(level)
				}
			}
		}
	}()
}

func createLogger(meta []logInitMeta, logDir string) error {
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
	if console {
		return nil
	}

	logDir := filepath.Join(dir, "manager")

	var meta = []logInitMeta{
		{
			fileName:             CoreLogFileName,
			setSugaredLoggerFunc: logger.SetCoreLogger,
		},
		{
			fileName:             GrpcLogFileName,
			setSugaredLoggerFunc: logger.SetGrpcLogger,
		},
		{
			fileName:             GCLogFileName,
			setSugaredLoggerFunc: logger.SetGCLogger,
		},
		{
			fileName:             JobLogFileName,
			setSugaredLoggerFunc: logger.SetJobLogger,
		},
	}

	return createLogger(meta, logDir)
}

func InitScheduler(console bool, dir string) error {
	if console {
		return nil
	}

	logDir := filepath.Join(dir, "scheduler")

	var meta = []logInitMeta{
		{
			fileName:             CoreLogFileName,
			setSugaredLoggerFunc: logger.SetCoreLogger,
		},
		{
			fileName:             GrpcLogFileName,
			setSugaredLoggerFunc: logger.SetGrpcLogger,
		},
		{
			fileName:             GCLogFileName,
			setSugaredLoggerFunc: logger.SetGCLogger,
		},
		{
			fileName:             JobLogFileName,
			setSugaredLoggerFunc: logger.SetJobLogger,
		},
	}

	return createLogger(meta, logDir)
}

func InitCdnSystem(console bool, dir string) error {
	if console {
		return nil
	}

	logDir := filepath.Join(dir, "cdn")
	var meta = []logInitMeta{
		{
			fileName:             CoreLogFileName,
			setSugaredLoggerFunc: logger.SetCoreLogger,
		},
		{
			fileName:             GrpcLogFileName,
			setSugaredLoggerFunc: logger.SetGrpcLogger,
		},
		{
			fileName:             GCLogFileName,
			setSugaredLoggerFunc: logger.SetGCLogger,
		},
		{
			fileName:             StorageGCLogFileName,
			setSugaredLoggerFunc: logger.SetStorageGCLogger,
		},
		{
			fileName:             JobLogFileName,
			setSugaredLoggerFunc: logger.SetJobLogger,
		},
		{
			fileName:      StatSeedLogFileName,
			setLoggerFunc: logger.SetStatSeedLogger,
		},
		{
			fileName:      DownloaderLogFileName,
			setLoggerFunc: logger.SetDownloadLogger,
		},
		{
			fileName:             KeepAliveLogFileName,
			setSugaredLoggerFunc: logger.SetKeepAliveLogger,
		},
	}

	return createLogger(meta, logDir)
}

func InitDaemon(console bool, dir string) error {
	if console {
		return nil
	}

	logDir := filepath.Join(dir, "daemon")

	var meta = []logInitMeta{
		{
			fileName:             CoreLogFileName,
			setSugaredLoggerFunc: logger.SetCoreLogger,
		},
		{
			fileName:             GrpcLogFileName,
			setSugaredLoggerFunc: logger.SetGrpcLogger,
		},
		{
			fileName:             GCLogFileName,
			setSugaredLoggerFunc: logger.SetGCLogger,
		},
	}

	return createLogger(meta, logDir)
}

func InitDfget(console bool, dir string) error {
	if console {
		return nil
	}

	logDir := filepath.Join(dir, "dfget")

	var meta = []logInitMeta{
		{
			fileName:             CoreLogFileName,
			setSugaredLoggerFunc: logger.SetCoreLogger,
		},
		{
			fileName:             GrpcLogFileName,
			setSugaredLoggerFunc: logger.SetGrpcLogger,
		},
	}

	return createLogger(meta, logDir)
}
