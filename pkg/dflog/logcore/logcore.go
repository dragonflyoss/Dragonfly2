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
	"os"
	"path/filepath"
	"strings"

	"d7y.io/dragonfly/v2/pkg/unit"
	"d7y.io/dragonfly/v2/pkg/util/fileutils"
	"d7y.io/dragonfly/v2/pkg/util/fileutils/filerw"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
)

const (
	CoreLogFileName = "core.log"
	GrpcLogFileName = "grpc.log"
	GCLogFileName   = "gc.log"
)

var coreLevel = zap.NewAtomicLevelAt(zapcore.InfoLevel)
var grpcLevel = zap.NewAtomicLevelAt(zapcore.WarnLevel)

func CreateLogger(filePath string, maxSize int, maxAge int, maxBackups int, compress bool, stats bool) (*zap.Logger, error) {
	var syncer zapcore.WriteSyncer

	if maxAge < 0 || maxBackups < 0 {
		if err := fileutils.MkdirAll(filepath.Dir(filePath)); err != nil {
			return nil, err
		}
		fileInfo, err := os.Stat(filePath)
		if err == nil && fileInfo.Size() >= int64(maxSize)*unit.MB.ToNumber() {
			_, _ = filerw.CopyFile(filePath, filePath+".old")
			_ = filerw.CleanFile(filePath)
		}
		if syncer, _, err = zap.Open(filePath); err != nil {
			return nil, err
		}
	} else {
		rotateConfig := &lumberjack.Logger{
			Filename:   filePath,
			MaxSize:    maxSize,
			MaxAge:     maxAge,
			MaxBackups: maxBackups,
			LocalTime:  true,
			Compress:   compress,
		}
		syncer = zapcore.AddSync(rotateConfig)
	}

	encoderConfig := zap.NewProductionEncoderConfig()
	encoderConfig.EncodeTime = zapcore.TimeEncoderOfLayout("2006-01-02 15:04:05.000")

	level := zap.NewAtomicLevel()
	if strings.HasSuffix(filePath, GrpcLogFileName) {
		level = grpcLevel
	} else if strings.HasSuffix(filePath, CoreLogFileName) {
		level = coreLevel
	}

	core := zapcore.NewCore(
		zapcore.NewJSONEncoder(encoderConfig),
		syncer,
		level,
	)

	var opts []zap.Option
	if !stats {
		opts = append(opts, zap.AddCaller(), zap.AddStacktrace(zap.WarnLevel), zap.AddCallerSkip(1))
	}

	return zap.New(core, opts...), nil
}

func SetCoreLevel(level zapcore.Level) {
	coreLevel.SetLevel(level)
}

func SetGrpcLevel(level zapcore.Level) {
	grpcLevel.SetLevel(level)
}
