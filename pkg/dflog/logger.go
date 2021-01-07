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
	"fmt"
	"github.com/dragonflyoss/Dragonfly2/pkg/basic/env"
	"github.com/dragonflyoss/Dragonfly2/pkg/util/fileutils"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc/grpclog"
	"gopkg.in/natefinch/lumberjack.v2"
	"os"
	"path/filepath"
)

var (
	bizLogger      *zap.SugaredLogger
	GrpcLogger     *zap.SugaredLogger
	StatPeerLogger *zap.Logger
	StatSeedLogger *zap.Logger
)

var LogLevel = zap.NewAtomicLevel()

type SugaredLoggerOnWith struct {
	withArgs []interface{}
}

func CreateLogger(filePath string, maxSize int, maxAge int, maxBackups int, compress bool, stats bool) *zap.Logger {
	if os.Getenv(env.ActiveProfile) == "local" {
		log, _ := zap.NewDevelopment(zap.AddCaller(), zap.AddStacktrace(zap.WarnLevel), zap.AddCallerSkip(1))
		return log
	}

	var syncer zapcore.WriteSyncer

	if maxAge < 0 || maxBackups < 0 {
		if err := os.MkdirAll(filepath.Dir(filePath), 0755); err != nil {
			panic(err)
		}
		fileInfo, err := os.Stat(filePath)
		if err == nil && fileInfo.Size() >= int64(maxSize*1024*1024) {
			_, _ = fileutils.CopyFile(filePath+".old", filePath)
			_ = os.Truncate(filePath, 0)
		}
		if syncer, _, err = zap.Open(filePath); err != nil {
			panic(err)
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

	core := zapcore.NewCore(
		zapcore.NewJSONEncoder(encoderConfig),
		syncer,
		LogLevel,
	)

	var opts []zap.Option
	if !stats {
		opts = append(opts, zap.AddCaller(), zap.AddStacktrace(zap.WarnLevel), zap.AddCallerSkip(1))
	}

	return zap.New(core, opts...)
}

func SetBizLogger(log *zap.SugaredLogger) {
	bizLogger = log
}

func SetStatPeerLogger(log *zap.Logger) {
	StatPeerLogger = log
}

func SetStatSeedLogger(log *zap.Logger) {
	StatSeedLogger = log
}

func SetGrpcLogger(log *zap.SugaredLogger) {
	GrpcLogger = log
	grpclog.SetLoggerV2(&zapGrpc{GrpcLogger})
}

func With(args ...interface{}) *SugaredLoggerOnWith {
	return &SugaredLoggerOnWith{
		withArgs: args,
	}
}

func (log *SugaredLoggerOnWith) Infof(template string, args ...interface{}) {
	bizLogger.Infow(fmt.Sprintf(template, args...), log.withArgs...)
}

func (log *SugaredLoggerOnWith) Warnf(template string, args ...interface{}) {
	bizLogger.Warnw(fmt.Sprintf(template, args...), log.withArgs...)
}

func (log *SugaredLoggerOnWith) Errorf(template string, args ...interface{}) {
	bizLogger.Errorw(fmt.Sprintf(template, args...), log.withArgs...)
}

func (log *SugaredLoggerOnWith) Debugf(template string, args ...interface{}) {
	bizLogger.Debugw(fmt.Sprintf(template, args...), log.withArgs...)
}

func Infof(template string, args ...interface{}) {
	bizLogger.Infof(template, args...)
}

func Warnf(template string, args ...interface{}) {
	bizLogger.Warnf(template, args...)
}

func Errorf(template string, args ...interface{}) {
	bizLogger.Errorf(template, args...)
}

func Debugf(template string, args ...interface{}) {
	bizLogger.Debugf(template, args...)
}

type zapGrpc struct {
	*zap.SugaredLogger
}

func (z *zapGrpc) Infoln(args ...interface{}) {
	z.SugaredLogger.Info(args...)
}

func (z *zapGrpc) Warning(args ...interface{}) {
	z.SugaredLogger.Warn(args...)
}

func (z *zapGrpc) Warningln(args ...interface{}) {
	z.SugaredLogger.Warn(args...)
}

func (z *zapGrpc) Warningf(format string, args ...interface{}) {
	z.SugaredLogger.Warnf(format, args...)
}

func (z *zapGrpc) Errorln(args ...interface{}) {
	z.SugaredLogger.Error(args...)
}

func (z *zapGrpc) Fatalln(args ...interface{}) {
	z.SugaredLogger.Fatal(args...)
}

func (z *zapGrpc) V(l int) bool {
	return true
}
