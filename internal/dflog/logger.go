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

	"go.uber.org/zap"
	"google.golang.org/grpc/grpclog"
)

var (
	CoreLogger       *zap.SugaredLogger
	GrpcLogger       *zap.SugaredLogger
	GcLogger         *zap.SugaredLogger
	JobLogger        *zap.SugaredLogger
	KeepAliveLogger  *zap.SugaredLogger
	StatPeerLogger   *zap.Logger
	StatSeedLogger   *zap.Logger
	DownloaderLogger *zap.Logger
)

func init() {
	config := zap.NewDevelopmentConfig()
	config.Level = zap.NewAtomicLevelAt(zap.DebugLevel)
	log, err := config.Build(zap.AddCaller(), zap.AddStacktrace(zap.WarnLevel), zap.AddCallerSkip(1))
	if err == nil {
		sugar := log.Sugar()
		SetCoreLogger(sugar)
		SetGrpcLogger(sugar)
		SetGcLogger(sugar)
		SetKeepAliveLogger(sugar)
		SetStatPeerLogger(log)
		SetStatSeedLogger(log)
		SetDownloadLogger(log)
		SetJobLogger(sugar)
	}
}

func SetCoreLogger(log *zap.SugaredLogger) {
	CoreLogger = log
}

func SetGcLogger(log *zap.SugaredLogger) {
	GcLogger = log
}

func SetKeepAliveLogger(log *zap.SugaredLogger) {
	KeepAliveLogger = log
}

func SetStatPeerLogger(log *zap.Logger) {
	StatPeerLogger = log
}

func SetStatSeedLogger(log *zap.Logger) {
	StatSeedLogger = log
}

func SetDownloadLogger(log *zap.Logger) {
	DownloaderLogger = log
}

func SetGrpcLogger(log *zap.SugaredLogger) {
	GrpcLogger = log
	grpclog.SetLoggerV2(&zapGrpc{GrpcLogger})
}

func SetJobLogger(log *zap.SugaredLogger) {
	JobLogger = log
}

type SugaredLoggerOnWith struct {
	withArgs []interface{}
}

func With(args ...interface{}) *SugaredLoggerOnWith {
	return &SugaredLoggerOnWith{
		withArgs: args,
	}
}

func WithTaskID(taskID string) *SugaredLoggerOnWith {
	return &SugaredLoggerOnWith{
		withArgs: []interface{}{"taskId", taskID},
	}
}

func WithTaskAndPeerID(taskID, peerID string) *SugaredLoggerOnWith {
	return &SugaredLoggerOnWith{
		withArgs: []interface{}{"taskId", taskID, "peerID", peerID},
	}
}

func WithTaskIDAndURL(taskID, url string) *SugaredLoggerOnWith {
	return &SugaredLoggerOnWith{
		withArgs: []interface{}{"taskId", taskID, "url", url},
	}
}

func (log *SugaredLoggerOnWith) Infof(template string, args ...interface{}) {
	CoreLogger.Infow(fmt.Sprintf(template, args...), log.withArgs...)
}

func (log *SugaredLoggerOnWith) Info(args ...interface{}) {
	CoreLogger.Infow(fmt.Sprint(args...), log.withArgs...)
}

func (log *SugaredLoggerOnWith) Warnf(template string, args ...interface{}) {
	CoreLogger.Warnw(fmt.Sprintf(template, args...), log.withArgs...)
}

func (log *SugaredLoggerOnWith) Warn(args ...interface{}) {
	CoreLogger.Warnw(fmt.Sprint(args...), log.withArgs...)
}

func (log *SugaredLoggerOnWith) Errorf(template string, args ...interface{}) {
	CoreLogger.Errorw(fmt.Sprintf(template, args...), log.withArgs...)
}

func (log *SugaredLoggerOnWith) Error(args ...interface{}) {
	CoreLogger.Errorw(fmt.Sprint(args...), log.withArgs...)
}

func (log *SugaredLoggerOnWith) Debugf(template string, args ...interface{}) {
	CoreLogger.Debugw(fmt.Sprintf(template, args...), log.withArgs...)
}

func (log *SugaredLoggerOnWith) Debug(args ...interface{}) {
	CoreLogger.Debugw(fmt.Sprint(args...), log.withArgs...)
}

func Infof(template string, args ...interface{}) {
	CoreLogger.Infof(template, args...)
}

func Info(args ...interface{}) {
	CoreLogger.Info(args...)
}

func Warnf(template string, args ...interface{}) {
	CoreLogger.Warnf(template, args...)
}

func Errorf(template string, args ...interface{}) {
	CoreLogger.Errorf(template, args...)
}

func Error(args ...interface{}) {
	CoreLogger.Error(args...)
}

func Debugf(template string, args ...interface{}) {
	CoreLogger.Debugf(template, args...)
}

func Fatalf(template string, args ...interface{}) {
	CoreLogger.Fatalf(template, args...)
}

func Fatal(args ...interface{}) {
	CoreLogger.Fatal(args...)
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

func (z *zapGrpc) V(level int) bool {
	return level > 0
}
