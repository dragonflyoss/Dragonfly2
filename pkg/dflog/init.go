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

import "github.com/dragonflyoss/Dragonfly2/pkg/basic"

func InitManager() error {
	logDir := basic.HomeDir + "/logs/dragonfly"

	if bizLogger, err := CreateLogger(logDir+"/manager.log", 300, 30, 0, false, false); err != nil {
		return err
	} else {
		SetBizLogger(bizLogger.Sugar())
	}

	if grpcLogger, err := CreateLogger(logDir+"/grpc.log", 300, 30, 0, false, false); err != nil {
		return err
	} else {
		SetGrpcLogger(grpcLogger.Sugar())
	}

	if gcLogger, err := CreateLogger(logDir+"/gc.log", 300, 7, 0, false, false); err != nil {
		return err
	} else {
		SetGcLogger(gcLogger.Sugar())
	}

	return nil
}

func InitScheduler() error {
	logDir := basic.HomeDir + "/logs/dragonfly"

	if bizLogger, err := CreateLogger(logDir+"/scheduler.log", 300, 30, 0, false, false); err != nil {
		return err
	} else {
		SetBizLogger(bizLogger.Sugar())
	}

	if grpcLogger, err := CreateLogger(logDir+"/grpc.log", 300, 30, 0, false, false); err != nil {
		return err
	} else {
		SetGrpcLogger(grpcLogger.Sugar())
	}

	if gcLogger, err := CreateLogger(logDir+"/gc.log", 300, 7, 0, false, false); err != nil {
		return err
	} else {
		SetGcLogger(gcLogger.Sugar())
	}

	if statPeerLogger, err := CreateLogger(logDir+"/stat/peer.log", 300, 30, 0, true, true); err != nil {
		return err
	} else {
		SetStatPeerLogger(statPeerLogger)
	}

	return nil
}

func InitCdnSystem() error {
	logDir := basic.HomeDir + "/logs/dragonfly"

	if bizLogger, err := CreateLogger(logDir+"/cdnsystem.log", 300, 30, 0, false, false); err != nil {
		return err
	} else {
		SetBizLogger(bizLogger.Sugar())
	}

	if grpcLogger, err := CreateLogger(logDir+"/grpc.log", 300, 30, 0, false, false); err != nil {
		return err
	} else {
		SetGrpcLogger(grpcLogger.Sugar())
	}

	if gcLogger, err := CreateLogger(logDir+"/gc.log", 300, 7, 0, false, false); err != nil {
		return err
	} else {
		SetGcLogger(gcLogger.Sugar())
	}

	if statSeedLogger, err := CreateLogger(logDir+"/stat/seed.log", 300, 30, 0, true, true); err != nil {
		return err
	} else {
		SetStatSeedLogger(statSeedLogger)
	}

	return nil
}

func InitDaemon() error {
	logDir := "/var/log/dragonfly"

	if bizLogger, err := CreateLogger(logDir+"/daemon.log", 100, 7, 14, false, false); err != nil {
		return err
	} else {
		SetBizLogger(bizLogger.Sugar())
	}

	if grpcLogger, err := CreateLogger(logDir+"/grpc.log", 100, 7, 14, false, false); err != nil {
		return err
	} else {
		SetGrpcLogger(grpcLogger.Sugar())
	}

	if gcLogger, err := CreateLogger(logDir+"/gc.log", 100, 7, 14, false, false); err != nil {
		return err
	} else {
		SetGcLogger(gcLogger.Sugar())
	}

	return nil
}

func InitDfget() error {
	logDir := "/var/log/dragonfly"

	if bizLogger, err := CreateLogger(logDir+"/dfget.log", 300, -1, -1, false, false); err != nil {
		return err
	} else {
		log := bizLogger.Sugar()
		SetBizLogger(log)
		SetGrpcLogger(log)
	}

	return nil
}
