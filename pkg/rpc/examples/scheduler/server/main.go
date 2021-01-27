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

package main

import (
	"context"
	"github.com/dragonflyoss/Dragonfly2/pkg/basic/env"
	"github.com/dragonflyoss/Dragonfly2/pkg/dferrors"
	logger "github.com/dragonflyoss/Dragonfly2/pkg/dflog"
	"github.com/dragonflyoss/Dragonfly2/pkg/rpc"
	"github.com/dragonflyoss/Dragonfly2/pkg/rpc/base"
	"github.com/dragonflyoss/Dragonfly2/pkg/rpc/base/common"
	"github.com/dragonflyoss/Dragonfly2/pkg/rpc/scheduler"
	_ "github.com/dragonflyoss/Dragonfly2/pkg/rpc/scheduler/server"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"os"
	"time"
)

type MyServer struct {
}

func (s *MyServer) RegisterPeerTask(ctx context.Context, ptr *scheduler.PeerTaskRequest) (*scheduler.RegisterResult, error) {
	return nil, dferrors.ErrClientError
}

func (s *MyServer) ReportPieceResult(stream scheduler.Scheduler_ReportPieceResultServer) error {
	time.Sleep(10 * time.Second)
	return dferrors.ErrInvalidArgument
}

func (s *MyServer) ReportPeerResult(ctx context.Context, pr *scheduler.PeerResult) (*base.ResponseState, error) {
	return nil, dferrors.ErrClientError
}

func (s *MyServer) LeaveTask(ctx context.Context, pt *scheduler.PeerTarget) (*base.ResponseState, error) {
	return nil, dferrors.ErrInvalidArgument
	//return nil,status.Error(codes.Aborted,"hello")
	e := status.New(codes.Unknown, "errorxxx")
	err := dferrors.ErrClientError
	if sta, err1 := e.WithDetails(common.NewState(err.Code, err.Message)); err1 == nil {
		return nil, sta.Err()
	} else {
		return nil, err
	}
}

func main() {
	os.Setenv(env.ActiveProfile, "local")
	logger.InitScheduler()

	rpc.StartTcpServer(32501, 32501, &MyServer{})
}
