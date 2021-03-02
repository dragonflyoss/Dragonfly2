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

package test

import (
	"context"
	"d7y.io/dragonfly/v2/pkg/dfcodes"
	"d7y.io/dragonfly/v2/pkg/idutils"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	"d7y.io/dragonfly/v2/pkg/rpc/scheduler"
	"d7y.io/dragonfly/v2/scheduler/mgr"
	"math/rand"
	"time"
)

func (suite *SchedulerTestSuite) Test101RegisterPeerTask() {
	ctx := context.TODO()
	request := &scheduler.PeerTaskRequest{
		Url:    "http://dragonfly.com/test1",
		Filter: "",
		BizId:  "12345",
		UrlMata: &base.UrlMeta{
			Md5:   "",
			Range: "",
		},
		PeerId: "rpc001",
		PeerHost: &scheduler.PeerHost{
			Uuid:           "host001",
			Ip:             "127.0.0.1",
			RpcPort:        23457,
			DownPort:       23456,
			HostName:       "host001",
			SecurityDomain: "",
			Location:       "",
			Idc:            "",
			NetTopology:    "",
		},
	}
	pkg, err := suite.ss.RegisterPeerTask(ctx, request)
	if !suite.Equal(nil, err, "register peer task failed") {
		return
	}
	if !suite.NotEmpty(pkg, "register peer task pkg return nil") {
		return
	}

	task, _ := mgr.GetTaskManager().GetTask(pkg.TaskId)
	if !suite.Equal(true, task != nil && task.TaskId == pkg.TaskId, "get task failed") {
		return
	}

	host, _ := mgr.GetHostManager().GetHost(request.PeerHost.Uuid)
	if !suite.Equal(true, host != nil && host.Uuid == request.PeerHost.Uuid, "get host failed") {
		return
	}

	peerTask, _ := mgr.GetPeerTaskManager().GetPeerTask(request.PeerId)
	if !suite.Equal(true, peerTask != nil && peerTask.Host != nil && peerTask.Host.Uuid == request.PeerHost.Uuid,
		"get peerTask Failed") {
		return
	}

	peerTask = host.GetPeerTask(request.PeerId)
	if !suite.NotEmpty(peerTask, "peerTask do not add into host") {
		return
	}
}

func (suite *SchedulerTestSuite) Test102SchedulerPeerTask() {
	ctx := context.TODO()
	request := &scheduler.PeerTaskRequest{
		Url:    "http://dragonfly.com/test1",
		Filter: "",
		BizId:  "12345",
		UrlMata: &base.UrlMeta{
			Md5:   "",
			Range: "",
		},
		PeerId: "rpc002",
		PeerHost: &scheduler.PeerHost{
			Uuid:           "host002",
			Ip:             "127.0.0.1",
			RpcPort:        22457,
			DownPort:       22456,
			HostName:       "host002",
			SecurityDomain: "",
			Location:       "",
			Idc:            "",
			NetTopology:    "",
		},
	}
	pkg, err := suite.ss.RegisterPeerTask(ctx, request)
	if !suite.Equal(nil, err, "register peer task failed") {
		return
	}
	if !suite.NotEmpty(pkg, "register peer task pkg return nil") {
		return
	}

	peerTask, _ := mgr.GetPeerTaskManager().GetPeerTask(request.PeerId)
	if !suite.Equal(true, peerTask != nil && peerTask.Host != nil && peerTask.Host.Uuid == request.PeerHost.Uuid,
		"get peerTask Failed") {
		return
	}

	p := peerTask.Task.GetOrCreatePiece(0)
	p.RangeStart = 0
	p.RangeSize = 100
	p.PieceMd5 = ""
	p.PieceOffset = 10
	p.PieceStyle = base.PieceStyle_PLAIN

	suite.svr.GetWorker().ReceiveUpdatePieceResult(&scheduler.PieceResult{
		TaskId:    peerTask.Task.TaskId,
		SrcPid:    "prc001",
		PieceNum:  0,
		Success:   true,
		Code:      dfcodes.Success,
		BeginTime: uint64(time.Now().UnixNano() / int64(time.Millisecond)),
		EndTime:   uint64(time.Now().UnixNano()/int64(time.Millisecond)) + uint64(rand.Int63n(1000)),
	})

	time.Sleep(time.Second)

	scheduler := suite.svr.GetSchedulerService().GetScheduler()

	_, _, err = scheduler.SchedulerParent(peerTask)
	if !suite.Empty(err, "scheduler failed") {
		return
	}

	suite.NotEmpty(peerTask.GetParent(), "scheduler failed parent is null")
}

func (suite *SchedulerTestSuite) Test103ReportResult() {
	ctx := context.TODO()
	taskId := idgen.GenerateTaskId("http://dragonfly.com/test1", "", nil)
	var result = &scheduler.PeerResult{
		TaskId:         taskId,
		PeerId:         "prc001",
		SrcIp:          "prc001",
		SecurityDomain: "",
		Idc:            "",
		ContentLength:  20,
		Traffic:        20,
		Cost:           20,
		Success:        true,
		Code:           dfcodes.Success,
	}
	_, err := suite.ss.ReportPeerResult(ctx, result)
	if !suite.Empty(err) {
		return
	}
	peerTask, _ := mgr.GetPeerTaskManager().GetPeerTask(result.PeerId)
	if !suite.Equal(true, peerTask != nil && peerTask.Success == result.Success && peerTask.Code == result.Code,
		"peerTask report Failed") {
		return
	}
}

func (suite *SchedulerTestSuite) Test104LeaveTask() {
	ctx := context.TODO()
	taskId := idgen.GenerateTaskId("http://dragonfly.com/test1", "", nil)
	var target = &scheduler.PeerTarget{
		TaskId: taskId,
		PeerId: "prc001",
	}
	resp, err := suite.ss.LeaveTask(ctx, target)
	if !suite.Empty(err) {
		return
	}
	if !suite.Equal(true, resp != nil && resp.Success, "leave task Failed") {
		return
	}
	peerTask, _ := mgr.GetPeerTaskManager().GetPeerTask(target.PeerId)
	if !suite.Empty(peerTask, "leave task failed") {
		return
	}

	host, _ := mgr.GetHostManager().GetHost("host001")
	if !suite.NotEmpty(host, "get host Failed") {
		return
	}

	peerTask = host.GetPeerTask(target.PeerId)
	if !suite.Empty(peerTask, "peerTask do not delete from host") {
		return
	}
}


