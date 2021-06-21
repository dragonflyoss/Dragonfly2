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

package peer

import (
	"context"
	"time"

	"d7y.io/dragonfly/v2/client/daemon/storage"
	"d7y.io/dragonfly/v2/pkg/dfcodes"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	"d7y.io/dragonfly/v2/pkg/rpc/scheduler"
)

type streamPeerTaskCallback struct {
	ctx   context.Context
	ptm   *peerTaskManager
	req   *scheduler.PeerTaskRequest
	start time.Time
}

var _ TaskCallback = (*streamPeerTaskCallback)(nil)

func (p *streamPeerTaskCallback) GetStartTime() time.Time {
	return p.start
}

func (p *streamPeerTaskCallback) Init(pt Task) error {
	// prepare storage
	err := p.ptm.storageManager.RegisterTask(p.ctx,
		storage.RegisterTaskRequest{
			CommonTaskRequest: storage.CommonTaskRequest{
				PeerID:      pt.GetPeerID(),
				TaskID:      pt.GetTaskID(),
				Destination: "",
			},
			ContentLength: pt.GetContentLength(),
			TotalPieces:   pt.GetTotalPieces(),
		})
	if err != nil {
		pt.Log().Errorf("register task to storage manager failed: %s", err)
	}
	return err
}

func (p *streamPeerTaskCallback) Update(pt Task) error {
	// update storage
	err := p.ptm.storageManager.UpdateTask(p.ctx,
		&storage.UpdateTaskRequest{
			PeerTaskMetaData: storage.PeerTaskMetaData{
				PeerID: pt.GetPeerID(),
				TaskID: pt.GetTaskID(),
			},
			ContentLength: pt.GetContentLength(),
			TotalPieces:   pt.GetTotalPieces(),
		})
	if err != nil {
		pt.Log().Errorf("update task to storage manager failed: %s", err)
	}
	return err
}

func (p *streamPeerTaskCallback) Done(pt Task) error {
	var cost = time.Now().Sub(p.start).Milliseconds()
	pt.Log().Infof("stream peer task done, cost: %dms", cost)
	e := p.ptm.storageManager.Store(
		context.Background(),
		&storage.StoreRequest{
			CommonTaskRequest: storage.CommonTaskRequest{
				PeerID: pt.GetPeerID(),
				TaskID: pt.GetTaskID(),
			},
			MetadataOnly: true,
			TotalPieces:  pt.GetTotalPieces(),
		})
	if e != nil {
		return e
	}
	p.ptm.PeerTaskDone(p.req.PeerId)
	err := p.ptm.schedulerClient.ReportPeerResult(context.Background(), &scheduler.PeerResult{
		TaskId:         pt.GetTaskID(),
		PeerId:         pt.GetPeerID(),
		SrcIp:          p.ptm.host.Ip,
		SecurityDomain: p.ptm.host.SecurityDomain,
		Idc:            p.ptm.host.Idc,
		Url:            p.req.Url,
		ContentLength:  pt.GetContentLength(),
		Traffic:        pt.GetTraffic(),
		Cost:           uint32(cost),
		Success:        true,
		Code:           dfcodes.Success,
	})
	if err != nil {
		pt.Log().Errorf("report successful peer result, error: %v", err)
	} else {
		pt.Log().Infof("report successful peer result ok")
	}
	return nil
}

func (p *streamPeerTaskCallback) Fail(pt Task, code base.Code, reason string) error {
	p.ptm.PeerTaskDone(p.req.PeerId)
	var end = time.Now()
	pt.Log().Errorf("stream peer task failed, code: %d, reason: %s", code, reason)
	err := p.ptm.schedulerClient.ReportPeerResult(context.Background(), &scheduler.PeerResult{
		TaskId:         pt.GetTaskID(),
		PeerId:         pt.GetPeerID(),
		SrcIp:          p.ptm.host.Ip,
		SecurityDomain: p.ptm.host.SecurityDomain,
		Idc:            p.ptm.host.Idc,
		Url:            p.req.Url,
		ContentLength:  pt.GetContentLength(),
		Traffic:        pt.GetTraffic(),
		Cost:           uint32(end.Sub(p.start).Milliseconds()),
		Success:        false,
		Code:           code,
	})
	if err != nil {
		pt.Log().Errorf("report fail peer result, error: %v", err)
	} else {
		pt.Log().Infof("report fail peer result ok")
	}
	return nil
}
