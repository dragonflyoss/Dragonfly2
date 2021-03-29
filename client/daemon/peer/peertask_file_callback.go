package peer

import (
	"context"
	"time"

	"d7y.io/dragonfly/v2/client/daemon/storage"
	"d7y.io/dragonfly/v2/pkg/dfcodes"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	"d7y.io/dragonfly/v2/pkg/rpc/scheduler"
)

type filePeerTaskCallback struct {
	ctx   context.Context
	ptm   *peerTaskManager
	req   *FilePeerTaskRequest
	start time.Time
}

func (p *filePeerTaskCallback) GetStartTime() time.Time {
	return p.start
}

func (p *filePeerTaskCallback) Init(pt PeerTask) error {
	// prepare storage
	err := p.ptm.storageManager.RegisterTask(p.ctx,
		storage.RegisterTaskRequest{
			CommonTaskRequest: storage.CommonTaskRequest{
				PeerID:      pt.GetPeerID(),
				TaskID:      pt.GetTaskID(),
				Destination: p.req.Output,
			},
			ContentLength: pt.GetContentLength(),
			TotalPieces:   pt.GetTotalPieces(),
		})
	if err != nil {
		pt.Log().Errorf("register task to storage manager failed: %s", err)
	}
	return err
}

func (p *filePeerTaskCallback) Update(pt PeerTask) error {
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

func (p *filePeerTaskCallback) Done(pt PeerTask) error {
	var cost = time.Now().Sub(p.start).Milliseconds()
	pt.Log().Infof("file peer task done, cost: %dms", cost)
	e := p.ptm.storageManager.Store(
		context.Background(),
		&storage.StoreRequest{
			CommonTaskRequest: storage.CommonTaskRequest{
				PeerID:      pt.GetPeerID(),
				TaskID:      pt.GetTaskID(),
				Destination: p.req.Output,
			},
			MetadataOnly: false,
			TotalPieces:  pt.GetTotalPieces(),
		})
	if e != nil {
		return e
	}
	p.ptm.PeerTaskDone(p.req.PeerId)
	state, err := p.ptm.schedulerClient.ReportPeerResult(context.Background(), &scheduler.PeerResult{
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
		pt.Log().Infof("report successful peer result, response state: (%t, %d, %s)",
			state.Success, state.Code, state.Msg)
	}
	return nil
}

func (p *filePeerTaskCallback) Fail(pt PeerTask, code base.Code, reason string) error {
	p.ptm.PeerTaskDone(p.req.PeerId)
	var end = time.Now()
	state, err := p.ptm.schedulerClient.ReportPeerResult(context.Background(), &scheduler.PeerResult{
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
		pt.Log().Infof("report fail peer result, response state: (%t, %d, %s)",
			state.Success, state.Code, state.Msg)
	}
	return nil
}
