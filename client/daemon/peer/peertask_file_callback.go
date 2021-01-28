package peer

import (
	"context"
	"time"

	"github.com/dragonflyoss/Dragonfly2/client/daemon/storage"
	logger "github.com/dragonflyoss/Dragonfly2/pkg/dflog"
	"github.com/dragonflyoss/Dragonfly2/pkg/rpc/base"
	"github.com/dragonflyoss/Dragonfly2/pkg/rpc/scheduler"
)

type filePeerTaskCallback struct {
	ctx   context.Context
	ptm   *peerTaskManager
	req   *FilePeerTaskRequest
	start time.Time
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
		})
	if err != nil {
		logger.Errorf("register task to storage manager failed: %s", err)
	}
	return err
}

func (p *filePeerTaskCallback) Done(pt PeerTask) error {
	e := p.ptm.storageManager.Store(
		context.Background(),
		&storage.StoreRequest{
			PeerID:      pt.GetPeerID(),
			TaskID:      pt.GetTaskID(),
			Destination: p.req.Output,
		})
	if e != nil {
		return e
	}
	p.ptm.PeerTaskDone(p.req.PeerId)
	var end = time.Now()
	// TODO error handling
	p.ptm.scheduler.ReportPeerResult(p.ctx, &scheduler.PeerResult{
		TaskId:         pt.GetTaskID(),
		PeerId:         pt.GetPeerID(),
		SrcIp:          p.ptm.host.Ip,
		SecurityDomain: p.ptm.host.SecurityDomain,
		Idc:            p.ptm.host.Idc,
		Url:            p.req.Url,
		ContentLength:  pt.GetContentLength(),
		Traffic:        pt.GetTraffic(),
		Cost:           uint32(end.Sub(p.start).Milliseconds()),
		Success:        true,
		Code:           base.Code_SUCCESS,
	})
	return nil
}

func (p *filePeerTaskCallback) Fail(pt PeerTask, reason string) error {
	p.ptm.PeerTaskDone(p.req.PeerId)
	var end = time.Now()
	// TODO error handling
	p.ptm.scheduler.ReportPeerResult(p.ctx, &scheduler.PeerResult{
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
		Code:           base.Code_CLIENT_ERROR,
	})
	return nil
}
