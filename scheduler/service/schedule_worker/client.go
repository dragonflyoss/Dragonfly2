package schedule_worker

import (
	"fmt"
	logger "github.com/dragonflyoss/Dragonfly2/pkg/log"
	"github.com/dragonflyoss/Dragonfly2/pkg/rpc/scheduler"
	"github.com/dragonflyoss/Dragonfly2/scheduler/mgr"
	"github.com/dragonflyoss/Dragonfly2/scheduler/types"
	"io"
)

type Client struct {
	client scheduler.Scheduler_PullPieceTasksServer
	worker IWorker
}

func CreateClient(client scheduler.Scheduler_PullPieceTasksServer, worker IWorker) *Client {
	c := &Client{
		client: client,
		worker: worker,
	}
	return c
}

func (c *Client) Start() {
	c.doWork()
	return
}

func (c *Client) doWork() {
	pr, err := c.client.Recv()
	if err == io.EOF {
		return
	} else if err != nil {
		// TODO error
		return
	}
	pid := pr.SrcPid
	peerTask, _ := mgr.GetPeerTaskManager().GetPeerTask(pid)
	peerTask.SetClient(c.client)

	for {
		if pr != nil {
			logger.Debugf("[%s][%s]: recieve a pieceResult %v - %v", pr.TaskId, pr.SrcPid, pr.PieceNum, pr.Success)
		}
		peerTask, _ := c.UpdatePieceResult(pr)
		if peerTask != nil {
			c.worker.ReceiveJob(peerTask)
		}
		pr, err = c.client.Recv()
		if err == io.EOF {
			return
		} else if err != nil {
			// TODO error
			return
		}
	}
}

func (c *Client) UpdatePieceResult(pr *scheduler.PieceResult) (peerTask *types.PeerTask, err error) {
	if pr == nil {
		return
	}
	peerTask, _ = mgr.GetPeerTaskManager().GetPeerTask(pr.SrcPid)
	if peerTask == nil {
		err = fmt.Errorf("[%s][%s]: peer task not exited", pr.TaskId, pr.SrcPid)
		logger.Errorf(err.Error())
		return
	}
	dstPeerTask, _ := mgr.GetPeerTaskManager().GetPeerTask(pr.DstPid)
	if dstPeerTask != nil {
		dstPeerTask.Host.AddLoad(-1)
		logger.Debugf("[%s][%s]: host[%s] [%d] add host load", peerTask.Task.TaskId, peerTask.Pid, dstPeerTask.Host.Uuid, pr.PieceNum)
		// TODO move to a global trigger
		c.worker.TriggerSchedule(dstPeerTask)
	}
	peerTask.AddPieceStatus(&types.PieceStatus{
		PieceNum:  pr.PieceNum,
		SrcPid:    pr.SrcPid,
		DstPid:    pr.DstPid,
		Success:   pr.Success,
		ErrorCode: pr.ErrorCode,
		Cost:      pr.Cost,
	})
	peerTask.DeleteDownloadingPiece(pr.PieceNum)

	return
}
