package schedule_worker

import (
	logger "github.com/dragonflyoss/Dragonfly2/pkg/log"
	"github.com/dragonflyoss/Dragonfly2/pkg/rpc/scheduler"
	"github.com/dragonflyoss/Dragonfly2/scheduler/mgr"
	scheduler2 "github.com/dragonflyoss/Dragonfly2/scheduler/scheduler"
	"io"
)

type Client struct {
	client scheduler.Scheduler_PullPieceTasksServer
	worker IWorker
	scheduler *scheduler2.Scheduler
}

func CreateClient(client scheduler.Scheduler_PullPieceTasksServer, worker IWorker, scheduler *scheduler2.Scheduler) *Client {
	c := &Client{
		client: client,
		worker: worker,
		scheduler: scheduler,
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
		c.worker.ReceiveUpdatePieceResult(pr)
		pr, err = c.client.Recv()
		if err == io.EOF {
			return
		} else if err != nil {
			// TODO error
			return
		}
	}
}
