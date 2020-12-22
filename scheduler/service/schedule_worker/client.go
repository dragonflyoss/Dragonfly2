package schedule_worker

import (
	"github.com/dragonflyoss/Dragonfly2/pkg/rpc/scheduler"
	"github.com/dragonflyoss/Dragonfly2/scheduler/mgr"
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
	go c.doWork()
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
		c.worker.ReceiveJob(pr)
		pr, err = c.client.Recv()
		if err == io.EOF {
			return
		} else if err != nil {
			// TODO error
			return
		}
	}
}
