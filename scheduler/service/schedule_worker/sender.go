package schedule_worker

import (
	logger "github.com/dragonflyoss/Dragonfly/v2/pkg/dflog"
	"github.com/dragonflyoss/Dragonfly/v2/scheduler/config"
	"github.com/dragonflyoss/Dragonfly/v2/scheduler/mgr"
	"github.com/dragonflyoss/Dragonfly/v2/scheduler/types"
	"hash/crc32"
)

type ISender interface {
	Start()
	Stop()
	Send(peerTask *types.PeerTask)
}

type SenderGroup struct {
	senderNum  int
	chanSize   int
	senderList []*Sender
	stopCh     chan struct{}
}

type Sender struct {
	jobChan chan *string
	stopCh  <-chan struct{}
}

func CreateSender() *SenderGroup {
	senderNum := config.GetConfig().Worker.SenderNum
	chanSize := config.GetConfig().Worker.SenderJobPoolSize
	sg := &SenderGroup{
		senderNum: senderNum,
		chanSize:  chanSize,
	}
	return sg
}

func (sg *SenderGroup) Start() {
	sg.stopCh = make(chan struct{})
	for i := 0; i < sg.senderNum; i++ {
		s := &Sender{
			jobChan: make(chan *string, sg.chanSize),
			stopCh:  sg.stopCh,
		}
		s.Start()
		sg.senderList = append(sg.senderList, s)
	}
	logger.Infof("start sender worker : %d", sg.senderNum)
}

func (sg *SenderGroup) Stop() {
	close(sg.stopCh)
	logger.Infof("stop sender worker : %d", sg.senderNum)
}

func (sg *SenderGroup) Send(peerTask *types.PeerTask) {
	sendId := crc32.ChecksumIEEE([]byte(peerTask.Pid)) % uint32(sg.senderNum)
	sg.senderList[sendId].Send(peerTask)
}

func (s *Sender) Send(peerTask *types.PeerTask) {
	s.jobChan <- &peerTask.Pid
}

func (s *Sender) Start() {
	go s.doSend()
}

func (s *Sender) doSend() {
	for {
		select {
		case job := <-s.jobChan:
			peerTask, _ := mgr.GetPeerTaskManager().GetPeerTask(*job)
			if peerTask == nil {
				break
			}
			err := peerTask.Send()
			if err != nil {
				//TODO error
				logger.Errorf("[%s][%s]: send result failed : %v", peerTask.Task, peerTask.Pid, err.Error())
				break
			} else {
				logger.Debugf("[%s][%s]: send result success", peerTask.Task.TaskId, peerTask.Pid)
			}
			if peerTask.Success {
				break
			}
		case <-s.stopCh:
			return
		}
	}
}
