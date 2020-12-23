package schedule_worker

import (
	logger "github.com/dragonflyoss/Dragonfly2/pkg/log"
	scheduler2 "github.com/dragonflyoss/Dragonfly2/pkg/rpc/scheduler"
	"github.com/dragonflyoss/Dragonfly2/scheduler/config"
	"github.com/dragonflyoss/Dragonfly2/scheduler/mgr"
	"hash/crc32"
)

type ISender interface {
	Start()
	Stop()
	Send(pid string, pkg *scheduler2.PiecePackage)
}

type SenderGroup struct {
	senderNum  int
	chanSize   int
	senderList []*Sender
	stopCh     chan struct{}
}

type Sender struct {
	jobChan chan *sendJob
	stopCh  <-chan struct{}
}

type sendJob struct {
	pid *string
	pkg *scheduler2.PiecePackage
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
			jobChan: make(chan *sendJob, sg.chanSize),
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

func (sg *SenderGroup) Send(pid string, pkg *scheduler2.PiecePackage) {
	sendId := crc32.ChecksumIEEE([]byte(pid)) % uint32(sg.senderNum)
	sg.senderList[sendId].Send(pid, pkg)
}

func (s *Sender) Send(pid string, pkg *scheduler2.PiecePackage) {
	s.jobChan <- &sendJob{pid: &pid, pkg: pkg}
}

func (s *Sender) Start() {
	go s.doSend()
}

func (s *Sender) doSend() {
	for {
		select {
		case job := <-s.jobChan:
			peerTask, _ := mgr.GetPeerTaskManager().GetPeerTask(*job.pid)
			err := peerTask.Send(job.pkg)
			if err != nil {
				//TODO error
				return
			}
			if job.pkg.Done {
				break
			}
		case <-s.stopCh:
			return
		}
	}
}
