package schedule_worker

import (
	scheduler2 "github.com/dragonflyoss/Dragonfly2/pkg/grpc/scheduler"
	"github.com/dragonflyoss/Dragonfly2/scheduler/mgr"
	"hash/crc32"
)

type ISender interface{
	Start()
	Stop()
	Send(pid string, pkg *scheduler2.PiecePackage)
}

type SenderGroup struct {
	senderNum int
	senderList []*Sender
	stopCh chan struct{}
}

type Sender struct {
	jobChan chan *sendJob
	stopCh <-chan struct{}
}

type sendJob struct {
	pid *string
	pkg *scheduler2.PiecePackage
}

func CreateSender() *SenderGroup {
	sg := &SenderGroup{
		senderNum: 10,
	}
	return sg
}

func (sg *SenderGroup) Start() {
	sg.stopCh = make(chan struct{})
	for i:=0; i<sg.senderNum; i++ {
		s := &Sender{
			jobChan: make(chan *sendJob, 10000),
			stopCh: sg.stopCh,
		}
		s.Start()
		sg.senderList = append(sg.senderList, s)
	}
}

func (sg *SenderGroup) Stop() {
	close(sg.stopCh)
}

func (sg *SenderGroup) Send(pid string, pkg *scheduler2.PiecePackage) {
	sendId := crc32.ChecksumIEEE([]byte(pid))
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
			peerTask.Send(job.pkg)
		case <- s.stopCh:
			return
		}
	}
}