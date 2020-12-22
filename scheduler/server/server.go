package server

import (
	"github.com/dragonflyoss/Dragonfly2/pkg/basic"
	"github.com/dragonflyoss/Dragonfly2/pkg/rpc"
	"github.com/dragonflyoss/Dragonfly2/scheduler/config"
	"github.com/dragonflyoss/Dragonfly2/scheduler/service"
	"github.com/dragonflyoss/Dragonfly2/scheduler/service/schedule_worker"
)

type Server struct {
	scheduler *service.SchedulerService
	worker    schedule_worker.IWorker
}

func NewServer() *Server {
	s := &Server{
		scheduler: service.CreateSchedulerService(),
	}
	s.worker = schedule_worker.CreateWorkerGroup(s.scheduler.GetScheduler())
	return s
}

func (s *Server) Start() (err error) {
	typ := config.GetConfig().Server.Type
	addr := config.GetConfig().Server.Addr

	if err != nil {
		return
	}
	go s.worker.Start()
	defer s.worker.Stop()
	lisAddr := basic.NetAddr{
		Type: typ,
		Addr: addr,
	}
	err = rpc.StartServer(lisAddr, &SchedulerServer{svc: s.scheduler, worker: s.worker})
	return
}
