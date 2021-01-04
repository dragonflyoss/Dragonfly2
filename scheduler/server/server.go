package server

import (
	"fmt"
	"github.com/dragonflyoss/Dragonfly2/pkg/basic"
	logger "github.com/dragonflyoss/Dragonfly2/pkg/log"
	"github.com/dragonflyoss/Dragonfly2/pkg/rpc"
	"github.com/dragonflyoss/Dragonfly2/scheduler/config"
	"github.com/dragonflyoss/Dragonfly2/scheduler/service"
	"github.com/dragonflyoss/Dragonfly2/scheduler/service/schedule_worker"
)

type Server struct {
	scheduler *service.SchedulerService
	worker    schedule_worker.IWorker
	server    *SchedulerServer
	running   bool
}

func NewServer() *Server {
	s := &Server{
		running:   false,
		scheduler: service.CreateSchedulerService(),
	}
	s.worker = schedule_worker.CreateWorkerGroup(s.scheduler.GetScheduler())
	s.server = &SchedulerServer{svc: s.scheduler, worker: s.worker}
	return s
}

func (s *Server) Start() (err error) {
	typ := basic.TCP
	addr := fmt.Sprintf("%s:%d", config.GetConfig().Server.IP,
		config.GetConfig().Server.Port)

	if err != nil {
		return
	}
	go s.worker.Start()
	defer s.worker.Stop()
	lisAddr := basic.NetAddr{
		Type: typ,
		Addr: addr,
	}
	s.running = true
	logger.Infof("start server at %s:%s", typ, addr)
	err = rpc.StartServer(lisAddr, s.server)
	return
}

func (s *Server) Stop() (err error) {
	if s.running {
		s.running = false
		rpc.StopServer()
	}
	return
}

func (s *Server) GetServer() *SchedulerServer {
	return s.server
}

func (s *Server) GetSchedulerService() *service.SchedulerService {
	return s.scheduler
}

func (s *Server) GetWorker() schedule_worker.IWorker {
	return s.worker
}
