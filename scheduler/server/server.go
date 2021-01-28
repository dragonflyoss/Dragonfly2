package server

import (
	logger "github.com/dragonflyoss/Dragonfly2/pkg/dflog"
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
	port := config.GetConfig().Server.Port

	go s.worker.Start()
	defer s.worker.Stop()

	s.running = true
	logger.Infof("start server at port %s", port)
	err = rpc.StartTcpServer(port, port, s.server)
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
