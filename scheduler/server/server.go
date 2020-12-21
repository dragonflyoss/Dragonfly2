package server

import (
	"fmt"
	"github.com/dragonflyoss/Dragonfly2/pkg/grpc/scheduler"
	"github.com/dragonflyoss/Dragonfly2/scheduler/config"
	"github.com/dragonflyoss/Dragonfly2/scheduler/service"
	"github.com/dragonflyoss/Dragonfly2/scheduler/service/schedule_worker"
	"google.golang.org/grpc"
	"net"
)

type Server struct {
	grpc *grpc.Server
	scheduler *service.SchedulerService
	worker schedule_worker.IWorker
}

func NewServer() *Server {
 	s := &Server{
 		grpc: grpc.NewServer(),
 		scheduler: service.CreateSchedulerService(),
	}
	s.worker = schedule_worker.CreateWorkerGroup(s.scheduler.GetScheduler())
	scheduler.RegisterSchedulerServer(s.grpc, &SchedulerServer{svc:s.scheduler, worker: s.worker})
 	return s
}

func (s *Server) Start() (err error) {
	addr := config.GetConfig().Server.Addr
	port := config.GetConfig().Server.Port
	socket, err := net.Listen("tcp", fmt.Sprintf("%s:%d", addr, port))
	if err != nil {
		return
	}
	go s.scheduler.Start()
	go s.worker.Start()
	err = s.grpc.Serve(socket)
	return
}
