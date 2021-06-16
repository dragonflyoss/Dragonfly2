package handlers

import "d7y.io/dragonfly/v2/manager/server/service"

type Handlers struct {
	server *service.ManagerServer
}

func NewHandler(server *service.ManagerServer) *Handlers {
	return &Handlers{server: server}
}
