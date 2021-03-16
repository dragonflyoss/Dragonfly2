package handler

import "d7y.io/dragonfly/v2/manager/server/service"

type Handler struct {
	server *service.ManagerServer
}

func NewHandler(server *service.ManagerServer) *Handler {
	return &Handler{server: server}
}
