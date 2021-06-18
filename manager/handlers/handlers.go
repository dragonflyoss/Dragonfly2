package handlers

import "d7y.io/dragonfly/v2/manager/service"

type Handlers struct {
	service service.Service
}

func NewHandler(service service.Service) *Handlers {
	return &Handlers{service: service}
}
