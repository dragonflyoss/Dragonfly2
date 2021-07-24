package service

import (
	"d7y.io/dragonfly/v2/manager/types"
)

func (s *rest) CreatePreheat(json types.CreatePreheatRequest) (*types.Preheat, error) {
	return s.tasks.CreatePreheat(json)
}

func (s *rest) GetPreheat(id string) (*types.Preheat, error) {
	return s.tasks.GetPreheat(id)
}
