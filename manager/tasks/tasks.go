package tasks

import (
	internaltasks "d7y.io/dragonfly/v2/internal/tasks"
	"d7y.io/dragonfly/v2/manager/config"
	"d7y.io/dragonfly/v2/manager/types"
)

type Task interface {
	CreatePreheat([]string, types.CreatePreheatRequest) (*types.Preheat, error)
	GetPreheat(string) (*types.Preheat, error)
}

type task struct {
	*internaltasks.Tasks
	Preheat Preheat
}

func New(cfg *config.Config) (Task, error) {
	t, err := internaltasks.New(&internaltasks.Config{
		Host:      cfg.Database.Redis.Host,
		Port:      cfg.Database.Redis.Port,
		Password:  cfg.Database.Redis.Password,
		BrokerDB:  cfg.Database.Redis.BrokerDB,
		BackendDB: cfg.Database.Redis.BackendDB,
	}, internaltasks.GlobalQueue)
	if err != nil {
		return nil, err
	}

	preheat, err := newPreheat(t, cfg.Server.Name)
	if err != nil {
		return nil, err
	}

	return &task{
		Tasks:   t,
		Preheat: preheat,
	}, nil
}

func (t *task) CreatePreheat(hostnames []string, json types.CreatePreheatRequest) (*types.Preheat, error) {
	return t.Preheat.CreatePreheat(hostnames, json)
}

func (t *task) GetPreheat(id string) (*types.Preheat, error) {
	groupTaskState, err := t.GetGroupTaskState(id)
	if err != nil {
		return nil, err
	}

	return &types.Preheat{
		ID:        groupTaskState.GroupUUID,
		Status:    groupTaskState.State,
		CreatedAt: groupTaskState.CreatedAt,
	}, nil
}
