package tasks

import (
	internaltasks "d7y.io/dragonfly/v2/internal/tasks"
	"d7y.io/dragonfly/v2/manager/config"
	"d7y.io/dragonfly/v2/manager/types"
)

type Task interface {
	Preheat(types.CreatePreheatRequest) (string, error)
}

type task struct {
	*internaltasks.Tasks
}

func New(cfg *config.RedisConfig) (Task, error) {
	t, err := internaltasks.New(&internaltasks.Config{
		Host:      cfg.Host,
		Port:      cfg.Port,
		Password:  cfg.Password,
		BrokerDB:  cfg.BrokerDB,
		BackendDB: cfg.BackendDB,
	}, internaltasks.GlobalQueue)
	if err != nil {
		return nil, err
	}

	return &task{t}, nil
}

func Preheat(json types.CreatePreheatRequest) (string, error) {
	return "", nil
}
