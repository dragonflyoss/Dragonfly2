package tasks

import (
	"encoding/json"
	"fmt"
	"github.com/RichardKnop/machinery/v1/backends/result"
	machineryv1tasks "github.com/RichardKnop/machinery/v1/tasks"

	"github.com/RichardKnop/machinery/v1"
	machineryv1config "github.com/RichardKnop/machinery/v1/config"
)

type Config struct {
	Host      string
	Port      int
	Password  string
	BrokerDB  int
	BackendDB int
}

const (
	PreheatTask = "preheat"
)

var tasks = map[string]interface{}{}

type Tasks struct {
	Server *machinery.Server
	Queue  Queue
}

func New(cfg *Config, queue Queue) (*Tasks, error) {
	broker := fmt.Sprintf("redis://%s@%s:%d/%d", cfg.Password, cfg.Host, cfg.Port, cfg.BrokerDB)
	backend := fmt.Sprintf("redis://%s@%s:%d/%d", cfg.Password, cfg.Host, cfg.Port, cfg.BackendDB)

	var cnf = &machineryv1config.Config{
		Broker:        broker,
		DefaultQueue:  queue.String(),
		ResultBackend: backend,
	}

	server, err := machinery.NewServer(cnf)
	if err != nil {
		return nil, err
	}

	if err := server.RegisterTasks(tasks); err != nil {
		return nil, err
	}

	return &Tasks{
		Server: server,
		Queue:  queue,
	}, nil
}

func (t *Tasks) LaunchWorker(consumerTag string, concurrency int) error {
	return t.Server.NewWorker(consumerTag, concurrency).Launch()
}

func (t *Tasks) SendTask(taskName string, taskArg interface{}) (*result.AsyncResult, error) {
	argJson, err := json.Marshal(taskArg)
	if err != nil {
		return nil, err
	}
	signature := &machineryv1tasks.Signature{
		Name: taskName,
		Args: []machineryv1tasks.Arg{
			{
				Type:  "string",
				Value: string(argJson),
			},
		},
	}
	return t.Server.SendTask(signature)
}