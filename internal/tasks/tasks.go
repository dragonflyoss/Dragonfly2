package tasks

import (
	"encoding/json"
	"fmt"
	"reflect"

	machineryv1tasks "github.com/RichardKnop/machinery/v1/tasks"
	"github.com/pkg/errors"

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

func Marshal(v interface{}) ([]machineryv1tasks.Arg, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}

	return []machineryv1tasks.Arg{{
		Type:  "string",
		Value: string(b),
	}}, nil
}

func Unmarshal(data []reflect.Value, v interface{}) error {
	if len(data) == 0 {
		return errors.New("empty data is not specified")
	}

	if err := json.Unmarshal([]byte(data[0].String()), v); err != nil {
		return err
	}
	return nil
}
