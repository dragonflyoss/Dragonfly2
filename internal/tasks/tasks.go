/*
 *     Copyright 2020 The Dragonfly Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package tasks

import (
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	machineryv1tasks "github.com/RichardKnop/machinery/v1/tasks"
	"github.com/pkg/errors"

	"github.com/RichardKnop/machinery/v1"
	machineryv1config "github.com/RichardKnop/machinery/v1/config"
)

const (
	DefaultResultsExpireIn = 86400
)

type Config struct {
	Host      string
	Port      int
	Password  string
	BrokerDB  int
	BackendDB int
}

type Tasks struct {
	Server *machinery.Server
	Worker *machinery.Worker
	Queue  Queue
}

func New(cfg *Config, queue Queue) (*Tasks, error) {
	broker := fmt.Sprintf("redis://%s@%s:%d/%d", cfg.Password, cfg.Host, cfg.Port, cfg.BrokerDB)
	backend := fmt.Sprintf("redis://%s@%s:%d/%d", cfg.Password, cfg.Host, cfg.Port, cfg.BackendDB)

	var cnf = &machineryv1config.Config{
		Broker:          broker,
		DefaultQueue:    queue.String(),
		ResultBackend:   backend,
		ResultsExpireIn: DefaultResultsExpireIn,
	}

	server, err := machinery.NewServer(cnf)
	if err != nil {
		return nil, err
	}

	return &Tasks{
		Server: server,
		Queue:  queue,
	}, nil
}

func (t *Tasks) RegisterTasks(namedTaskFuncs map[string]interface{}) error {
	return t.Server.RegisterTasks(namedTaskFuncs)
}

func (t *Tasks) LaunchWorker(consumerTag string, concurrency int) error {
	t.Worker = t.Server.NewWorker(consumerTag, concurrency)
	return t.Worker.Launch()
}

type GroupTaskState struct {
	GroupUUID string
	State     string
	CreatedAt time.Time
}

func (t *Tasks) GetGroupTaskState(groupUUID string) (*GroupTaskState, error) {
	taskStates, err := t.Server.GetBackend().GroupTaskStates(groupUUID, 0)
	if err != nil {
		return nil, err
	}

	if len(taskStates) == 0 {
		return nil, errors.New("empty group task")
	}

	for _, taskState := range taskStates {
		if taskState.IsFailure() {
			return &GroupTaskState{
				GroupUUID: groupUUID,
				State:     machineryv1tasks.StateFailure,
				CreatedAt: taskState.CreatedAt,
			}, nil
		}
	}

	for _, taskState := range taskStates {
		if !taskState.IsSuccess() {
			return &GroupTaskState{
				GroupUUID: groupUUID,
				State:     machineryv1tasks.StatePending,
				CreatedAt: taskState.CreatedAt,
			}, nil
		}
	}

	return &GroupTaskState{
		GroupUUID: groupUUID,
		State:     machineryv1tasks.StateSuccess,
		CreatedAt: taskStates[0].CreatedAt,
	}, nil
}

func MarshalRequest(v interface{}) ([]machineryv1tasks.Arg, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}

	return []machineryv1tasks.Arg{{
		Type:  "string",
		Value: string(b),
	}}, nil
}

func UnmarshalResponse(data []reflect.Value, v interface{}) error {
	if len(data) == 0 {
		return errors.New("empty data is not specified")
	}

	if err := json.Unmarshal([]byte(data[0].String()), v); err != nil {
		return err
	}
	return nil
}

func UnmarshalRequest(data string, v interface{}) error {
	return json.Unmarshal([]byte(data), v)
}
