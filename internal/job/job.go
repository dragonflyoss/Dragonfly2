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

package job

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/RichardKnop/machinery/v1"
	machineryv1config "github.com/RichardKnop/machinery/v1/config"
	machineryv1log "github.com/RichardKnop/machinery/v1/log"
	machineryv1tasks "github.com/RichardKnop/machinery/v1/tasks"
	"github.com/go-redis/redis/v8"
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

type Job struct {
	Server *machinery.Server
	Worker *machinery.Worker
	Queue  Queue
}

func New(cfg *Config, queue Queue) (*Job, error) {
	// Set logger
	machineryv1log.Set(&MachineryLogger{})

	broker := fmt.Sprintf("redis://%s@%s:%d/%d", cfg.Password, cfg.Host, cfg.Port, cfg.BrokerDB)
	if err := ping(&redis.Options{
		Addr:     fmt.Sprintf("%s:%d", cfg.Host, cfg.Port),
		Password: cfg.Password,
		DB:       cfg.BrokerDB,
	}); err != nil {
		return nil, err
	}

	backend := fmt.Sprintf("redis://%s@%s:%d/%d", cfg.Password, cfg.Host, cfg.Port, cfg.BackendDB)
	if err := ping(&redis.Options{
		Addr:     fmt.Sprintf("%s:%d", cfg.Host, cfg.Port),
		Password: cfg.Password,
		DB:       cfg.BackendDB,
	}); err != nil {
		return nil, err
	}

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

	return &Job{
		Server: server,
		Queue:  queue,
	}, nil
}

func ping(options *redis.Options) error {
	client := redis.NewClient(options)
	return client.Ping(context.Background()).Err()
}

func (t *Job) RegisterJob(namedJobFuncs map[string]interface{}) error {
	return t.Server.RegisterTasks(namedJobFuncs)
}

func (t *Job) LaunchWorker(consumerTag string, concurrency int) error {
	t.Worker = t.Server.NewWorker(consumerTag, concurrency)
	return t.Worker.Launch()
}

type GroupJobState struct {
	GroupUUID string
	State     string
	CreatedAt time.Time
}

func (t *Job) GetGroupJobState(groupUUID string) (*GroupJobState, error) {
	jobStates, err := t.Server.GetBackend().GroupTaskStates(groupUUID, 0)
	if err != nil {
		return nil, err
	}

	if len(jobStates) == 0 {
		return nil, errors.New("empty group job")
	}

	for _, jobState := range jobStates {
		if jobState.IsFailure() {
			return &GroupJobState{
				GroupUUID: groupUUID,
				State:     machineryv1tasks.StateFailure,
				CreatedAt: jobState.CreatedAt,
			}, nil
		}
	}

	for _, jobState := range jobStates {
		if !jobState.IsSuccess() {
			return &GroupJobState{
				GroupUUID: groupUUID,
				State:     machineryv1tasks.StatePending,
				CreatedAt: jobState.CreatedAt,
			}, nil
		}
	}

	return &GroupJobState{
		GroupUUID: groupUUID,
		State:     machineryv1tasks.StateSuccess,
		CreatedAt: jobStates[0].CreatedAt,
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
