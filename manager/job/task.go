/*
 *     Copyright 2024 The Dragonfly Authors
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

//go:generate mockgen -destination mocks/task_mock.go -source task.go -package mocks

package job

import (
	"context"
	"fmt"
	"time"

	machineryv1tasks "github.com/RichardKnop/machinery/v1/tasks"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel/trace"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	internaljob "d7y.io/dragonfly/v2/internal/job"
	"d7y.io/dragonfly/v2/manager/config"
	"d7y.io/dragonfly/v2/manager/models"
	"d7y.io/dragonfly/v2/manager/types"
	"d7y.io/dragonfly/v2/pkg/idgen"
)

// Task is an interface for manager tasks.
type Task interface {
	// CreateGetTask create a get task job.
	CreateGetTask(context.Context, []models.Scheduler, types.GetTaskArgs) (*internaljob.GroupJobState, error)

	// CreateDeleteTask create a delete task job.
	CreateDeleteTask(context.Context, []models.Scheduler, types.DeleteTaskArgs) (*internaljob.GroupJobState, error)
}

// task is an implementation of Task.
type task struct {
	job *internaljob.Job
}

// newTask returns a new Task.
func newTask(job *internaljob.Job) Task {
	return &task{job}
}

// CreateGetTask create a get task job.
func (t *task) CreateGetTask(ctx context.Context, schedulers []models.Scheduler, json types.GetTaskArgs) (*internaljob.GroupJobState, error) {
	var span trace.Span
	ctx, span = tracer.Start(ctx, config.SpanGetTask, trace.WithSpanKind(trace.SpanKindProducer))
	span.SetAttributes(config.AttributeGetTaskID.String(json.TaskID))
	defer span.End()

	taskID := json.TaskID
	if json.URL != "" {
		taskID = idgen.TaskIDV2(json.URL, json.Tag, json.Application, idgen.ParseFilteredQueryParams(json.FilteredQueryParams))
	}

	args, err := internaljob.MarshalRequest(internaljob.GetTaskRequest{
		TaskID: taskID,
	})
	if err != nil {
		logger.Errorf("get tasks marshal request: %v, error: %v", args, err)
		return nil, err
	}

	queues, err := getSchedulerQueues(schedulers)
	if err != nil {
		return nil, err
	}

	var signatures []*machineryv1tasks.Signature
	for _, queue := range queues {
		signatures = append(signatures, &machineryv1tasks.Signature{
			UUID:       fmt.Sprintf("task_%s", uuid.New().String()),
			Name:       internaljob.GetTaskJob,
			RoutingKey: queue.String(),
			Args:       args,
		})
	}

	group, err := machineryv1tasks.NewGroup(signatures...)
	if err != nil {
		return nil, err
	}

	var tasks []machineryv1tasks.Signature
	for _, signature := range signatures {
		tasks = append(tasks, *signature)
	}

	logger.Infof("create task group %s in queues %v, tasks: %#v", group.GroupUUID, queues, tasks)
	if _, err := t.job.Server.SendGroupWithContext(ctx, group, 0); err != nil {
		logger.Errorf("create task group %s failed", group.GroupUUID, err)
		return nil, err
	}

	return &internaljob.GroupJobState{
		GroupUUID: group.GroupUUID,
		State:     machineryv1tasks.StatePending,
		CreatedAt: time.Now(),
	}, nil
}

// CreateDeleteTask create a delete task job.
func (t *task) CreateDeleteTask(ctx context.Context, schedulers []models.Scheduler, json types.DeleteTaskArgs) (*internaljob.GroupJobState, error) {
	var span trace.Span
	ctx, span = tracer.Start(ctx, config.SpanDeleteTask, trace.WithSpanKind(trace.SpanKindProducer))
	span.SetAttributes(config.AttributeDeleteTaskID.String(json.TaskID))
	defer span.End()

	taskID := json.TaskID
	if json.URL != "" {
		taskID = idgen.TaskIDV2(json.URL, json.Tag, json.Application, idgen.ParseFilteredQueryParams(json.FilteredQueryParams))
	}

	args, err := internaljob.MarshalRequest(internaljob.DeleteTaskRequest{
		TaskID:  taskID,
		Timeout: json.Timeout,
	})
	if err != nil {
		logger.Errorf("delete task marshal request: %v, error: %v", args, err)
		return nil, err
	}

	queues, err := getSchedulerQueues(schedulers)
	if err != nil {
		return nil, err
	}

	var signatures []*machineryv1tasks.Signature
	for _, queue := range queues {
		signatures = append(signatures, &machineryv1tasks.Signature{
			UUID:       fmt.Sprintf("task_%s", uuid.New().String()),
			Name:       internaljob.DeleteTaskJob,
			RoutingKey: queue.String(),
			Args:       args,
		})
	}

	group, err := machineryv1tasks.NewGroup(signatures...)
	if err != nil {
		return nil, err
	}

	var tasks []machineryv1tasks.Signature
	for _, signature := range signatures {
		tasks = append(tasks, *signature)
	}

	logger.Infof("create task group %s in queues %v, tasks: %#v", group.GroupUUID, queues, tasks)
	if _, err := t.job.Server.SendGroupWithContext(ctx, group, 0); err != nil {
		logger.Errorf("create preheat group %s failed", group.GroupUUID, err)
		return nil, err
	}

	return &internaljob.GroupJobState{
		GroupUUID: group.GroupUUID,
		State:     machineryv1tasks.StatePending,
		CreatedAt: time.Now(),
	}, nil
}
