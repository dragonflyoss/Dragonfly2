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
)

// DefaultDeleteTaskTimeout is the default timeout for delete task.
const DefaultDeleteTaskTimeout = 120 * time.Second

// Task is an interface for manager tasks.
type Task interface {
	// DeleteTask delete task
	DeleteTask(context.Context, []models.Scheduler, types.DeleteTaskArgs) (*machineryv1tasks.Group, map[string]*internaljob.DeleteTaskResponse, error)

	// GetTask get task
	GetTask(context.Context, []models.Scheduler, types.GetTaskArgs) (*machineryv1tasks.Group, map[string]*internaljob.GetTaskResponse, error)
}

// task is an implementation of Task.
type task struct {
	job *internaljob.Job
}

// newTask returns a new Task.
func newTask(job *internaljob.Job) Task {
	return &task{job}
}

// DeleteTask delete task
func (t *task) DeleteTask(ctx context.Context, schedulers []models.Scheduler, json types.DeleteTaskArgs) (*machineryv1tasks.Group, map[string]*internaljob.DeleteTaskResponse, error) {
	var span trace.Span
	ctx, span = tracer.Start(ctx, config.SpanDeleteTask, trace.WithSpanKind(trace.SpanKindProducer))
	span.SetAttributes(config.AttributeDeleteTaskID.String(json.TaskID))
	defer span.End()

	args, err := internaljob.MarshalRequest(json)
	if err != nil {
		logger.Errorf("delete task marshal request: %v, error: %v", args, err)
		return nil, nil, err
	}

	// Initialize queues.
	queues, err := getSchedulerQueues(schedulers)
	if err != nil {
		return nil, nil, err
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
		return nil, nil, err
	}

	var tasks []machineryv1tasks.Signature
	for _, signature := range signatures {
		tasks = append(tasks, *signature)
	}

	logger.Infof("create task group %s in queues %v, tasks: %#v", group.GroupUUID, queues, tasks)
	asyncResults, err := t.job.Server.SendGroupWithContext(ctx, group, 0)
	if err != nil {
		logger.Errorf("create task group %s failed", group.GroupUUID, err)
		return group, nil, err
	}

	deleteTaskResponses := make(map[string]*internaljob.DeleteTaskResponse, len(schedulers))
	for _, asyncResult := range asyncResults {
		results, err := asyncResult.GetWithTimeout(DefaultDeleteTaskTimeout, DefaultTaskPollingInterval)
		if err != nil {
			return group, nil, err
		}

		// Unmarshal task group result.
		var deleteTaskResponse internaljob.DeleteTaskResponse
		if err := internaljob.UnmarshalResponse(results, &deleteTaskResponse); err != nil {
			logger.Errorf("unmarshal task group result failed: %v", err)
			deleteTaskResponses[asyncResult.Signature.RoutingKey] = nil
			continue
		}

		// Store task group result by routing key.
		deleteTaskResponses[asyncResult.Signature.RoutingKey] = &deleteTaskResponse
	}

	return group, deleteTaskResponses, nil
}

// GetTask get task
func (t *task) GetTask(ctx context.Context, schedulers []models.Scheduler, json types.GetTaskArgs) (*machineryv1tasks.Group, map[string]*internaljob.GetTaskResponse, error) {
	var span trace.Span
	ctx, span = tracer.Start(ctx, config.SpanGetTask, trace.WithSpanKind(trace.SpanKindProducer))
	span.SetAttributes(config.AttributeGetTaskID.String(json.TaskID))
	defer span.End()

	args, err := internaljob.MarshalRequest(json)
	if err != nil {
		logger.Errorf("list tasks marshal request: %v, error: %v", args, err)
		return nil, nil, err
	}

	// Initialize queues.
	queues, err := getSchedulerQueues(schedulers)
	if err != nil {
		return nil, nil, err
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
		return nil, nil, err
	}

	var tasks []machineryv1tasks.Signature
	for _, signature := range signatures {
		tasks = append(tasks, *signature)
	}

	logger.Infof("create task group %s in queues %v, tasks: %#v", group.GroupUUID, queues, tasks)
	asyncResults, err := t.job.Server.SendGroupWithContext(ctx, group, 0)
	if err != nil {
		logger.Errorf("create task group %s failed", group.GroupUUID, err)
		return group, nil, err
	}

	getTaskResponses := make(map[string]*internaljob.GetTaskResponse, len(schedulers))
	for _, asyncResult := range asyncResults {
		results, err := asyncResult.GetWithTimeout(DefaultDeleteTaskTimeout, DefaultTaskPollingInterval)
		if err != nil {
			return group, nil, err
		}

		// Unmarshal task group result.
		var getTaskResponse internaljob.GetTaskResponse
		if err := internaljob.UnmarshalResponse(results, &getTaskResponse); err != nil {
			logger.Errorf("unmarshal task group result failed: %v", err)
			getTaskResponses[asyncResult.Signature.RoutingKey] = nil
			continue
		}

		// Store task group result by routing key.
		getTaskResponses[asyncResult.Signature.RoutingKey] = &getTaskResponse
	}

	return group, getTaskResponses, nil
}
