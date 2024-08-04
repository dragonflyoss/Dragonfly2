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

//go:generate mockgen -destination mocks/manager_tasks_mock.go -source manager_tasks.go -package mocks

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

// ManagerTask is an interface for delete and list tasks.
type ManagerTasks interface {
	// CreateDeleteTask create a delete task job
	CreateDeleteTask(context.Context, []models.Scheduler, types.DeleteTasksArgs) (*internaljob.GroupJobState, error)
	// CreateListTasks create a list tasks job
	CreateListTasks(context.Context, []models.Scheduler, types.ListTasksArgs) (*internaljob.GroupJobState, error)
}

// managerTasks is an implementation of ManagerTasks.
type managerTasks struct {
	job             *internaljob.Job
	registryTimeout time.Duration
}

// newManagerTasks create a new ManagerTasks.
func newManagerTasks(job *internaljob.Job, registryTimeout time.Duration) ManagerTasks {
	return &managerTasks{
		job:             job,
		registryTimeout: registryTimeout,
	}
}

// Create a delete task job.
func (m *managerTasks) CreateDeleteTask(ctx context.Context, schedulers []models.Scheduler, json types.DeleteTasksArgs) (*internaljob.GroupJobState, error) {
	var span trace.Span
	ctx, span = tracer.Start(ctx, config.SpanDeleteTask, trace.WithSpanKind(trace.SpanKindProducer))
	span.SetAttributes(config.AttributeDeleteTaskID.String(json.TaskID))
	defer span.End()

	args, err := internaljob.MarshalRequest(json)
	if err != nil {
		logger.Errorf("delete task marshal request: %v, error: %v", args, err)
		return nil, err
	}

	// Initialize queues.
	queues := getSchedulerQueues(schedulers)
	return m.createGroupJob(ctx, internaljob.DeleteTaskJob, args, queues)
}

// Create a list tasks job.
func (m *managerTasks) CreateListTasks(ctx context.Context, schedulers []models.Scheduler, json types.ListTasksArgs) (*internaljob.GroupJobState, error) {
	var span trace.Span
	ctx, span = tracer.Start(ctx, config.SpanListTasks, trace.WithSpanKind(trace.SpanKindProducer))
	span.SetAttributes(config.AttributeListTasksID.String(json.TaskID))
	span.SetAttributes(config.AttributeListTasksPage.Int(json.Page))
	span.SetAttributes(config.AttributeListTasksPerPage.Int(json.PerPage))
	defer span.End()

	args, err := internaljob.MarshalRequest(json)
	if err != nil {
		logger.Errorf("list tasks marshal request: %v, error: %v", args, err)
		return nil, err
	}

	// Initialize queues.
	queues := getSchedulerQueues(schedulers)
	return m.createGroupJob(ctx, internaljob.ListTasksJob, args, queues)
}

// createGroupJob creates a group job.
func (m *managerTasks) createGroupJob(ctx context.Context, name string, args []machineryv1tasks.Arg, queues []internaljob.Queue) (*internaljob.GroupJobState, error) {
	var signatures []*machineryv1tasks.Signature
	for _, queue := range queues {
		signatures = append(signatures, &machineryv1tasks.Signature{
			UUID:       fmt.Sprintf("task_%s", uuid.New().String()),
			Name:       name,
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

	logger.Infof("create manager tasks group %s in queues %v, tasks: %#v", group.GroupUUID, queues, tasks)
	if _, err := m.job.Server.SendGroupWithContext(ctx, group, 0); err != nil {
		logger.Errorf("create manager tasks group %s failed", group.GroupUUID, err)
		return nil, err
	}

	return &internaljob.GroupJobState{
		GroupUUID: group.GroupUUID,
		State:     machineryv1tasks.StatePending,
		CreatedAt: time.Now(),
	}, nil
}
