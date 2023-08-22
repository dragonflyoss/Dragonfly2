/*
 *     Copyright 2023 The Dragonfly Authors
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

//go:generate mockgen -destination mocks/sync_peers_mock.go -source sync_peers.go -package mocks

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
)

// SyncPeers is an interface for sync peers.
type SyncPeers interface {
	// Started sync peers server.
	Serve()

	// Stop sync peers server.
	Stop()
}

// syncPeers is an implementation of SyncPeers.
type syncPeers struct {
	job *internaljob.Job
}

// newSyncPeers returns a new SyncPeers.
func newSyncPeers(job *internaljob.Job) (SyncPeers, error) {
	return &syncPeers{job}, nil
}

// TODO Implement function.
// Started sync peers server.
func (s *syncPeers) Serve() {
}

// TODO Implement function.
// Stop sync peers server.
func (s *syncPeers) Stop() {
}

// createSyncPeers creates sync peers.
func (s *syncPeers) createSyncPeers(ctx context.Context, schedulers []models.Scheduler) (*internaljob.GroupJobState, error) {
	var span trace.Span
	ctx, span = tracer.Start(ctx, config.SpanSyncPeers, trace.WithSpanKind(trace.SpanKindProducer))
	defer span.End()

	// Initialize queues
	queues := getSchedulerQueues(schedulers)

	var signatures []*machineryv1tasks.Signature
	for _, queue := range queues {
		signatures = append(signatures, &machineryv1tasks.Signature{
			UUID:       fmt.Sprintf("task_%s", uuid.New().String()),
			Name:       internaljob.SyncPeersJob,
			RoutingKey: queue.String(),
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

	logger.Infof("create sync peers group %s in queues %v, tasks: %#v", group.GroupUUID, queues, tasks)
	if _, err := s.job.Server.SendGroupWithContext(ctx, group, 0); err != nil {
		logger.Errorf("create sync peers group %s failed", group.GroupUUID, err)
		return nil, err
	}

	return &internaljob.GroupJobState{
		GroupUUID: group.GroupUUID,
		State:     machineryv1tasks.StatePending,
		CreatedAt: time.Now(),
	}, nil
}
