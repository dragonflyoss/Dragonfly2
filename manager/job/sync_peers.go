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
	"gorm.io/gorm"

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
	config *config.Config
	job    *internaljob.Job
	db     *gorm.DB
	done   chan struct{}
}

// newSyncPeers returns a new SyncPeers.
func newSyncPeers(cfg *config.Config, job *internaljob.Job, gdb *gorm.DB) (SyncPeers, error) {
	return &syncPeers{
		config: cfg,
		db:     gdb,
		job:    job,
		done:   make(chan struct{}),
	}, nil
}

// TODO Implement function.
// Started sync peers server.
func (s *syncPeers) Serve() {
	tick := time.NewTicker(s.config.Job.SyncPeers.Interval)
	for {
		select {
		case <-tick.C:
			// Find all of the scheduler clusters that has active schedulers.
			var candidateSchedulerClusters []models.SchedulerCluster
			if err := s.db.WithContext(context.Background()).Find(&candidateSchedulerClusters).Error; err != nil {
				logger.Errorf("find candidate scheduler clusters failed: %v", err)
				break
			}

			var candidateSchedulers []models.Scheduler
			for _, candidateSchedulerCluster := range candidateSchedulerClusters {
				var schedulers []models.Scheduler
				if err := s.db.WithContext(context.Background()).Preload("SchedulerCluster").Find(&schedulers, models.Scheduler{
					SchedulerClusterID: candidateSchedulerCluster.ID,
					State:              models.SchedulerStateActive,
				}).Error; err != nil {
					continue
				}

				candidateSchedulers = append(candidateSchedulers, schedulers...)
			}

			for _, scheduler := range candidateSchedulers {
				if _, err := s.createSyncPeers(context.Background(), scheduler); err != nil {
					logger.Error(err)
				}
			}
		case <-s.done:
			return
		}
	}
}

// Stop sync peers server.
func (s *syncPeers) Stop() {
	close(s.done)
}

// createSyncPeers creates sync peers.
func (s *syncPeers) createSyncPeers(ctx context.Context, scheduler models.Scheduler) (any, error) {
	var span trace.Span
	ctx, span = tracer.Start(ctx, config.SpanSyncPeers, trace.WithSpanKind(trace.SpanKindProducer))
	defer span.End()

	// Initialize queue.
	queue, err := getSchedulerQueue(scheduler)
	if err != nil {
		return nil, err
	}

	// Initialize task signature.
	task := &machineryv1tasks.Signature{
		UUID:       fmt.Sprintf("task_%s", uuid.New().String()),
		Name:       internaljob.SyncPeersJob,
		RoutingKey: queue.String(),
	}

	logger.Infof("create sync peers in queue %v, task: %#v", queue, task)
	asyncResult, err := s.job.Server.SendTaskWithContext(ctx, task)
	if err != nil {
		logger.Errorf("create sync peers in queue %v failed", queue, err)
		return nil, err
	}

	return asyncResult.GetWithTimeout(s.config.Job.SyncPeers.Timeout, DefaultTaskPollingInterval)
}
