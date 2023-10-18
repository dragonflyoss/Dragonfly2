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
	"d7y.io/dragonfly/v2/pkg/idgen"
	"d7y.io/dragonfly/v2/scheduler/resource"
)

// SyncPeers is an interface for sync peers.
type SyncPeers interface {
	// Run sync peers.
	Run(context.Context) error

	// Serve started sync peers server.
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

// Run sync peers.
func (s *syncPeers) Run(ctx context.Context) error {
	// Find all of the scheduler clusters that has active schedulers.
	var candidateSchedulerClusters []models.SchedulerCluster
	if err := s.db.WithContext(ctx).Find(&candidateSchedulerClusters).Error; err != nil {
		return err
	}

	// Find all of the schedulers that has active scheduler cluster.
	var candidateSchedulers []models.Scheduler
	for _, candidateSchedulerCluster := range candidateSchedulerClusters {
		var scheduler models.Scheduler
		if err := s.db.WithContext(ctx).Preload("SchedulerCluster").First(&scheduler, models.Scheduler{
			SchedulerClusterID: candidateSchedulerCluster.ID,
			State:              models.SchedulerStateActive,
		}).Error; err != nil {
			continue
		}

		logger.Infof("sync peers find candidate scheduler cluster %s", candidateSchedulerCluster.Name)
		candidateSchedulers = append(candidateSchedulers, scheduler)
	}
	logger.Infof("sync peers find candidate schedulers count is %d", len(candidateSchedulers))

	// Send sync peer requests to all available schedulers,
	// and merge the sync peer results with the data in
	// the peer table in the database.
	for _, scheduler := range candidateSchedulers {
		log := logger.WithScheduler(scheduler.Hostname, scheduler.IP, uint64(scheduler.SchedulerClusterID))

		// Send sync peer request to scheduler.
		results, err := s.createSyncPeers(ctx, scheduler)
		if err != nil {
			log.Error(err)
			continue
		}
		log.Infof("sync peers count is %d", len(results))

		// Merge sync peer results with the data in the peer table.
		s.mergePeers(ctx, scheduler, results, log)
	}
	return nil
}

// Serve started sync peers server.
func (s *syncPeers) Serve() {
	tick := time.NewTicker(s.config.Job.SyncPeers.Interval)
	for {
		select {
		case <-tick.C:
			if err := s.Run(context.Background()); err != nil {
				logger.Errorf("sync peers failed: %v", err)
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
func (s *syncPeers) createSyncPeers(ctx context.Context, scheduler models.Scheduler) ([]*resource.Host, error) {
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

	// Send sync peer task to worker.
	logger.Infof("create sync peers in queue %v, task: %#v", queue, task)
	asyncResult, err := s.job.Server.SendTaskWithContext(ctx, task)
	if err != nil {
		logger.Errorf("create sync peers in queue %v failed", queue, err)
		return nil, err
	}

	// Get sync peer task result.
	results, err := asyncResult.GetWithTimeout(s.config.Job.SyncPeers.Timeout, DefaultTaskPollingInterval)
	if err != nil {
		return nil, err
	}

	// Unmarshal sync peer task result.
	var hosts []*resource.Host
	if err := internaljob.UnmarshalResponse(results, &hosts); err != nil {
		return nil, err
	}

	return hosts, nil
}

// Merge sync peer results with the data in the peer table.
func (s *syncPeers) mergePeers(ctx context.Context, scheduler models.Scheduler, results []*resource.Host, log *logger.SugaredLoggerOnWith) {
	// Convert sync peer results from slice to map.
	syncPeers := make(map[string]*resource.Host)
	for _, result := range results {
		syncPeers[result.ID] = result
	}

	rows, err := s.db.Model(&models.Peer{}).Find(&models.Peer{SchedulerClusterID: scheduler.SchedulerClusterID}).Rows()
	if err != nil {
		log.Error(err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		peer := models.Peer{}
		if err := s.db.ScanRows(rows, &peer); err != nil {
			log.Error(err)
			continue
		}

		// If the peer exists in the sync peer results, update the peer data in the database with
		// the sync peer results and delete the sync peer from the sync peers map.
		id := idgen.HostIDV2(peer.IP, peer.Hostname)
		if syncPeer, ok := syncPeers[id]; ok {
			if err := s.db.WithContext(ctx).First(&models.Peer{}, peer.ID).Updates(models.Peer{
				Type:              syncPeer.Type.Name(),
				IDC:               syncPeer.Network.IDC,
				Location:          syncPeer.Network.Location,
				Port:              syncPeer.Port,
				DownloadPort:      syncPeer.DownloadPort,
				ObjectStoragePort: syncPeer.ObjectStoragePort,
				State:             models.PeerStateActive,
				OS:                syncPeer.OS,
				Platform:          syncPeer.Platform,
				PlatformFamily:    syncPeer.PlatformFamily,
				PlatformVersion:   syncPeer.PlatformVersion,
				KernelVersion:     syncPeer.KernelVersion,
				GitVersion:        syncPeer.Build.GitVersion,
				GitCommit:         syncPeer.Build.GitCommit,
				BuildPlatform:     syncPeer.Build.Platform,
			}).Error; err != nil {
				log.Error(err)
			}

			// Delete the sync peer from the sync peers map.
			delete(syncPeers, id)
		} else {
			// If the peer does not exist in the sync peer results, delete the peer in the database.
			if err := s.db.WithContext(ctx).Unscoped().Delete(&models.Peer{}, peer.ID).Error; err != nil {
				log.Error(err)
			}
		}
	}

	// Insert the sync peers that do not exist in the database into the peer table.
	for _, syncPeer := range syncPeers {
		if err := s.db.WithContext(ctx).Create(&models.Peer{
			Hostname:           syncPeer.Hostname,
			Type:               syncPeer.Type.Name(),
			IDC:                syncPeer.Network.IDC,
			Location:           syncPeer.Network.Location,
			IP:                 syncPeer.IP,
			Port:               syncPeer.Port,
			DownloadPort:       syncPeer.DownloadPort,
			ObjectStoragePort:  syncPeer.ObjectStoragePort,
			State:              models.PeerStateActive,
			OS:                 syncPeer.OS,
			Platform:           syncPeer.Platform,
			PlatformFamily:     syncPeer.PlatformFamily,
			PlatformVersion:    syncPeer.PlatformVersion,
			KernelVersion:      syncPeer.KernelVersion,
			GitVersion:         syncPeer.Build.GitVersion,
			GitCommit:          syncPeer.Build.GitCommit,
			BuildPlatform:      syncPeer.Build.Platform,
			SchedulerClusterID: uint(syncPeer.SchedulerClusterID),
		}).Error; err != nil {
			log.Error(err)
		}
	}
}
