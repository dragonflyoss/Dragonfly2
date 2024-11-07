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
	"sync"
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
	"d7y.io/dragonfly/v2/pkg/types"
	resource "d7y.io/dragonfly/v2/scheduler/resource/standard"
	"github.com/samber/lo"
)

// SyncPeers is an interface for sync peers.
type SyncPeers interface {
	// AsyncSyncPeers execute action to sync peers, which is async.
	AsyncSyncPeers(context.Context, SyncPeersArgs) error

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

	syncLocker sync.Mutex
	workChan   chan SyncPeersArgs
}

type SyncPeersArgs struct {
	CandidateSchedulerClusters []models.SchedulerCluster
	TaskID                     string
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

// AsyncSyncPeers start to sync peers.
func (s *syncPeers) AsyncSyncPeers(ctx context.Context, args SyncPeersArgs) error {
	if len(args.CandidateSchedulerClusters) == 0 {
		if err := s.db.WithContext(ctx).Find(&args.CandidateSchedulerClusters).Error; err != nil {
			return fmt.Errorf("failed to get candidate scheduler clusters: %v", err)
		}
	}

	s.workChan <- args
	return nil
}

// Serve started sync peers server.
func (s *syncPeers) Serve() {
	tick := time.NewTicker(s.config.Job.SyncPeers.Interval)
	for {
		select {
		case <-tick.C:
			if err := s.syncPeers(context.Background(), nil); err != nil {
				logger.Errorf("sync peers failed periodically: %v", err)
			}
		case args := <-s.workChan:
			err := s.syncPeers(context.Background(), args.CandidateSchedulerClusters)
			if err != nil {
				logger.Errorf("sync peers failed for request: %v", err)
			}

			if args.TaskID != "" {
				job := models.Job{}
				state := machineryv1tasks.StateFailure
				if err == nil {
					state = machineryv1tasks.StateSuccess
				}
				if updateErr := s.db.WithContext(context.Background()).First(&job, args.TaskID).Updates(models.Job{
					State: state,
				}).Error; updateErr != nil {
					logger.Errorf("update sync peers job result failed for request: %v", updateErr)
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

// syncPeers is the real working function in synchronous mode.
func (s *syncPeers) syncPeers(ctx context.Context, candidateSchedulerClusters []models.SchedulerCluster) error {
	if !s.syncLocker.TryLock() {
		return fmt.Errorf("another sync peers is already running")
	}
	defer s.syncLocker.Unlock()

	if len(candidateSchedulerClusters) == 0 {
		if err := s.db.WithContext(ctx).Find(&candidateSchedulerClusters).Error; err != nil {
			return err
		}
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
	// Fetch existing peers from the database
	var existingPeers []models.Peer
	if err := s.db.Model(&models.Peer{}).Where("scheduler_cluster_id = ?", scheduler.SchedulerClusterID).Find(&existingPeers).Error; err != nil {
		log.Error(err)
		return
	}

	// Calculate differences using diffPeers function
	toAdd, toUpdate, toDelete := diffPeers(existingPeers, results)

	// Perform database updates based on the differences
	if err := s.db.WithContext(ctx).CreateInBatches(toAdd, s.config.Job.SyncPeers.BatchSize).Error; err != nil {
		log.Error(err)
	}

	for _, peer := range toUpdate {
		if err := s.db.WithContext(ctx).Model(&models.Peer{}).Where("id = ?", peer.ID).Updates(peer).Error; err != nil {
			log.Error(err)
		}
	}

	for _, peer := range toDelete {
		if err := s.db.WithContext(ctx).Unscoped().Delete(&models.Peer{}, peer.ID).Error; err != nil {
			log.Error(err)
		}
	}
}

func diffPeers(existingPeers []models.Peer, currentPeers []*resource.Host) (toAdd, toUpdate, toDelete []models.Peer) {
	// Convert current peers to a map for quick lookup
	currentPeersMap := lo.KeyBy[string, *resource.Host](currentPeers, func(item *resource.Host) string {
		return item.ID
	})

	// // Convert existing peers to a map for quick lookup
	existingPeersMap := lo.KeyBy[string, models.Peer](existingPeers, func(item models.Peer) string {
		return idgen.HostIDV2(item.IP, item.Hostname, types.ParseHostType(item.Type) != types.HostTypeNormal)
	})

	// Calculate differences
	for id, currentPeer := range currentPeersMap {
		if existingPeer, ok := existingPeersMap[id]; ok {
			// Peer exists in both, check if it needs to be updated
			if !isPeerEqual(existingPeer, *currentPeer) {
				toUpdate = append(toUpdate, convertToModelPeer(*currentPeer))
			}
			// Remove from existingPeersMap to mark it as processed
			delete(existingPeersMap, id)
		} else {
			// Peer exists in currentPeers but not in existingPeers, add it
			toAdd = append(toAdd, convertToModelPeer(*currentPeer))
		}
	}

	// Peers left in existingPeersMap are to be deleted
	toDelete = lo.Values(existingPeersMap)

	return toAdd, toUpdate, lo.Values(existingPeersMap)
}

// Helper function to check if two peers are equal
func isPeerEqual(peer models.Peer, currentPeer resource.Host) bool {
	// Implement the equality check based on your requirements
	// For example, compare all fields that should be considered for equality
	return peer.Type == currentPeer.Type.Name() &&
		peer.IDC == currentPeer.Network.IDC &&
		peer.Location == currentPeer.Network.Location &&
		peer.Port == currentPeer.Port &&
		peer.DownloadPort == currentPeer.DownloadPort &&
		peer.ObjectStoragePort == currentPeer.ObjectStoragePort &&
		peer.OS == currentPeer.OS &&
		peer.Platform == currentPeer.Platform &&
		peer.PlatformFamily == currentPeer.PlatformFamily &&
		peer.PlatformVersion == currentPeer.PlatformVersion &&
		peer.KernelVersion == currentPeer.KernelVersion &&
		peer.GitVersion == currentPeer.Build.GitVersion &&
		peer.GitCommit == currentPeer.Build.GitCommit &&
		peer.BuildPlatform == currentPeer.Build.Platform
}

// Helper function to convert resource.Host to models.Peer
func convertToModelPeer(peer resource.Host) models.Peer {
	return models.Peer{
		Hostname:           peer.Hostname,
		Type:               peer.Type.Name(),
		IDC:                peer.Network.IDC,
		Location:           peer.Network.Location,
		IP:                 peer.IP,
		Port:               peer.Port,
		DownloadPort:       peer.DownloadPort,
		ObjectStoragePort:  peer.ObjectStoragePort,
		State:              models.PeerStateActive,
		OS:                 peer.OS,
		Platform:           peer.Platform,
		PlatformFamily:     peer.PlatformFamily,
		PlatformVersion:    peer.PlatformVersion,
		KernelVersion:      peer.KernelVersion,
		GitVersion:         peer.Build.GitVersion,
		GitCommit:          peer.Build.GitCommit,
		BuildPlatform:      peer.Build.Platform,
		SchedulerClusterID: uint(peer.SchedulerClusterID),
	}
}
