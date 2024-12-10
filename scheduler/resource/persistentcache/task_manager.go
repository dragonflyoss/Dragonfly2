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

//go:generate mockgen -destination task_manager_mock.go -source task_manager.go -package persistentcache

package persistentcache

import (
	"context"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/digest"
	pkgredis "d7y.io/dragonfly/v2/pkg/redis"
	"d7y.io/dragonfly/v2/scheduler/config"
)

// TaskManager is the interface used for persistent cache task manager.
type TaskManager interface {
	// Load returns persistent cache task by a key.
	Load(context.Context, string) (*Task, bool)

	// LoadCorrentReplicaCount returns current replica count of the persistent cache task.
	LoadCorrentReplicaCount(context.Context, string) (int64, error)

	// LoadCurrentPersistentReplicaCount returns current persistent replica count of the persistent cache task.
	LoadCurrentPersistentReplicaCount(context.Context, string) (int64, error)

	// Store sets persistent cache task.
	Store(context.Context, *Task) error

	// Delete deletes persistent cache task by a key.
	Delete(context.Context, string) error

	// LoadAll returns all persistent cache tasks.
	LoadAll(context.Context) ([]*Task, error)
}

// taskManager contains content for persistent cache task manager.
type taskManager struct {
	// Config is scheduler config.
	config *config.Config

	// Redis universal client interface.
	rdb redis.UniversalClient
}

// New persistent cache task manager interface.
func newTaskManager(cfg *config.Config, rdb redis.UniversalClient) TaskManager {
	return &taskManager{config: cfg, rdb: rdb}
}

// Load returns persistent cache task by a key.
func (t *taskManager) Load(ctx context.Context, taskID string) (*Task, bool) {
	log := logger.WithTaskID(taskID)
	rawTask, err := t.rdb.HGetAll(ctx, pkgredis.MakePersistentCacheTaskKeyInScheduler(t.config.Manager.SchedulerClusterID, taskID)).Result()
	if err != nil {
		log.Errorf("getting task failed from redis: %v", err)
		return nil, false
	}

	if len(rawTask) == 0 {
		return nil, false
	}

	// Set integer fields from raw task.
	persistentReplicaCount, err := strconv.ParseUint(rawTask["persistent_replica_count"], 10, 64)
	if err != nil {
		log.Errorf("parsing persistent replica count failed: %v", err)
		return nil, false
	}

	pieceLength, err := strconv.ParseInt(rawTask["piece_length"], 10, 32)
	if err != nil {
		log.Errorf("parsing piece length failed: %v", err)
		return nil, false
	}

	contentLength, err := strconv.ParseInt(rawTask["content_length"], 10, 64)
	if err != nil {
		log.Errorf("parsing content length failed: %v", err)
		return nil, false
	}

	totalPieceCount, err := strconv.ParseInt(rawTask["total_piece_count"], 10, 32)
	if err != nil {
		log.Errorf("parsing total piece count failed: %v", err)
		return nil, false
	}

	// Set time fields from raw task.
	ttl, err := strconv.ParseUint(rawTask["ttl"], 10, 64)
	if err != nil {
		log.Errorf("parsing ttl failed: %v", err)
		return nil, false
	}

	createdAt, err := time.Parse(time.RFC3339, rawTask["created_at"])
	if err != nil {
		log.Errorf("parsing created at failed: %v", err)
		return nil, false
	}

	updatedAt, err := time.Parse(time.RFC3339, rawTask["updated_at"])
	if err != nil {
		log.Errorf("parsing updated at failed: %v", err)
		return nil, false
	}

	// Set digest from raw task.
	digest, err := digest.Parse(rawTask["digest"])
	if err != nil {
		log.Errorf("parsing digest failed: %v", err)
		return nil, false
	}

	return NewTask(
		rawTask["id"],
		rawTask["tag"],
		rawTask["application"],
		rawTask["state"],
		persistentReplicaCount,
		int32(pieceLength),
		contentLength,
		int32(totalPieceCount),
		digest,
		time.Duration(ttl),
		createdAt,
		updatedAt,
		logger.WithTaskID(rawTask["id"]),
	), true
}

// LoadCorrentReplicaCount returns current replica count of the persistent cache task.
func (t *taskManager) LoadCorrentReplicaCount(ctx context.Context, taskID string) (int64, error) {
	return t.rdb.SCard(ctx, pkgredis.MakePersistentCachePeersOfPersistentCacheTaskInScheduler(t.config.Manager.SchedulerClusterID, taskID)).Result()
}

// LoadCurrentPersistentReplicaCount returns current persistent replica count of the persistent cache task.
func (t *taskManager) LoadCurrentPersistentReplicaCount(ctx context.Context, taskID string) (int64, error) {
	return t.rdb.SCard(ctx, pkgredis.MakePersistentPeersOfPersistentCacheTaskInScheduler(t.config.Manager.SchedulerClusterID, taskID)).Result()
}

// Store sets persistent cache task.
func (t *taskManager) Store(ctx context.Context, task *Task) error {
	if _, err := t.rdb.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		if _, err := pipe.HSet(ctx,
			pkgredis.MakePersistentCacheTaskKeyInScheduler(t.config.Manager.SchedulerClusterID, task.ID),
			"id", task.ID,
			"persistent_replica_count", task.PersistentReplicaCount,
			"digest", task.Digest.String(),
			"tag", task.Tag,
			"application", task.Application,
			"piece_length", task.PieceLength,
			"content_length", task.ContentLength,
			"total_piece_count", task.TotalPieceCount,
			"state", task.FSM.Current(),
			"ttl", task.TTL,
			"created_at", task.CreatedAt.Format(time.RFC3339),
			"updated_at", task.UpdatedAt.Format(time.RFC3339)).Result(); err != nil {
			task.Log.Errorf("store task failed: %v", err)
			return err
		}

		ttl := task.TTL - time.Since(task.CreatedAt)
		if _, err := pipe.Expire(ctx, pkgredis.MakePersistentCacheTaskKeyInScheduler(t.config.Manager.SchedulerClusterID, task.ID), ttl).Result(); err != nil {
			task.Log.Errorf("set task ttl failed: %v", err)
			return err
		}

		return nil
	}); err != nil {
		task.Log.Errorf("store task failed: %v", err)
		return err
	}

	return nil
}

// Delete deletes persistent cache task by a key.
func (t *taskManager) Delete(ctx context.Context, taskID string) error {
	_, err := t.rdb.Del(ctx, pkgredis.MakePersistentCacheTaskKeyInScheduler(t.config.Manager.SchedulerClusterID, taskID)).Result()
	return err
}

// LoadAll returns all persistent cache tasks.
func (t *taskManager) LoadAll(ctx context.Context) ([]*Task, error) {
	var (
		tasks  []*Task
		cursor uint64
	)

	for {
		var (
			taskKeys []string
			err      error
		)

		taskKeys, cursor, err = t.rdb.Scan(ctx, cursor, pkgredis.MakePersistentCacheTasksInScheduler(t.config.Manager.SchedulerClusterID), 10).Result()
		if err != nil {
			logger.Error("scan tasks failed")
			return nil, err
		}

		for _, taskKey := range taskKeys {
			task, loaded := t.Load(ctx, taskKey)
			if !loaded {
				logger.WithTaskID(taskKey).Error("load task failed")
				continue
			}

			tasks = append(tasks, task)
		}

		if cursor == 0 {
			break
		}
	}

	return tasks, nil
}
