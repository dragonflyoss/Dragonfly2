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

package ratelimiter

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/mennanov/limiters"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/manager/config"
	"d7y.io/dragonfly/v2/manager/database"
	"d7y.io/dragonfly/v2/manager/models"
	"d7y.io/dragonfly/v2/manager/types"
)

const (
	// jobRateLimiterSuffix is the suffix of the job rate limiter key.
	jobRateLimiterSuffix = "job"

	// defaultRefreshInterval is the default interval to refresh the rate limiters.
	defaultRefreshInterval = 10 * time.Minute
)

// JobRateLimiter is an interface for a job rate limiter.
type JobRateLimiter interface {
	// TakeByClusterID takes a token from the rate limiter by cluster ID, returns the time to wait
	// until the next token is available.
	TakeByClusterID(ctx context.Context, clusterID uint, tokens int64) (time.Duration, error)

	// TakeByClusterIDs takes a token from the rate limiter by cluster IDs, returns the time to wait
	// until the next token is available. If only one cluster is reatcted the limit, return the
	// time to wait.
	TakeByClusterIDs(ctx context.Context, clusterIDs []uint, tokens int64) (time.Duration, error)

	// Serve started job rate limiter server.
	Serve()

	// Stop job rate limiter server.
	Stop()
}

// jobRateLimiter is an implementation of JobRateLimiter.
type jobRateLimiter struct {
	// database used to store the rate limit.
	database *database.Database

	// clusters used to store the rate limit by cluster.
	clusters *sync.Map

	// refreshInterval is the interval to refresh the rate limiters.
	refreshInterval time.Duration

	// done is the channel to stop the rate limiter server.
	done chan struct{}
}

// NewJobRateLimiter creates a new instance of JobRateLimiter.
func NewJobRateLimiter(database *database.Database) (JobRateLimiter, error) {
	j := &jobRateLimiter{
		database:        database,
		clusters:        &sync.Map{},
		refreshInterval: defaultRefreshInterval,
		done:            make(chan struct{}),
	}

	if err := j.refresh(context.Background()); err != nil {
		return nil, err
	}

	return j, nil
}

// TakeByClusterID takes a token from the rate limiter by cluster ID, returns the time to wait
// until the next token is available.
func (j *jobRateLimiter) TakeByClusterID(ctx context.Context, clusterID uint, tokens int64) (time.Duration, error) {
	rawLimiter, loaded := j.clusters.Load(clusterID)
	if !loaded {
		return 0, fmt.Errorf("cluster %d not found", clusterID)
	}

	limiter, ok := rawLimiter.(*limiters.TokenBucket)
	if !ok {
		return 0, fmt.Errorf("cluster %d is not a distributed rate limiter", clusterID)
	}

	return limiter.Take(ctx, tokens)
}

// TakeByClusterIDs takes a token from the rate limiter by cluster IDs, returns the time to wait
// until the next token is available.
func (j *jobRateLimiter) TakeByClusterIDs(ctx context.Context, clusterIDs []uint, tokens int64) (time.Duration, error) {
	for _, clusterID := range clusterIDs {
		if duration, err := j.TakeByClusterID(ctx, clusterID, tokens); err != nil {
			return duration, err
		}
	}

	return 0, nil
}

// Serve started rate limiter server.
func (j *jobRateLimiter) Serve() {
	tick := time.NewTicker(j.refreshInterval)
	for {
		select {
		case <-tick.C:
			logger.Infof("refresh job rate limiter started")
			if err := j.refresh(context.Background()); err != nil {
				logger.Errorf("refresh job rate limiter failed: %v", err)
			}
		case <-j.done:
			return
		}
	}
}

// Stop rate limiter server.
func (j *jobRateLimiter) Stop() {
	close(j.done)
}

// refresh refreshes the rate limiters for all scheduler clusters.
func (j *jobRateLimiter) refresh(ctx context.Context) error {
	var schedulerClusters []models.SchedulerCluster
	if err := j.database.DB.WithContext(ctx).Find(&schedulerClusters).Error; err != nil {
		return err
	}

	j.clusters.Clear()
	for _, schedulerCluster := range schedulerClusters {
		b, err := schedulerCluster.Config.MarshalJSON()
		if err != nil {
			logger.Errorf("marshal scheduler cluster %d config failed: %v", schedulerCluster.ID, err)
			return err
		}

		var schedulerClusterConfig types.SchedulerClusterConfig
		if err := json.Unmarshal(b, &schedulerClusterConfig); err != nil {
			logger.Errorf("unmarshal scheduler cluster %d config failed: %v", schedulerCluster.ID, err)
			return err
		}

		// Use the default rate limit if the rate limit is not set.
		jobRateLimit := config.DefaultClusterJobRateLimit
		if schedulerClusterConfig.JobRateLimit != 0 {
			jobRateLimit = int(schedulerClusterConfig.JobRateLimit)
		}

		logger.Debugf("create job rate limiter for scheduler cluster %d with rate limit %d", schedulerCluster.ID, jobRateLimit)
		j.clusters.Store(schedulerCluster.ID,
			NewDistributedRateLimiter(j.database.RDB, j.key(schedulerCluster.ID)).TokenBucket(ctx, int64(jobRateLimit), time.Second))
	}

	return nil
}

// key is the rate limiter key for storing value in the database.
func (j *jobRateLimiter) key(clusterID uint) string {
	return fmt.Sprintf("%d-%s", clusterID, jobRateLimiterSuffix)
}
