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

//go:generate mockgen -destination mocks/gc_mock.go -source gc.go -package mocks

package job

import (
	"context"
	"time"

	"gorm.io/gorm"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/manager/config"
	"d7y.io/dragonfly/v2/manager/models"
)

// GC is an interface for gc.
type GC interface {
	// Serve started gc server.
	Serve()

	// Stop gc server.
	Stop()
}

// gc is an implementation of GC.
type gc struct {
	config *config.Config
	db     *gorm.DB
	done   chan struct{}
}

// newGC returns a new GC.
func newGC(cfg *config.Config, gdb *gorm.DB) (GC, error) {
	return &gc{
		config: cfg,
		db:     gdb,
		done:   make(chan struct{}),
	}, nil
}

// Serve started gc server.
func (gc *gc) Serve() {
	tick := time.NewTicker(gc.config.Job.GC.Interval)
	for {
		select {
		case <-tick.C:
			logger.Infof("gc job started")
			if err := gc.deleteInBatches(context.Background()); err != nil {
				logger.Errorf("gc job failed: %v", err)
			}
		case <-gc.done:
			return
		}
	}
}

// Stop gc server.
func (gc *gc) Stop() {
	close(gc.done)
}

// deleteInBatches deletes jobs in batches.
func (gc *gc) deleteInBatches(ctx context.Context) error {
	for {
		result := gc.db.WithContext(ctx).Where("created_at < ?", time.Now().Add(-gc.config.Job.GC.TTL)).Limit(gc.config.Job.GC.BatchSize).Unscoped().Delete(&models.Job{})
		if result.Error != nil {
			return result.Error
		}

		if result.RowsAffected == 0 {
			break
		}

		logger.Infof("gc job deleted %d jobs", result.RowsAffected)
	}

	return nil
}
