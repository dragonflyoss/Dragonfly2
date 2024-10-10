/*
 *     Copyright 2020 The Dragonfly Authors
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

package rpcserver

import (
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"gorm.io/gorm"

	"d7y.io/dragonfly/v2/manager/cache"
	"d7y.io/dragonfly/v2/manager/config"
	"d7y.io/dragonfly/v2/manager/database"
	"d7y.io/dragonfly/v2/manager/models"
	"d7y.io/dragonfly/v2/manager/searcher"
	"d7y.io/dragonfly/v2/pkg/objectstorage"
	managerserver "d7y.io/dragonfly/v2/pkg/rpc/manager/server"
)

// Server is grpc server.
type Server struct {
	// Manager configuration.
	config *config.Config

	// GORM instance.
	db *gorm.DB

	// Redis universal client interface.
	rdb redis.UniversalClient

	// Cache instance.
	cache *cache.Cache

	// Searcher interface.
	searcher searcher.Searcher

	// Object storage interface.
	objectStorage objectstorage.ObjectStorage
}

// New returns a new manager server from the given options.
func New(
	cfg *config.Config, database *database.Database, cache *cache.Cache, searcher searcher.Searcher,
	objectStorage objectstorage.ObjectStorage, opts ...grpc.ServerOption) (*Server, *grpc.Server, error) {
	s := &Server{
		config:        cfg,
		db:            database.DB,
		rdb:           database.RDB,
		cache:         cache,
		searcher:      searcher,
		objectStorage: objectStorage,
	}

	return s, managerserver.New(
		newManagerServerV1(s.config, database, s.cache, s.searcher, s.objectStorage),
		newManagerServerV2(s.config, database, s.cache, s.searcher),
		opts...), nil
}

// Get scheduler cluster names.
func getSchedulerClusterNames(clusters []models.SchedulerCluster) []string {
	names := []string{}
	for _, cluster := range clusters {
		names = append(names, cluster.Name)
	}

	return names
}
