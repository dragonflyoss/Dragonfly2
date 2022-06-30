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

package main

import (
	"context"

	"d7y.io/dragonfly/v2/manager/model"
	"d7y.io/dragonfly/v2/pkg/rpc/manager"
)

type searcher struct{}

func (s *searcher) FindSchedulerClusters(ctx context.Context, schedulerClusters []model.SchedulerCluster, client *manager.ListSchedulersRequest) ([]model.SchedulerCluster, error) {
	return []model.SchedulerCluster{{Name: "foo"}}, nil
}

func DragonflyPluginInit(option map[string]string) (any, map[string]string, error) {
	return &searcher{}, map[string]string{"type": "manager", "name": "searcher"}, nil
}
