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

package searcher

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/manager/models"
)

func TestSearcher_FindSchedulerClusters(t *testing.T) {
	pluginDir := "."
	tests := []struct {
		name              string
		schedulerClusters []models.SchedulerCluster
		conditions        map[string]string
		expect            func(t *testing.T, data []models.SchedulerCluster, err error)
	}{
		{
			name:              "scheduler clusters is empty",
			schedulerClusters: []models.SchedulerCluster{},
			conditions:        map[string]string{"location": "foo"},
			expect: func(t *testing.T, data []models.SchedulerCluster, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "empty scheduler clusters")
			},
		},
		{
			name: "scheduler clusters have default cluster",
			schedulerClusters: []models.SchedulerCluster{
				{
					Name: "foo",
					Schedulers: []models.Scheduler{
						{
							Hostname: "foo",
							State:    "active",
						},
					},
				},
				{
					Name: "bar",
					Schedulers: []models.Scheduler{
						{
							Hostname: "bar",
							State:    "active",
						},
					},
					IsDefault: true,
				},
			},
			conditions: map[string]string{},
			expect: func(t *testing.T, data []models.SchedulerCluster, err error) {
				assert := assert.New(t)
				assert.Equal(data[0].Name, "bar")
				assert.Equal(data[1].Name, "foo")
				assert.Equal(len(data), 2)
			},
		},
		{
			name: "match according to location condition",
			schedulerClusters: []models.SchedulerCluster{
				{
					Name: "foo",
					Scopes: map[string]any{
						"location": "location-1",
					},
					Schedulers: []models.Scheduler{
						{
							Hostname: "foo",
							State:    "active",
						},
					},
				},
				{
					Name: "bar",
					Schedulers: []models.Scheduler{
						{
							Hostname: "bar",
							State:    "active",
						},
					},
				},
			},
			conditions: map[string]string{"location": "location-1"},
			expect: func(t *testing.T, data []models.SchedulerCluster, err error) {
				assert := assert.New(t)
				assert.Equal(data[0].Name, "foo")
				assert.Equal(data[1].Name, "bar")
			},
		},
		{
			name: "match according to idc condition",
			schedulerClusters: []models.SchedulerCluster{
				{
					Name: "foo",
					Scopes: map[string]any{
						"idc": "idc|idc-1",
					},
					Schedulers: []models.Scheduler{
						{
							Hostname: "foo",
							State:    "active",
						},
					},
				},
				{
					Name: "bar",
					Scopes: map[string]any{
						"idc": "idc-2|idc-3",
					},
					Schedulers: []models.Scheduler{
						{
							Hostname: "bar",
							State:    "active",
						},
					},
				},
			},
			conditions: map[string]string{"idc": "idc-2"},
			expect: func(t *testing.T, data []models.SchedulerCluster, err error) {
				assert := assert.New(t)
				assert.Equal(data[0].Name, "bar")
				assert.Equal(data[1].Name, "foo")
				assert.Equal(len(data), 2)
			},
		},
		{
			name: "match according to cidr condition",
			schedulerClusters: []models.SchedulerCluster{
				{
					Name: "foo",
					Scopes: map[string]any{
						"cidrs": []string{"128.168.1.0/24"},
					},
					Schedulers: []models.Scheduler{
						{
							Hostname: "foo",
							State:    "active",
						},
					},
				},
				{
					Name:   "bar",
					Scopes: map[string]any{},
					Schedulers: []models.Scheduler{
						{
							Hostname: "bar",
							State:    "active",
						},
					},
				},
			},
			conditions: map[string]string{},
			expect: func(t *testing.T, data []models.SchedulerCluster, err error) {
				assert := assert.New(t)
				assert.Equal(data[0].Name, "foo")
				assert.Equal(data[1].Name, "bar")
				assert.Equal(len(data), 2)
			},
		},
		{
			name: "match according to hostname condition",
			schedulerClusters: []models.SchedulerCluster{
				{
					Name: "foo",
					Scopes: map[string]any{
						"hostnames": []string{"f.*"},
					},
					Schedulers: []models.Scheduler{
						{
							Hostname: "foo",
							State:    "active",
						},
					},
				},
				{
					Name:   "bar",
					Scopes: map[string]any{},
					Schedulers: []models.Scheduler{
						{
							Hostname: "bar",
							State:    "active",
						},
					},
				},
			},
			conditions: map[string]string{},
			expect: func(t *testing.T, data []models.SchedulerCluster, err error) {
				assert := assert.New(t)
				assert.Equal(data[0].Name, "foo")
				assert.Equal(data[1].Name, "bar")
				assert.Equal(len(data), 2)
			},
		},
		{
			name: "match according to idc and location conditions",
			schedulerClusters: []models.SchedulerCluster{
				{
					Name: "foo",
					Scopes: map[string]any{
						"idc":      "idc-3",
						"location": "location-1",
					},
					Schedulers: []models.Scheduler{
						{
							Hostname: "foo",
							State:    "active",
						},
					},
				},
				{
					Name: "bar",
					Scopes: map[string]any{
						"idc":      "idc-1",
						"location": "location-3",
					},
					Schedulers: []models.Scheduler{
						{
							Hostname: "bar",
							State:    "active",
						},
					},
				},
				{
					Name: "baz",
					Scopes: map[string]any{
						"idc":      "idc-1",
						"location": "location-1|location-2",
					},
					Schedulers: []models.Scheduler{
						{
							Hostname: "baz",
							State:    "active",
						},
					},
				},
				{
					Name: "bax",
					Scopes: map[string]any{
						"idc":      "idc-3",
						"location": "location-3",
					},
					Schedulers: []models.Scheduler{
						{
							Hostname: "bax",
							State:    "active",
						},
					},
					IsDefault: true,
				},
				{
					Name: "bac",
					Scopes: map[string]any{
						"idc":      "idc-1",
						"location": "location-2",
					},
					Schedulers: []models.Scheduler{
						{
							Hostname: "bac",
							State:    "active",
						},
					},
				},
			},
			conditions: map[string]string{
				"idc":      "idc-1",
				"location": "location-1|location-2",
			},
			expect: func(t *testing.T, data []models.SchedulerCluster, err error) {
				assert := assert.New(t)
				assert.Equal(data[0].Name, "baz")
				assert.Equal(data[1].Name, "bar")
				assert.Equal(data[2].Name, "bac")
				assert.Equal(data[3].Name, "foo")
				assert.Equal(data[4].Name, "bax")
				assert.Equal(len(data), 5)
			},
		},
		{
			name: "match according to all conditions with the case insensitive",
			schedulerClusters: []models.SchedulerCluster{
				{
					Name: "foo",
					Scopes: map[string]any{
						"idc":       "IDC-1",
						"location":  "LOCATION-2",
						"cidrs":     []string{"128.168.1.0/24"},
						"hostnames": []string{"b.*"},
					},
					Schedulers: []models.Scheduler{
						{
							Hostname: "foo",
							State:    "active",
						},
					},
				},
				{
					Name: "bar",
					Scopes: map[string]any{
						"idc":       "IDC-1",
						"location":  "LOCATION-1",
						"cidrs":     []string{"128.168.1.0/24"},
						"hostnames": []string{"c.*"},
					},
					Schedulers: []models.Scheduler{
						{
							Hostname: "bar",
							State:    "active",
						},
					},
				},
				{
					Name: "baz",
					Scopes: map[string]any{
						"idc":       "IDC-1",
						"location":  "LOCATION-1|LOCATION-2",
						"cidrs":     []string{"128.168.1.0/24"},
						"hostnames": []string{"f.*"},
					},
					Schedulers: []models.Scheduler{
						{
							Hostname: "baz",
							State:    "active",
						},
					},
				},
				{
					Name: "bax",
					Scopes: map[string]any{
						"idc":       "IDC-1",
						"location":  "LOCATION-2",
						"cidrs":     []string{"128.168.1.0/24"},
						"hostnames": []string{"d.*"},
					},
					Schedulers: []models.Scheduler{
						{
							Hostname: "bax",
							State:    "active",
						},
					},
					IsDefault: true,
				},
				{
					Name: "bac",
					Scopes: map[string]any{
						"idc":       "IDC-1",
						"location":  "LOCATION-2",
						"cidrs":     []string{"128.168.1.0/24"},
						"hostnames": []string{"e.*"},
					},
					Schedulers: []models.Scheduler{
						{
							Hostname: "bac",
							State:    "active",
						},
					},
				},
				{
					Name: "bae",
					Scopes: map[string]any{
						"idc":       "IDC-1",
						"location":  "LOCATION-2",
						"cidrs":     []string{"128.168.1.0/24"},
						"hostnames": []string{"a.*"},
					},
					Schedulers: []models.Scheduler{
						{
							Hostname: "bae",
							State:    "active",
						},
					},
					IsDefault: true,
				},
				{
					Name: "bat",
					Scopes: map[string]any{
						"idc":       "IDC-1",
						"location":  "LOCATION-2",
						"cidrs":     []string{"192.168.1.0/24"},
						"hostnames": []string{"g.*"},
					},
					Schedulers: []models.Scheduler{
						{
							Hostname: "bae",
							State:    "active",
						},
					},
					IsDefault: true,
				},
			},
			conditions: map[string]string{
				"idc":      "idc-1",
				"location": "location-1|location-2",
			},
			expect: func(t *testing.T, data []models.SchedulerCluster, err error) {
				assert := assert.New(t)
				assert.Equal(data[0].Name, "baz")
				assert.Equal(data[1].Name, "bar")
				assert.Equal(data[2].Name, "bax")
				assert.Equal(data[3].Name, "bae")
				assert.Equal(data[4].Name, "foo")
				assert.Equal(data[5].Name, "bac")
				assert.Equal(data[6].Name, "bat")
				assert.Equal(len(data), 7)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			searcher := New(pluginDir)
			clusters, found := searcher.FindSchedulerClusters(context.Background(), tc.schedulerClusters, "128.168.1.0", "foo", tc.conditions, logger.CoreLogger)
			tc.expect(t, clusters, found)
		})
	}
}
