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
			name: "security_domain does not match",
			schedulerClusters: []models.SchedulerCluster{
				{
					Name: "foo",
					SecurityGroup: models.SecurityGroup{
						SecurityRules: []models.SecurityRule{
							{
								Domain: "domain-2",
							},
						},
					},
					Schedulers: []models.Scheduler{
						{
							HostName: "foo",
							State:    "active",
						},
					},
				},
				{
					Name: "bar",
				},
			},
			conditions: map[string]string{"security_domain": "domain-1"},
			expect: func(t *testing.T, data []models.SchedulerCluster, err error) {
				assert := assert.New(t)
				assert.Error(err)
			},
		},
		{
			name: "scheduler clusters have default cluster",
			schedulerClusters: []models.SchedulerCluster{
				{
					Name: "foo",
					SecurityGroup: models.SecurityGroup{
						SecurityRules: []models.SecurityRule{
							{
								Domain: "domain-2",
							},
						},
					},
					Schedulers: []models.Scheduler{
						{
							HostName: "foo",
							State:    "active",
						},
					},
				},
				{
					Name: "bar",
					Schedulers: []models.Scheduler{
						{
							HostName: "bar",
							State:    "active",
						},
					},
					IsDefault: true,
				},
				{
					Name: "baz",
					Schedulers: []models.Scheduler{
						{
							HostName: "baz",
							State:    "active",
						},
					},
				},
			},
			conditions: map[string]string{"security_domain": "domain-1"},
			expect: func(t *testing.T, data []models.SchedulerCluster, err error) {
				assert := assert.New(t)
				assert.Equal(data[0].Name, "bar")
				assert.Equal(data[1].Name, "baz")
				assert.Equal(len(data), 2)
			},
		},
		{
			name: "scheduler cluster SecurityRules does not exist",
			schedulerClusters: []models.SchedulerCluster{
				{
					Name: "foo",
					SecurityGroup: models.SecurityGroup{
						SecurityRules: []models.SecurityRule{
							{
								Domain: "domain-2",
							},
						},
					},
					Schedulers: []models.Scheduler{
						{
							HostName: "foo",
							State:    "active",
						},
					},
				},
				{
					Name: "bar",
					Schedulers: []models.Scheduler{
						{
							HostName: "bar",
							State:    "active",
						},
					},
				},
			},
			conditions: map[string]string{"security_domain": "domain-1"},
			expect: func(t *testing.T, data []models.SchedulerCluster, err error) {
				assert := assert.New(t)
				assert.Equal(data[0].Name, "bar")
				assert.Equal(len(data), 1)
			},
		},
		{
			name: "match according to security_domain condition",
			schedulerClusters: []models.SchedulerCluster{
				{
					Name: "foo",
					SecurityGroup: models.SecurityGroup{
						SecurityRules: []models.SecurityRule{
							{
								Domain: "domain-2",
							},
						},
					},
					Schedulers: []models.Scheduler{
						{
							HostName: "foo",
							State:    "active",
						},
					},
				},
				{
					Name: "bar",
					Schedulers: []models.Scheduler{
						{
							HostName: "bar",
							State:    "active",
						},
					},
				},
				{
					Name: "baz",
					SecurityGroup: models.SecurityGroup{
						SecurityRules: []models.SecurityRule{
							{
								Domain: "domain-1",
							},
						},
					},
					Schedulers: []models.Scheduler{
						{
							HostName: "baz",
							State:    "active",
						},
					},
				},
			},
			conditions: map[string]string{"security_domain": "domain-1"},
			expect: func(t *testing.T, data []models.SchedulerCluster, err error) {
				assert := assert.New(t)
				assert.Equal(data[0].Name, "baz")
				assert.Equal(data[1].Name, "bar")
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
							HostName: "foo",
							State:    "active",
						},
					},
				},
				{
					Name: "bar",
					Schedulers: []models.Scheduler{
						{
							HostName: "bar",
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
							HostName: "foo",
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
							HostName: "bar",
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
							HostName: "foo",
							State:    "active",
						},
					},
				},
				{
					Name:   "bar",
					Scopes: map[string]any{},
					Schedulers: []models.Scheduler{
						{
							HostName: "bar",
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
			name: "match according to location and idc condition",
			schedulerClusters: []models.SchedulerCluster{
				{
					Name: "foo",
					Scopes: map[string]any{
						"location": "location-1|location-2",
						"idc":      "idc-1|idc-2",
					},
					Schedulers: []models.Scheduler{
						{
							HostName: "foo",
							State:    "active",
						},
					},
				},
				{
					Name: "bar",
					Schedulers: []models.Scheduler{
						{
							HostName: "bar",
							State:    "active",
						},
					},
				},
			},
			conditions: map[string]string{
				"location": "location-1",
				"idc":      "idc-1",
			},
			expect: func(t *testing.T, data []models.SchedulerCluster, err error) {
				assert := assert.New(t)
				assert.Equal(data[0].Name, "foo")
				assert.Equal(data[1].Name, "bar")
				assert.Equal(len(data), 2)
			},
		},
		{
			name: "match according to security_domain and location conditions",
			schedulerClusters: []models.SchedulerCluster{
				{
					Name: "foo",
					Scopes: map[string]any{
						"location": "location-1",
					},
					SecurityGroup: models.SecurityGroup{
						SecurityRules: []models.SecurityRule{
							{
								Domain: "domain-1",
							},
						},
					},
					Schedulers: []models.Scheduler{
						{
							HostName: "foo",
							State:    "active",
						},
					},
				},
				{
					Name: "bar",
					Schedulers: []models.Scheduler{
						{
							HostName: "bar",
							State:    "active",
						},
					},
				},
			},
			conditions: map[string]string{
				"security_domain": "domain-1",
				"location":        "location-1",
			},
			expect: func(t *testing.T, data []models.SchedulerCluster, err error) {
				assert := assert.New(t)
				assert.Equal(data[0].Name, "foo")
				assert.Equal(data[1].Name, "bar")
				assert.Equal(len(data), 2)
			},
		},
		{
			name: "match according to security_domain and idc conditions",
			schedulerClusters: []models.SchedulerCluster{
				{
					Name: "foo",
					Scopes: map[string]any{
						"idc": "idc-1",
					},
					SecurityGroup: models.SecurityGroup{
						SecurityRules: []models.SecurityRule{
							{
								Domain: "domain-1",
							},
						},
					},
					Schedulers: []models.Scheduler{
						{
							HostName: "foo",
							State:    "active",
						},
					},
				},
				{
					Name: "bar",
					Schedulers: []models.Scheduler{
						{
							HostName: "bar",
							State:    "active",
						},
					},
				},
			},
			conditions: map[string]string{
				"security_domain": "domain-1",
				"idc":             "idc-1",
			},
			expect: func(t *testing.T, data []models.SchedulerCluster, err error) {
				assert := assert.New(t)
				assert.Equal(data[0].Name, "foo")
				assert.Equal(data[1].Name, "bar")
				assert.Equal(len(data), 2)
			},
		},
		{
			name: "match according to security_domain, idc and location conditions",
			schedulerClusters: []models.SchedulerCluster{
				{
					Name: "foo",
					Scopes: map[string]any{
						"idc":      "idc-1",
						"location": "location-1",
					},
					SecurityGroup: models.SecurityGroup{
						SecurityRules: []models.SecurityRule{
							{
								Domain: "domain-1",
							},
						},
					},
					Schedulers: []models.Scheduler{
						{
							HostName: "foo",
							State:    "active",
						},
					},
				},
				{
					Name: "bar",
					Scopes: map[string]any{
						"idc":      "idc-2",
						"location": "location-1|location-2",
					},
					SecurityGroup: models.SecurityGroup{
						SecurityRules: []models.SecurityRule{
							{
								Domain: "domain-1",
							},
						},
					},
					Schedulers: []models.Scheduler{
						{
							HostName: "bar",
							State:    "active",
						},
					},
				},
				{
					Name: "baz",
					Scopes: map[string]any{
						"idc":      "idc-2",
						"location": "location-1",
					},
					SecurityGroup: models.SecurityGroup{
						SecurityRules: []models.SecurityRule{
							{
								Domain: "domain-2",
							},
						},
					},
					Schedulers: []models.Scheduler{
						{
							HostName: "baz",
							State:    "active",
						},
					},
				},
			},
			conditions: map[string]string{
				"security_domain": "domain-1",
				"idc":             "idc-1|idc-2",
				"location":        "location-1|location-2",
			},
			expect: func(t *testing.T, data []models.SchedulerCluster, err error) {
				assert := assert.New(t)
				assert.Equal(data[0].Name, "bar")
				assert.Equal(data[1].Name, "foo")
				assert.Equal(len(data), 2)
			},
		},
		{
			name: "match according to all conditions",
			schedulerClusters: []models.SchedulerCluster{
				{
					Name: "foo",
					Scopes: map[string]any{
						"idc":      "idc-1",
						"location": "location-2",
					},
					SecurityGroup: models.SecurityGroup{
						SecurityRules: []models.SecurityRule{
							{
								Domain: "domain-1",
							},
						},
					},
					Schedulers: []models.Scheduler{
						{
							HostName: "foo",
							State:    "active",
						},
					},
				},
				{
					Name: "bar",
					Scopes: map[string]any{
						"idc":      "idc-1",
						"location": "location-1",
					},
					SecurityGroup: models.SecurityGroup{
						SecurityRules: []models.SecurityRule{
							{
								Domain: "domain-1",
							},
						},
					},
					Schedulers: []models.Scheduler{
						{
							HostName: "bar",
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
							HostName: "baz",
							State:    "active",
						},
					},
				},
				{
					Name: "bax",
					Scopes: map[string]any{
						"idc":      "idc-1",
						"location": "location-2",
					},
					Schedulers: []models.Scheduler{
						{
							HostName: "bax",
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
					SecurityGroup: models.SecurityGroup{
						SecurityRules: []models.SecurityRule{
							{
								Domain: "domain-2",
							},
						},
					},
					Schedulers: []models.Scheduler{
						{
							HostName: "bac",
							State:    "active",
						},
					},
				},
			},
			conditions: map[string]string{
				"security_domain": "domain-1",
				"idc":             "idc-1|idc-2",
				"location":        "location-1|location-2",
			},
			expect: func(t *testing.T, data []models.SchedulerCluster, err error) {
				assert := assert.New(t)
				assert.Equal(data[0].Name, "bar")
				assert.Equal(data[1].Name, "foo")
				assert.Equal(data[2].Name, "baz")
				assert.Equal(data[3].Name, "bax")
				assert.Equal(len(data), 4)
			},
		},
		{
			name: "match according to all conditions with the case insensitive",
			schedulerClusters: []models.SchedulerCluster{
				{
					Name: "foo",
					Scopes: map[string]any{
						"idc":      "IDC-1",
						"location": "LOCATION-2",
						"cidrs":    []string{"128.168.1.0/24"},
					},
					SecurityGroup: models.SecurityGroup{
						SecurityRules: []models.SecurityRule{
							{
								Domain: "DOMAIN-1",
							},
						},
					},
					Schedulers: []models.Scheduler{
						{
							HostName: "foo",
							State:    "active",
						},
					},
				},
				{
					Name: "bar",
					Scopes: map[string]any{
						"idc":      "IDC-1",
						"location": "LOCATION-1",
						"cidrs":    []string{"128.168.1.0/24"},
					},
					SecurityGroup: models.SecurityGroup{
						SecurityRules: []models.SecurityRule{
							{
								Domain: "DOMAIN-1",
							},
						},
					},
					Schedulers: []models.Scheduler{
						{
							HostName: "bar",
							State:    "active",
						},
					},
				},
				{
					Name: "baz",
					Scopes: map[string]any{
						"idc":      "IDC-1",
						"location": "LOCATION-1|LOCATION-2",
						"cidrs":    []string{"128.168.1.0/24"},
					},
					Schedulers: []models.Scheduler{
						{
							HostName: "baz",
							State:    "active",
						},
					},
				},
				{
					Name: "bax",
					Scopes: map[string]any{
						"idc":      "IDC-1",
						"location": "LOCATION-2",
						"cidrs":    []string{"128.168.1.0/24"},
					},
					Schedulers: []models.Scheduler{
						{
							HostName: "bax",
							State:    "active",
						},
					},
					IsDefault: true,
				},
				{
					Name: "bac",
					Scopes: map[string]any{
						"idc":      "IDC-1",
						"location": "LOCATION-2",
						"cidrs":    []string{"128.168.1.0/24"},
					},
					SecurityGroup: models.SecurityGroup{
						SecurityRules: []models.SecurityRule{
							{
								Domain: "DOMAIN-2",
							},
						},
					},
					Schedulers: []models.Scheduler{
						{
							HostName: "bac",
							State:    "active",
						},
					},
				},
				{
					Name: "bae",
					Scopes: map[string]any{
						"idc":      "IDC-1",
						"location": "LOCATION-2",
						"cidrs":    []string{"128.168.1.0/24"},
					},
					SecurityGroup: models.SecurityGroup{
						SecurityRules: []models.SecurityRule{
							{
								Domain: "DOMAIN-2",
							},
						},
					},
					Schedulers: []models.Scheduler{
						{
							HostName: "bae",
							State:    "active",
						},
					},
					IsDefault: true,
				},
			},
			conditions: map[string]string{
				"security_domain": "domain-1",
				"idc":             "idc-1|idc-2",
				"location":        "location-1|location-2",
			},
			expect: func(t *testing.T, data []models.SchedulerCluster, err error) {
				assert := assert.New(t)
				assert.Equal(data[0].Name, "bae")
				assert.Equal(data[1].Name, "bar")
				assert.Equal(data[2].Name, "foo")
				assert.Equal(data[3].Name, "baz")
				assert.Equal(data[4].Name, "bax")
				assert.Equal(len(data), 5)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			searcher := New(pluginDir)
			clusters, found := searcher.FindSchedulerClusters(context.Background(), tc.schedulerClusters, "128.168.1.0", "foo", tc.conditions)
			tc.expect(t, clusters, found)
		})
	}
}
