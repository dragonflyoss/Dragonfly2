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

package job

import (
	"context"
	"errors"
	"testing"

	"github.com/RichardKnop/machinery/v1"
	"github.com/stretchr/testify/assert"

	"d7y.io/dragonfly/v2/internal/job"
	"d7y.io/dragonfly/v2/manager/models"
	"d7y.io/dragonfly/v2/manager/types"
)

func TestTask_CreateGetTask(t *testing.T) {
	task := newTask(&job.Job{Server: &machinery.Server{}})

	tests := []struct {
		name       string
		schedulers []models.Scheduler
		args       types.GetTaskArgs
		expect     func(t *testing.T, g *job.GroupJobState, e error)
	}{
		{
			name: "queue retrieval error",
			schedulers: []models.Scheduler{
				{
					SchedulerClusterID: 0,
					Hostname:           "",
				},
			},
			args: types.GetTaskArgs{
				TaskID: "e7a7132d133ab769af4b64c54c0ea4572d2089142d9f1c04203572697439f467",
			},
			expect: func(t *testing.T, g *job.GroupJobState, e error) {
				assert := assert.New(t)
				assert.Error(errors.New("empty cluster id config is not specified"), e)
			},
		},
		{
			name: "send group failure",
			schedulers: []models.Scheduler{
				{
					SchedulerClusterID: 1,
					Hostname:           "hostname",
				},
			},
			args: types.GetTaskArgs{
				TaskID: "55697b6298a6ee5118443f23c4f4532666421361d31d4b1e822d2480c42eda0c",
			},
			expect: func(t *testing.T, g *job.GroupJobState, e error) {
				assert := assert.New(t)
				assert.Error(errors.New("Result backend required"), e)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			res, err := task.CreateGetTask(context.TODO(), tc.schedulers, tc.args)
			tc.expect(t, res, err)
		})
	}
}

func TestTask_CreateDeleteTask(t *testing.T) {
	tk := newTask(&job.Job{Server: &machinery.Server{}})

	tests := []struct {
		name       string
		schedulers []models.Scheduler
		args       types.DeleteTaskArgs
		expect     func(t *testing.T, g *job.GroupJobState, e error)
	}{
		{
			name: "queue retrieval error",
			schedulers: []models.Scheduler{
				{
					SchedulerClusterID: 0,
					Hostname:           "",
				},
			},
			args: types.DeleteTaskArgs{
				TaskID: "e7a7132d133ab769af4b64c54c0ea4572d2089142d9f1c04203572697439f467",
			},
			expect: func(t *testing.T, g *job.GroupJobState, e error) {
				assert := assert.New(t)
				assert.Error(errors.New("empty cluster id config is not specified"), e)
			},
		},
		{
			name: "send group failure",
			schedulers: []models.Scheduler{
				{
					SchedulerClusterID: 1,
					Hostname:           "hostname",
				},
			},
			args: types.DeleteTaskArgs{
				TaskID: "55697b6298a6ee5118443f23c4f4532666421361d31d4b1e822d2480c42eda0c",
			},
			expect: func(t *testing.T, g *job.GroupJobState, e error) {
				assert := assert.New(t)
				assert.Error(errors.New("Result backend required"), e)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			res, err := tk.CreateDeleteTask(context.TODO(), tc.schedulers, tc.args)
			tc.expect(t, res, err)
		})
	}
}
