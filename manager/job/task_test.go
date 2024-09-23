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
	tk := newTask(&job.Job{Server: &machinery.Server{}})

	tests := []struct {
		name       string
		schedulers []models.Scheduler
		args       types.GetTaskArgs
		expect     *job.GroupJobState
		wantErr    error
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
				TaskID: "valid-task-id",
			},
			expect:  nil,
			wantErr: errors.New("empty cluster id config is not specified"),
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
				TaskID: "valid-task-id",
			},
			expect:  nil,
			wantErr: errors.New("Result backend required"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res, err := tk.CreateGetTask(context.TODO(), tt.schedulers, tt.args)

			if tt.wantErr != nil {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, res)
				assert.Equal(t, tt.expect.State, res.State)
			}
		})
	}
}

func TestTask_CreateDeleteTask(t *testing.T) {
	tk := newTask(&job.Job{Server: &machinery.Server{}})

	tests := []struct {
		name       string
		schedulers []models.Scheduler
		args       types.DeleteTaskArgs
		expect     *job.GroupJobState
		wantErr    error
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
				TaskID: "valid-task-id",
			},
			expect:  nil,
			wantErr: errors.New("empty cluster id config is not specified"),
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
				TaskID: "valid-task-id",
			},
			expect:  nil,
			wantErr: errors.New("Result backend required"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res, err := tk.CreateDeleteTask(context.TODO(), tt.schedulers, tt.args)

			if tt.wantErr != nil {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, res)
				assert.Equal(t, tt.expect.State, res.State)
			}
		})
	}
}
