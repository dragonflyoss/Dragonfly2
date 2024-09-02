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

	machineryv1tasks "github.com/RichardKnop/machinery/v1/tasks"
	testifyassert "github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"

	"d7y.io/dragonfly/v2/internal/job"
	"d7y.io/dragonfly/v2/manager/job/mocks"
	"d7y.io/dragonfly/v2/manager/models"
	"d7y.io/dragonfly/v2/manager/types"
	"d7y.io/dragonfly/v2/scheduler/resource"
)

func TestDeleteTask(t *testing.T) {
	tests := []struct {
		name       string
		setupMocks func(mockTask *mocks.MockTask)
		ctx        context.Context
		schedulers []models.Scheduler
		args       types.DeleteTaskArgs
		expect     func(t *testing.T, group *machineryv1tasks.Group, responses map[string]*job.DeleteTaskResponse, err error)
	}{
		{
			name: "DeleteTask succeeds",
			setupMocks: func(mockTask *mocks.MockTask) {
				expectedGroup := &machineryv1tasks.Group{
					GroupUUID: "test-group-uuid",
				}
				expectedResponses := map[string]*job.DeleteTaskResponse{
					"scheduler1": {
						SuccessPeers: []*job.DeletePeerResponse{
							{
								Peer:        &resource.Peer{ID: "peer1"},
								Description: "Deleted successfully",
							},
						},
						FailurePeers: []*job.DeletePeerResponse{},
					},
					"scheduler2": {
						SuccessPeers: []*job.DeletePeerResponse{},
						FailurePeers: []*job.DeletePeerResponse{
							{
								Peer:        &resource.Peer{ID: "peer2"},
								Description: "Failed to delete",
							},
						},
					},
				}
				mockTask.EXPECT().DeleteTask(gomock.Any(), gomock.Any(), gomock.Any()).Return(expectedGroup, expectedResponses, nil)
			},
			ctx: context.TODO(),
			schedulers: []models.Scheduler{
				{Hostname: "scheduler1"},
				{Hostname: "scheduler2"},
			},
			args: types.DeleteTaskArgs{
				TaskID: "test-task-id",
			},
			expect: func(t *testing.T, group *machineryv1tasks.Group, responses map[string]*job.DeleteTaskResponse, err error) {
				assert := testifyassert.New(t)
				assert.NoError(err)
				assert.Equal("test-group-uuid", group.GroupUUID)
				assert.Equal("peer1", responses["scheduler1"].SuccessPeers[0].Peer.ID)
				assert.Equal("peer2", responses["scheduler2"].FailurePeers[0].Peer.ID)
			},
		},
		{
			name: "DeleteTask fails",
			setupMocks: func(mockTask *mocks.MockTask) {
				mockTask.EXPECT().DeleteTask(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil, errors.New("delete task error"))
			},
			ctx: context.TODO(),
			schedulers: []models.Scheduler{
				{Hostname: "scheduler1"},
			},
			args: types.DeleteTaskArgs{
				TaskID: "test-task-id",
			},
			expect: func(t *testing.T, group *machineryv1tasks.Group, responses map[string]*job.DeleteTaskResponse, err error) {
				assert := testifyassert.New(t)
				assert.Error(err)
				assert.Nil(group)
				assert.Nil(responses)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockTask := mocks.NewMockTask(ctrl)
			tc.setupMocks(mockTask)

			group, responses, err := mockTask.DeleteTask(tc.ctx, tc.schedulers, tc.args)
			tc.expect(t, group, responses, err)
		})
	}
}

func TestGetTask(t *testing.T) {
	tests := []struct {
		name       string
		setupMocks func(mockTask *mocks.MockTask)
		ctx        context.Context
		schedulers []models.Scheduler
		args       types.GetTaskArgs
		expect     func(t *testing.T, group *machineryv1tasks.Group, responses map[string]*job.GetTaskResponse, err error)
	}{
		{
			name: "GetTask succeeds",
			setupMocks: func(mockTask *mocks.MockTask) {
				expectedGroup := &machineryv1tasks.Group{
					GroupUUID: "test-group-uuid",
				}
				expectedResponses := map[string]*job.GetTaskResponse{
					"scheduler1": {
						Peers: []*resource.Peer{
							{ID: "peer1"},
							{ID: "peer2"},
						},
					},
					"scheduler2": {
						Peers: []*resource.Peer{
							{ID: "peer3"},
							{ID: "peer4"},
						},
					},
				}
				mockTask.EXPECT().GetTask(gomock.Any(), gomock.Any(), gomock.Any()).Return(expectedGroup, expectedResponses, nil)
			},
			ctx: context.TODO(),
			schedulers: []models.Scheduler{
				{Hostname: "scheduler1"},
				{Hostname: "scheduler2"},
			},
			args: types.GetTaskArgs{
				TaskID: "test-task-id",
			},
			expect: func(t *testing.T, group *machineryv1tasks.Group, responses map[string]*job.GetTaskResponse, err error) {
				assert := testifyassert.New(t)
				assert.NoError(err)
				assert.Equal("test-group-uuid", group.GroupUUID)
				assert.Equal("peer1", responses["scheduler1"].Peers[0].ID)
				assert.Equal("peer3", responses["scheduler2"].Peers[0].ID)
			},
		},
		{
			name: "GetTask fails",
			setupMocks: func(mockTask *mocks.MockTask) {
				mockTask.EXPECT().GetTask(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil, errors.New("get task error"))
			},
			ctx: context.TODO(),
			schedulers: []models.Scheduler{
				{Hostname: "scheduler1"},
			},
			args: types.GetTaskArgs{
				TaskID: "test-task-id",
			},
			expect: func(t *testing.T, group *machineryv1tasks.Group, responses map[string]*job.GetTaskResponse, err error) {
				assert := testifyassert.New(t)
				assert.Error(err)
				assert.Nil(group)
				assert.Nil(responses)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockTask := mocks.NewMockTask(ctrl)
			tc.setupMocks(mockTask)

			group, responses, err := mockTask.GetTask(tc.ctx, tc.schedulers, tc.args)
			tc.expect(t, group, responses, err)
		})
	}
}
