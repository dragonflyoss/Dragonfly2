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

package progress

import (
	"context"
	"reflect"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"

	taskMock "d7y.io/dragonfly/v2/cdn/supervisor/mocks/task"
	"d7y.io/dragonfly/v2/cdn/supervisor/task"
	"d7y.io/dragonfly/v2/pkg/synclock"
)

func TestProgressManagerSuite(t *testing.T) {
	suite.Run(t, new(ProgressManagerTestSuite))
}

type ProgressManagerTestSuite struct {
	manager *manager
	suite.Suite
}

func (suite *ProgressManagerTestSuite) SetupSuite() {
	ctrl := gomock.NewController(suite.T())
	taskManager := taskMock.NewMockManager(ctrl)
	manager, err := newManager(taskManager)
	suite.Nil(err)
	suite.manager = manager
}

func (suite *ProgressManagerTestSuite) Test_manager_PublishPiece() {
	type fields struct {
		mu               *synclock.LockerPool
		taskManager      task.Manager
		seedTaskSubjects map[string]*publisher
	}
	type args struct {
		ctx    context.Context
		taskID string
		record *task.PieceInfo
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		//{}
	}
	for _, tt := range tests {
		suite.Run(tt.name, func() {
			if err := suite.manager.PublishPiece(tt.args.ctx, tt.args.taskID, tt.args.record); (err != nil) != tt.wantErr {
				suite.T().Errorf("PublishPiece() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func (suite *ProgressManagerTestSuite) Test_manager_PublishTask() {
	type fields struct {
		mu               *synclock.LockerPool
		taskManager      task.Manager
		seedTaskSubjects map[string]*publisher
	}
	type args struct {
		ctx      context.Context
		taskID   string
		seedTask *task.SeedTask
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		suite.Run(tt.name, func() {
			if err := suite.manager.PublishTask(tt.args.ctx, tt.args.taskID, tt.args.seedTask); (err != nil) != tt.wantErr {
				suite.T().Errorf("PublishTask() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func (suite *ProgressManagerTestSuite) Test_manager_WatchSeedProgress() {
	type fields struct {
		mu               *synclock.LockerPool
		taskManager      task.Manager
		seedTaskSubjects map[string]*publisher
	}
	type args struct {
		ctx    context.Context
		taskID string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    <-chan *task.PieceInfo
		wantErr bool
	}{
		//{},
	}
	for _, tt := range tests {
		suite.Run(tt.name, func() {
			got, err := suite.manager.WatchSeedProgress(tt.args.ctx, "", tt.args.taskID)
			if (err != nil) != tt.wantErr {
				suite.T().Errorf("WatchSeedProgress() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				suite.T().Errorf("WatchSeedProgress() got = %v, want %v", got, tt.want)
			}
		})
	}
}
