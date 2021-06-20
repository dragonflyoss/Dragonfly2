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

package task

import (
	"context"
	"reflect"
	"testing"

	"d7y.io/dragonfly/v2/cdnsystem/types"
	"d7y.io/dragonfly/v2/pkg/structure/syncmap"
	"github.com/stretchr/testify/suite"
)

func TestTaskManagerSuite(t *testing.T) {
	suite.Run(t, new(TaskManagerTestSuite))
}

type TaskManagerTestSuite struct {
	tm *Manager
	suite.Suite
}

func (suite *TaskManagerTestSuite) TestManager_Delete() {
	type args struct {
		taskID string
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		suite.Run(tt.name, func() {
			if err := suite.tm.Delete(tt.args.taskID); (err != nil) != tt.wantErr {
				suite.T().Errorf("Delete() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func (suite *TaskManagerTestSuite) TestManager_GC() {
	tests := []struct {
		name    string
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		suite.Run(tt.name, func() {
			if err := suite.tm.GC(); (err != nil) != tt.wantErr {
				suite.T().Errorf("GC() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func (suite *TaskManagerTestSuite) TestManager_Get() {
	type args struct {
		taskID string
	}
	tests := []struct {
		name    string
		args    args
		want    *types.SeedTask
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		suite.Run(tt.name, func() {
			got, err := suite.tm.Get(tt.args.taskID)
			if (err != nil) != tt.wantErr {
				suite.T().Errorf("Get() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				suite.T().Errorf("Get() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func (suite *TaskManagerTestSuite) TestManager_GetAccessTime() {
	tests := []struct {
		name    string
		want    *syncmap.SyncMap
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		suite.Run(tt.name, func() {
			got, err := suite.tm.GetAccessTime()
			if (err != nil) != tt.wantErr {
				suite.T().Errorf("GetAccessTime() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				suite.T().Errorf("GetAccessTime() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func (suite *TaskManagerTestSuite) TestManager_GetPieces() {
	type args struct {
		ctx    context.Context
		taskID string
	}
	tests := []struct {
		name       string
		args       args
		wantPieces []*types.SeedPiece
		wantErr    bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		suite.Run(tt.name, func() {
			gotPieces, err := suite.tm.GetPieces(tt.args.ctx, tt.args.taskID)
			if (err != nil) != tt.wantErr {
				suite.T().Errorf("GetPieces() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(gotPieces, tt.wantPieces) {
				suite.T().Errorf("GetPieces() gotPieces = %v, want %v", gotPieces, tt.wantPieces)
			}
		})
	}
}

func (suite *TaskManagerTestSuite) TestManager_Register() {
	type args struct {
		ctx context.Context
		req *types.TaskRegisterRequest
	}
	tests := []struct {
		name          string
		args          args
		wantPieceChan <-chan *types.SeedPiece
		wantErr       bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		suite.Run(tt.name, func() {
			gotPieceChan, err := suite.tm.Register(tt.args.ctx, tt.args.req)
			if (err != nil) != tt.wantErr {
				suite.T().Errorf("Register() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(gotPieceChan, tt.wantPieceChan) {
				suite.T().Errorf("Register() gotPieceChan = %v, want %v", gotPieceChan, tt.wantPieceChan)
			}
		})
	}
}

func (suite *TaskManagerTestSuite) TestManager_getTask(t *testing.T) {
	type args struct {
		taskID string
	}
	tests := []struct {
		name    string
		args    args
		want    *types.SeedTask
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		suite.Run(tt.name, func() {
			got, err := suite.tm.getTask(tt.args.taskID)
			if (err != nil) != tt.wantErr {
				t.Errorf("getTask() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("getTask() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func (suite *TaskManagerTestSuite) TestManager_triggerCdnSyncAction() {
	type args struct {
		ctx  context.Context
		task *types.SeedTask
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		suite.Run(tt.name, func() {
			if err := suite.tm.triggerCdnSyncAction(tt.args.ctx, tt.args.task); (err != nil) != tt.wantErr {
				suite.T().Errorf("triggerCdnSyncAction() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
