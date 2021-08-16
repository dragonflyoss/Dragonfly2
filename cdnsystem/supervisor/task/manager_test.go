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
	"testing"

	"d7y.io/dragonfly/v2/cdnsystem/config"
	"d7y.io/dragonfly/v2/cdnsystem/supervisor/mock"
	"d7y.io/dragonfly/v2/cdnsystem/types"
	"d7y.io/dragonfly/v2/internal/idgen"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
)

func TestTaskManagerSuite(t *testing.T) {
	suite.Run(t, new(TaskManagerTestSuite))
}

type TaskManagerTestSuite struct {
	tm *Manager
	suite.Suite
}

func (suite *TaskManagerTestSuite) TestRegister() {
	dragonflyURL := "http://dragonfly.io.com?a=a&b=b&c=c"
	ctrl := gomock.NewController(suite.T())
	cdnMgr := mock.NewMockCDNMgr(ctrl)
	progressMgr := mock.NewMockSeedProgressMgr(ctrl)
	progressMgr.EXPECT().SetTaskMgr(gomock.Any()).Times(1)
	tm, err := NewManager(config.New(), cdnMgr, progressMgr)
	suite.Nil(err)
	suite.NotNil(tm)
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
		{
			name: "register_md5",
			args: args{
				ctx: context.Background(),
				req: &types.TaskRegisterRequest{
					URL:    dragonflyURL,
					TaskID: idgen.TaskID(dragonflyURL, &base.UrlMeta{Filter: "a&b", Tag: "dragonfly", Digest: "md5:f1e2488bba4d1267948d9e2f7008571c"}),
					Digest: "md5:f1e2488bba4d1267948d9e2f7008571c",
					Filter: []string{"a", "b"},
					Header: nil,
				},
			},
			wantPieceChan: nil,
			wantErr:       false,
		},
		{
			name: "register_sha256",
			args: args{
				ctx: context.Background(),
				req: &types.TaskRegisterRequest{
					URL:    dragonflyURL,
					TaskID: idgen.TaskID(dragonflyURL, &base.UrlMeta{Filter: "a&b", Tag: "dragonfly", Digest: "sha256:b9907b9a5ba2b0223868c201b9addfe2ec1da1b90325d57c34f192966b0a68c5"}),
					Digest: "sha256:b9907b9a5ba2b0223868c201b9addfe2ec1da1b90325d57c34f192966b0a68c5",
					Filter: []string{"a", "b"},
					Header: nil,
				},
			},
			wantPieceChan: nil,
			wantErr:       false,
		},
	}
	for _, tt := range tests {
		suite.Run(tt.name, func() {
			//gotPieceChan, err := tm.Register(tt.args.ctx, tt.args.req)
			//
			//if (err != nil) != tt.wantErr {
			//	suite.T().Errorf("Register() error = %v, wantErr %v", err, tt.wantErr)
			//	return
			//}
			//if !reflect.DeepEqual(gotPieceChan, tt.wantPieceChan) {
			//	suite.T().Errorf("Register() gotPieceChan = %v, want %v", gotPieceChan, tt.wantPieceChan)
			//}
		})
	}
}
