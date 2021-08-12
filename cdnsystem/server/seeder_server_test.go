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

package server

import (
	"context"
	"reflect"
	"testing"

	"d7y.io/dragonfly/v2/cdnsystem/config"
	"d7y.io/dragonfly/v2/cdnsystem/supervisor"
	"d7y.io/dragonfly/v2/cdnsystem/types"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	"d7y.io/dragonfly/v2/pkg/rpc/cdnsystem"
)

func TestCdnSeedServer_GetPieceTasks(t *testing.T) {
	type fields struct {
		taskMgr supervisor.SeedTaskMgr
		cfg     *config.Config
	}
	type args struct {
		ctx context.Context
		req *base.PieceTaskRequest
	}
	tests := []struct {
		name            string
		fields          fields
		args            args
		wantPiecePacket *base.PiecePacket
		wantErr         bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			css := &CdnSeedServer{
				taskMgr: tt.fields.taskMgr,
				cfg:     tt.fields.cfg,
			}
			gotPiecePacket, err := css.GetPieceTasks(tt.args.ctx, tt.args.req)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetPieceTasks() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(gotPiecePacket, tt.wantPiecePacket) {
				t.Errorf("GetPieceTasks() gotPiecePacket = %v, want %v", gotPiecePacket, tt.wantPiecePacket)
			}
		})
	}
}

func TestCdnSeedServer_ObtainSeeds(t *testing.T) {
	type fields struct {
		taskMgr supervisor.SeedTaskMgr
		cfg     *config.Config
	}
	type args struct {
		ctx context.Context
		req *cdnsystem.SeedRequest
		psc chan<- *cdnsystem.PieceSeed
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
		t.Run(tt.name, func(t *testing.T) {
			css := &CdnSeedServer{
				taskMgr: tt.fields.taskMgr,
				cfg:     tt.fields.cfg,
			}
			if err := css.ObtainSeeds(tt.args.ctx, tt.args.req, tt.args.psc); (err != nil) != tt.wantErr {
				t.Errorf("ObtainSeeds() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestNewCdnSeedServer(t *testing.T) {
	type args struct {
		cfg     *config.Config
		taskMgr supervisor.SeedTaskMgr
	}
	tests := []struct {
		name    string
		args    args
		want    *CdnSeedServer
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewCdnSeedServer(tt.args.cfg, tt.args.taskMgr)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewCdnSeedServer() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewCdnSeedServer() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_checkPieceTasksRequestParams(t *testing.T) {
	type args struct {
		req *base.PieceTaskRequest
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := checkPieceTasksRequestParams(tt.args.req); (err != nil) != tt.wantErr {
				t.Errorf("checkPieceTasksRequestParams() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_checkSeedRequestParams(t *testing.T) {
	type args struct {
		req *cdnsystem.SeedRequest
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := checkSeedRequestParams(tt.args.req); (err != nil) != tt.wantErr {
				t.Errorf("checkSeedRequestParams() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_constructRegisterRequest(t *testing.T) {
	type args struct {
		req *cdnsystem.SeedRequest
	}
	tests := []struct {
		name    string
		args    args
		want    *types.TaskRegisterRequest
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := constructRegisterRequest(tt.args.req)
			if (err != nil) != tt.wantErr {
				t.Errorf("constructRegisterRequest() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("constructRegisterRequest() got = %v, want %v", got, tt.want)
			}
		})
	}
}
