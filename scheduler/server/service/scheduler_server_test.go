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

package service

import (
	"context"
	"reflect"
	"testing"

	"d7y.io/dragonfly/v2/pkg/rpc/base"
	"d7y.io/dragonfly/v2/pkg/rpc/scheduler"
	"d7y.io/dragonfly/v2/scheduler/config"
	"d7y.io/dragonfly/v2/scheduler/core"
	"d7y.io/dragonfly/v2/scheduler/types"
)

func TestNewSchedulerServer(t *testing.T) {
	type args struct {
		cfg       *config.SchedulerConfig
		dynConfig config.DynconfigInterface
	}
	tests := []struct {
		name    string
		args    args
		want    *SchedulerServer
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewSchedulerServer(tt.args.cfg, tt.args.dynConfig)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewSchedulerServer() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewSchedulerServer() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSchedulerServer_LeaveTask(t *testing.T) {
	type fields struct {
		service *core.SchedulerService
	}
	type args struct {
		ctx    context.Context
		target *scheduler.PeerTarget
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
			s := &SchedulerServer{
				service: tt.fields.service,
			}
			if err := s.LeaveTask(tt.args.ctx, tt.args.target); (err != nil) != tt.wantErr {
				t.Errorf("LeaveTask() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestSchedulerServer_RegisterPeerTask(t *testing.T) {
	type fields struct {
		service *core.SchedulerService
	}
	type args struct {
		ctx     context.Context
		request *scheduler.PeerTaskRequest
	}
	tests := []struct {
		name     string
		fields   fields
		args     args
		wantResp *scheduler.RegisterResult
		wantErr  bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &SchedulerServer{
				service: tt.fields.service,
			}
			gotResp, err := s.RegisterPeerTask(tt.args.ctx, tt.args.request)
			if (err != nil) != tt.wantErr {
				t.Errorf("RegisterPeerTask() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(gotResp, tt.wantResp) {
				t.Errorf("RegisterPeerTask() gotResp = %v, want %v", gotResp, tt.wantResp)
			}
		})
	}
}

func TestSchedulerServer_ReportPeerResult(t *testing.T) {
	type fields struct {
		service *core.SchedulerService
	}
	type args struct {
		ctx    context.Context
		result *scheduler.PeerResult
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
			s := &SchedulerServer{
				service: tt.fields.service,
			}
			if err := s.ReportPeerResult(tt.args.ctx, tt.args.result); (err != nil) != tt.wantErr {
				t.Errorf("ReportPeerResult() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestSchedulerServer_ReportPieceResult(t *testing.T) {
	type fields struct {
		service *core.SchedulerService
	}
	type args struct {
		stream scheduler.Scheduler_ReportPieceResultServer
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
			s := &SchedulerServer{
				service: tt.fields.service,
			}
			if err := s.ReportPieceResult(tt.args.stream); (err != nil) != tt.wantErr {
				t.Errorf("ReportPieceResult() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_getTaskSizeScope(t *testing.T) {
	type args struct {
		task *types.Task
	}
	tests := []struct {
		name string
		args args
		want base.SizeScope
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := getTaskSizeScope(tt.args.task); got != tt.want {
				t.Errorf("getTaskSizeScope() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_validateParams(t *testing.T) {
	type args struct {
		req *scheduler.PeerTaskRequest
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
			if err := validateParams(tt.args.req); (err != nil) != tt.wantErr {
				t.Errorf("validateParams() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
