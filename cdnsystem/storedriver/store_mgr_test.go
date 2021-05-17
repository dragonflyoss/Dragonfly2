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

package storedriver

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

func TestStoreMgrTestSuite(t *testing.T) {
	suite.Run(t, new(StoreMgrTestSuite))
}

type StoreMgrTestSuite struct {
	suite.Suite
}

func (s *StoreMgrTestSuite) SetupSuite() {
	type args struct {
		name    string
		builder StorageBuilder
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "test1",
			args: args{
				name: "disk1",
				builder: func(conf interface{}) (Driver, error) {
					return nil, nil
				},
			},
		}, {
			name: "test2",
			args: args{
				name: "memory1",
				builder: func(conf interface{}) (Driver, error) {
					return nil, nil
				},
			},
		},
	}
	for _, tt := range tests {
		Register(tt.name, tt.args.builder)
	}
}

func (s *StoreMgrTestSuite) TestGet() {
	type args struct {
		name string
	}
	tests := []struct {
		name    string
		args    args
		want    *Store
		wantErr bool
	}{
		{
			name: "test1",
			args: args{name: "disk"},
			want: &Store{
				driverName: "disk",
				config:     nil,
				driver:     nil,
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		got, err := Get(tt.args.name)
		s.Equal(tt.wantErr, err != nil)
		s.Equal(tt.want, got)
	}
}
