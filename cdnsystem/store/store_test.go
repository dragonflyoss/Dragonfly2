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

package store

import (
	"context"
	"fmt"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/config"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/plugins"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/store/disk"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/store/hybrid"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/store/memory"
	"github.com/dragonflyoss/Dragonfly2/pkg/util/fileutils"
	"github.com/stretchr/testify/suite"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
)

func Test(t *testing.T) {
	suite.Run(t, new(StoreSuite))
}

type StoreSuite struct {
	workHome string
	mgr      *Manager
	suite.Suite
}


func (s *StoreSuite) SetupSuite() {
	s.workHome, _ = ioutil.TempDir("/tmp", "cdn-storageDriver-StoreTestSuite-")
	pluginProps := map[config.PluginType][]*config.PluginProperties{
		config.StoragePlugin: {
			&config.PluginProperties{
				Name:    disk.StorageDriver,
				Enabled: true,
				Config:  "baseDir: " + filepath.Join(s.workHome, "repo"),
			},
			&config.PluginProperties{
				Name:    memory.StorageDriver,
				Enabled: true,
				Config:  "baseDir: " + filepath.Join(s.workHome, "repo"),
			},
			&config.PluginProperties{
				Name:    hybrid.StorageDriver,
				Enabled: true,
				Config:  "baseDir: " + filepath.Join(s.workHome, "repo"),
			},
		},
	}
	cfg := &config.Config{
		Plugins: pluginProps,
	}
	plugins.Initialize(cfg)
	mgr, err := NewManager(cfg)
	s.Nil(err)
	s.NotNil(mgr)
}

func (s *StoreSuite) TearDownSuite() {
	if s.workHome != "" {
		if err := os.RemoveAll(s.workHome); err != nil {
			fmt.Printf("remove path:%s error", s.workHome)
		}
	}
}

func (s *StoreSuite) TestCheckGetRaw() {
	type args struct {
		raw        *Raw
		fileLength int64
	}
	cases := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "test1",
			args: args{
				raw: &Raw{},
				fileLength: 23,
			},
			wantErr: false,
		},
		{
			name: "test2",
			args: args{
				raw: &Raw{},
				fileLength: 23,
			},
			wantErr: true,
		},
	}
	for _, v := range cases {
		if err := CheckGetRaw(v.args.raw, v.args.fileLength); (err != nil) != v.wantErr {
			s.Errorf(err, "CheckGetRaw() error = %v, wantErr %v", err, v.wantErr)
		}

	}
}

func TestCheckPutRaw(t *testing.T) {
	type args struct {
		raw *Raw
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
			if err := CheckPutRaw(tt.args.raw); (err != nil) != tt.wantErr {
				t.Errorf("CheckPutRaw() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestManager_Get(t *testing.T) {
	type fields struct {
		cfg            *config.Config
		defaultStorage *Store
		mutex          sync.Mutex
	}
	type args struct {
		name string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *Store
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sm := &Manager{
				cfg:            tt.fields.cfg,
				defaultStorage: tt.fields.defaultStorage,
				mutex:          tt.fields.mutex,
			}
			got, err := sm.Get(tt.args.name)
			if (err != nil) != tt.wantErr {
				t.Errorf("Get() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Get() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestManager_getDefaultStorage(t *testing.T) {
	type fields struct {
		cfg            *config.Config
		defaultStorage *Store
		mutex          sync.Mutex
	}
	tests := []struct {
		name    string
		fields  fields
		want    *Store
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sm := &Manager{
				cfg:            tt.fields.cfg,
				defaultStorage: tt.fields.defaultStorage,
				mutex:          tt.fields.mutex,
			}
			got, err := sm.getDefaultStorage()
			if (err != nil) != tt.wantErr {
				t.Errorf("getDefaultStorage() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("getDefaultStorage() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNewManager(t *testing.T) {
	type args struct {
		cfg *config.Config
	}
	tests := []struct {
		name    string
		args    args
		want    *Manager
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewManager(tt.args.cfg)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewManager() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewManager() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNewStore(t *testing.T) {
	type args struct {
		name    string
		builder StorageBuilder
		cfg     string
	}
	tests := []struct {
		name    string
		args    args
		want    *Store
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewStore(tt.args.name, tt.args.builder, tt.args.cfg)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewStore() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewStore() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestRegister(t *testing.T) {
	type args struct {
		name    string
		builder StorageBuilder
	}
	tests := []struct {
		name string
		args args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
		})
	}
}

func TestStore_AppendBytes(t *testing.T) {
	type fields struct {
		driverName string
		config     interface{}
		driver     StorageDriver
	}
	type args struct {
		ctx  context.Context
		raw  *Raw
		data []byte
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
			s := &Store{
				driverName: tt.fields.driverName,
				config:     tt.fields.config,
				driver:     tt.fields.driver,
			}
			if err := s.AppendBytes(tt.args.ctx, tt.args.raw, tt.args.data); (err != nil) != tt.wantErr {
				t.Errorf("AppendBytes() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestStore_Get(t *testing.T) {
	type fields struct {
		driverName string
		config     interface{}
		driver     StorageDriver
	}
	type args struct {
		ctx context.Context
		raw *Raw
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    io.Reader
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Store{
				driverName: tt.fields.driverName,
				config:     tt.fields.config,
				driver:     tt.fields.driver,
			}
			got, err := s.Get(tt.args.ctx, tt.args.raw)
			if (err != nil) != tt.wantErr {
				t.Errorf("Get() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Get() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestStore_GetAvailSpace(t *testing.T) {
	type fields struct {
		driverName string
		config     interface{}
		driver     StorageDriver
	}
	type args struct {
		ctx context.Context
		raw *Raw
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    fileutils.Fsize
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Store{
				driverName: tt.fields.driverName,
				config:     tt.fields.config,
				driver:     tt.fields.driver,
			}
			got, err := s.GetAvailSpace(tt.args.ctx, tt.args.raw)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetAvailSpace() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("GetAvailSpace() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestStore_GetBytes(t *testing.T) {
	type fields struct {
		driverName string
		config     interface{}
		driver     StorageDriver
	}
	type args struct {
		ctx context.Context
		raw *Raw
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []byte
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Store{
				driverName: tt.fields.driverName,
				config:     tt.fields.config,
				driver:     tt.fields.driver,
			}
			got, err := s.GetBytes(tt.args.ctx, tt.args.raw)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetBytes() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetBytes() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestStore_Name(t *testing.T) {
	type fields struct {
		driverName string
		config     interface{}
		driver     StorageDriver
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Store{
				driverName: tt.fields.driverName,
				config:     tt.fields.config,
				driver:     tt.fields.driver,
			}
			if got := s.Name(); got != tt.want {
				t.Errorf("Name() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestStore_Put(t *testing.T) {
	type fields struct {
		driverName string
		config     interface{}
		driver     StorageDriver
	}
	type args struct {
		ctx  context.Context
		raw  *Raw
		data io.Reader
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
			s := &Store{
				driverName: tt.fields.driverName,
				config:     tt.fields.config,
				driver:     tt.fields.driver,
			}
			if err := s.Put(tt.args.ctx, tt.args.raw, tt.args.data); (err != nil) != tt.wantErr {
				t.Errorf("Put() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestStore_PutBytes(t *testing.T) {
	type fields struct {
		driverName string
		config     interface{}
		driver     StorageDriver
	}
	type args struct {
		ctx  context.Context
		raw  *Raw
		data []byte
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
			s := &Store{
				driverName: tt.fields.driverName,
				config:     tt.fields.config,
				driver:     tt.fields.driver,
			}
			if err := s.PutBytes(tt.args.ctx, tt.args.raw, tt.args.data); (err != nil) != tt.wantErr {
				t.Errorf("PutBytes() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestStore_Remove(t *testing.T) {
	type fields struct {
		driverName string
		config     interface{}
		driver     StorageDriver
	}
	type args struct {
		ctx context.Context
		raw *Raw
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
			s := &Store{
				driverName: tt.fields.driverName,
				config:     tt.fields.config,
				driver:     tt.fields.driver,
			}
			if err := s.Remove(tt.args.ctx, tt.args.raw); (err != nil) != tt.wantErr {
				t.Errorf("Remove() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestStore_Stat(t *testing.T) {
	type fields struct {
		driverName string
		config     interface{}
		driver     StorageDriver
	}
	type args struct {
		ctx context.Context
		raw *Raw
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *StorageInfo
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Store{
				driverName: tt.fields.driverName,
				config:     tt.fields.config,
				driver:     tt.fields.driver,
			}
			got, err := s.Stat(tt.args.ctx, tt.args.raw)
			if (err != nil) != tt.wantErr {
				t.Errorf("Stat() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Stat() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestStore_Type(t *testing.T) {
	type fields struct {
		driverName string
		config     interface{}
		driver     StorageDriver
	}
	tests := []struct {
		name   string
		fields fields
		want   config.PluginType
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Store{
				driverName: tt.fields.driverName,
				config:     tt.fields.config,
				driver:     tt.fields.driver,
			}
			if got := s.Type(); got != tt.want {
				t.Errorf("Type() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestStore_Walk(t *testing.T) {
	type fields struct {
		driverName string
		config     interface{}
		driver     StorageDriver
	}
	type args struct {
		ctx context.Context
		raw *Raw
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
			s := &Store{
				driverName: tt.fields.driverName,
				config:     tt.fields.config,
				driver:     tt.fields.driver,
			}
			if err := s.Walk(tt.args.ctx, tt.args.raw); (err != nil) != tt.wantErr {
				t.Errorf("Walk() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_checkEmptyKey(t *testing.T) {
	type args struct {
		raw *Raw
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
			if err := checkEmptyKey(tt.args.raw); (err != nil) != tt.wantErr {
				t.Errorf("checkEmptyKey() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}