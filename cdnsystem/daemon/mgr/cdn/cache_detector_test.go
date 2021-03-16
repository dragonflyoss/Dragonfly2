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

package cdn

import (
	"context"
	"d7y.io/dragonfly/v2/cdnsystem/daemon/mgr/cdn/storage"
	"d7y.io/dragonfly/v2/cdnsystem/source"
	"d7y.io/dragonfly/v2/cdnsystem/types"
	"reflect"
	"testing"
)

func Test_cacheDetector_detectCache(t *testing.T) {
	type fields struct {
		cacheStore      storage.StorageMgr
		metaDataManager *metaDataManager
		resourceClient  source.ResourceClient
	}
	type args struct {
		ctx  context.Context
		task *types.SeedTask
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *cacheResult
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cd := &cacheDetector{
				cacheStore:      tt.fields.cacheStore,
				metaDataManager: tt.fields.metaDataManager,
				resourceClient:  tt.fields.resourceClient,
			}
			got, err := cd.detectCache(tt.args.ctx, tt.args.task)
			if (err != nil) != tt.wantErr {
				t.Errorf("detectCache() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("detectCache() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_cacheDetector_doDetect(t *testing.T) {
	type fields struct {
		cacheStore      storage.StorageMgr
		metaDataManager *metaDataManager
		resourceClient  source.ResourceClient
	}
	type args struct {
		ctx  context.Context
		task *types.SeedTask
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *cacheResult
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cd := &cacheDetector{
				cacheStore:      tt.fields.cacheStore,
				metaDataManager: tt.fields.metaDataManager,
				resourceClient:  tt.fields.resourceClient,
			}
			got, err := cd.doDetect(tt.args.ctx, tt.args.task)
			if (err != nil) != tt.wantErr {
				t.Errorf("doDetect() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("doDetect() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_cacheDetector_parseByReadFile(t *testing.T) {
	type fields struct {
		cacheStore      storage.StorageMgr
		metaDataManager *metaDataManager
		resourceClient  source.ResourceClient
	}
	type args struct {
		ctx      context.Context
		taskID   string
		metaData *storage.FileMetaData
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *cacheResult
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cd := &cacheDetector{
				cacheStore:      tt.fields.cacheStore,
				metaDataManager: tt.fields.metaDataManager,
				resourceClient:  tt.fields.resourceClient,
			}
			got, err := cd.parseByReadFile(tt.args.ctx, tt.args.taskID, tt.args.metaData)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseByReadFile() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("parseByReadFile() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_cacheDetector_parseByReadMetaFile(t *testing.T) {
	type fields struct {
		cacheStore      storage.StorageMgr
		metaDataManager *metaDataManager
		resourceClient  source.ResourceClient
	}
	type args struct {
		ctx          context.Context
		taskID       string
		fileMetaData *storage.FileMetaData
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *cacheResult
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cd := &cacheDetector{
				cacheStore:      tt.fields.cacheStore,
				metaDataManager: tt.fields.metaDataManager,
				resourceClient:  tt.fields.resourceClient,
			}
			got, err := cd.parseByReadMetaFile(tt.args.ctx, tt.args.taskID, tt.args.fileMetaData)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseByReadMetaFile() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("parseByReadMetaFile() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_cacheDetector_resetRepo(t *testing.T) {
	type fields struct {
		cacheStore      storage.StorageMgr
		metaDataManager *metaDataManager
		resourceClient  source.ResourceClient
	}
	type args struct {
		ctx  context.Context
		task *types.SeedTask
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *storage.FileMetaData
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cd := &cacheDetector{
				cacheStore:      tt.fields.cacheStore,
				metaDataManager: tt.fields.metaDataManager,
				resourceClient:  tt.fields.resourceClient,
			}
			got, err := cd.resetRepo(tt.args.ctx, tt.args.task)
			if (err != nil) != tt.wantErr {
				t.Errorf("resetRepo() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("resetRepo() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_checkSameFile(t *testing.T) {
	type args struct {
		task     *types.SeedTask
		metaData *storage.FileMetaData
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
			if err := checkSameFile(tt.args.task, tt.args.metaData); (err != nil) != tt.wantErr {
				t.Errorf("checkSameFile() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_newCacheDetector(t *testing.T) {
	type args struct {
		cacheStore      storage.StorageMgr
		metaDataManager *metaDataManager
		resourceClient  source.ResourceClient
	}
	tests := []struct {
		name string
		args args
		want *cacheDetector
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := newCacheDetector(tt.args.cacheStore, tt.args.metaDataManager, tt.args.resourceClient); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("newCacheDetector() = %v, want %v", got, tt.want)
			}
		})
	}
}
