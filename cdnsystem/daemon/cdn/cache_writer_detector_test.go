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
	"bufio"
	"context"
	"fmt"
	"hash"
	"io"
	"io/ioutil"
	"os"
	"reflect"
	"strings"
	"testing"

	"d7y.io/dragonfly/v2/cdnsystem/config"
	"d7y.io/dragonfly/v2/cdnsystem/daemon/cdn/storage"
	"d7y.io/dragonfly/v2/cdnsystem/daemon/progress"
	"d7y.io/dragonfly/v2/cdnsystem/plugins"
	"d7y.io/dragonfly/v2/cdnsystem/types"
	"github.com/stretchr/testify/suite"
)

func TestCacheWriterAndDetectorSuite(t *testing.T) {
	suite.Run(t, new(CacheWriterAndDetectorTestSuite))
}

type CacheWriterAndDetectorTestSuite struct {
	workHome string
	detector *cacheDetector
	writer   *cacheWriter
	suite.Suite
}

func (suite *CacheWriterAndDetectorTestSuite) SetupSuite() {
	suite.workHome, _ = ioutil.TempDir("/tmp", "cdn-CacheWriterDetectorTestSuite-")
	plugins.Initialize(config.NewDefaultPlugins())
	storeMgr, ok := storage.Get(config.DefaultStorageMode)
	if !ok {
		suite.Failf("failed to get storage mode %s", config.DefaultStorageMode)
	}
	cacheDataManager := newCacheDataManager(storeMgr)
	progressMgr, _ := progress.NewManager()
	cdnReporter := newReporter(progressMgr)
	suite.writer = newCacheWriter(cdnReporter, cacheDataManager)
	suite.detector = newCacheDetector(cacheDataManager)
}

func (suite *CacheWriterAndDetectorTestSuite) TeardownSuite() {
	if suite.workHome != "" {
		if err := os.RemoveAll(suite.workHome); err != nil {
			fmt.Printf("remove path: %s error", suite.workHome)
		}
	}
}

func (suite *CacheWriterAndDetectorTestSuite) TestStartWriter() {
	content, err := ioutil.ReadFile("../../testdata/cdn/go.html")
	suite.Nil(err)
	contentLen := int64(len(content))
	type args struct {
		reader       io.Reader
		task         *types.SeedTask
		detectResult *cacheResult
	}

	tests := []struct {
		name    string
		args    args
		result  *downloadMetadata
		wantErr bool
	}{
		{
			name: "write with nil detectResult",
			args: args{
				reader: bufio.NewReader(strings.NewReader(string(content))),
				task: &types.SeedTask{
					TaskID:    "5806501c3bb92f0b645918c5a4b15495a63259e3e0363008f97e186509e9e",
					PieceSize: 50,
				},
			},
			result: &downloadMetadata{
				backSourceLength:     contentLen,
				realCdnFileLength:    contentLen,
				realSourceFileLength: contentLen,
				pieceTotalCount:      2,
				pieceMd5Sign:         "3b0d3defb43bc99e9d0c31eef4d2efaf1d2bcb73fae8f337d6ebf4f643469173",
			},
		}, {
			name: "write with non nil detectResult",
			args: args{
				reader: bufio.NewReader(strings.NewReader(string(content))),
				task: &types.SeedTask{
					TaskID:    "5816501c3bb92f0b645918c5a4b15495a63259e3e0363008f97e186509e9e",
					PieceSize: 50,
				},
				detectResult: &cacheResult{
					breakPoint:       0,
					pieceMetaRecords: nil,
					fileMetaData:     nil,
					fileMd5:          nil,
				},
			},
			result: &downloadMetadata{
				backSourceLength:     contentLen,
				realCdnFileLength:    0,
				realSourceFileLength: 0,
				pieceTotalCount:      0,
				pieceMd5Sign:         "",
			},
		}, {
			name: "write with task length",
			args: args{
				reader: bufio.NewReader(strings.NewReader(string(content))),
				task: &types.SeedTask{
					TaskID:           "5826501c3bb92f0b645918c5a4b15495a63259e3e0363008f97e186509e93",
					PieceSize:        50,
					SourceFileLength: contentLen,
				},
				detectResult: &cacheResult{
					breakPoint:       0,
					pieceMetaRecords: nil,
					fileMetaData:     nil,
					fileMd5:          nil,
				},
			},
			result: &downloadMetadata{
				backSourceLength:     contentLen,
				realCdnFileLength:    0,
				realSourceFileLength: 0,
				pieceTotalCount:      0,
				pieceMd5Sign:         "",
			},
		},
	}
	for _, tt := range tests {
		suite.writer.cdnReporter.progress.InitSeedProgress(context.Background(), tt.args.task.TaskID)
		downloadMetadata, err := suite.writer.startWriter(tt.args.reader, tt.args.task, tt.args.detectResult)
		suite.Equal(tt.wantErr, err != nil)
		suite.Equal(tt.result, downloadMetadata)
		//suite.checkFileSize(suite.writer.cacheDataManager.storage, task.TaskID, contentLen)
	}
}

func (suite *CacheWriterAndDetectorTestSuite) TestCalculateRoutineCount() {
	type args struct {
		remainingFileLength int64
		pieceSize           int32
	}
	tests := []struct {
		name string
		args args
		want int
	}{
		{},
		{},
	}
	for _, tt := range tests {
		suite.Run(tt.name, func() {
			if got := calculateRoutineCount(tt.args.remainingFileLength, tt.args.pieceSize); got != tt.want {
				suite.Equal(tt.want, got)
			}
		})
	}
}

func (suite *CacheWriterAndDetectorTestSuite) checkFileSize(cdnStore storage.Manager, taskID string, expectedSize int64) {
	storageInfo, err := cdnStore.StatDownloadFile(taskID)
	suite.Nil(err)
	suite.Equal(expectedSize, storageInfo.Size)
}

func (suite *CacheWriterAndDetectorTestSuite) TestDetectCache() {
	type fields struct {
		cacheDataManager *cacheDataManager
	}
	type args struct {
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
		suite.Run(tt.name, func() {
			cd := &cacheDetector{
				cacheDataManager: tt.fields.cacheDataManager,
			}
			got, err := cd.detectCache(tt.args.task)
			suite.Equal(err, tt.wantErr)
			suite.Equal(tt.want, got)
		})
	}
}

func (suite *CacheWriterAndDetectorTestSuite) TestDoDetect(t *testing.T) {
	type fields struct {
		cacheDataManager *cacheDataManager
	}
	type args struct {
		task *types.SeedTask
	}
	tests := []struct {
		name       string
		fields     fields
		args       args
		wantResult *cacheResult
		wantErr    bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cd := &cacheDetector{
				cacheDataManager: tt.fields.cacheDataManager,
			}
			gotResult, err := cd.doDetect(tt.args.task)
			if (err != nil) != tt.wantErr {
				t.Errorf("doDetect() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(gotResult, tt.wantResult) {
				t.Errorf("doDetect() gotResult = %v, want %v", gotResult, tt.wantResult)
			}
		})
	}
}

func (suite *CacheWriterAndDetectorTestSuite) TestParseByReadFile(t *testing.T) {
	type fields struct {
		cacheDataManager *cacheDataManager
	}
	type args struct {
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
				cacheDataManager: tt.fields.cacheDataManager,
			}
			got, err := cd.parseByReadFile(tt.args.taskID, tt.args.metaData)
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

func (suite *CacheWriterAndDetectorTestSuite) TestParseByReadMetaFile(t *testing.T) {
	type fields struct {
		cacheDataManager *cacheDataManager
	}
	type args struct {
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
				cacheDataManager: tt.fields.cacheDataManager,
			}
			got, err := cd.parseByReadMetaFile(tt.args.taskID, tt.args.fileMetaData)
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

func TestResetCache(t *testing.T) {
	type fields struct {
		cacheDataManager *cacheDataManager
	}
	type args struct {
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
				cacheDataManager: tt.fields.cacheDataManager,
			}
			got, err := cd.resetCache(tt.args.task)
			if (err != nil) != tt.wantErr {
				t.Errorf("resetCache() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("resetCache() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_cacheResult_String(t *testing.T) {
	type fields struct {
		breakPoint       int64
		pieceMetaRecords []*storage.PieceMetaRecord
		fileMetaData     *storage.FileMetaData
		fileMd5          hash.Hash
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
			s := &cacheResult{
				breakPoint:       tt.fields.breakPoint,
				pieceMetaRecords: tt.fields.pieceMetaRecords,
				fileMetaData:     tt.fields.fileMetaData,
				fileMd5:          tt.fields.fileMd5,
			}
			if got := s.String(); got != tt.want {
				t.Errorf("String() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_checkPieceContent(t *testing.T) {
	type args struct {
		reader      io.Reader
		pieceRecord *storage.PieceMetaRecord
		fileMd5     hash.Hash
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
			if err := checkPieceContent(tt.args.reader, tt.args.pieceRecord, tt.args.fileMd5); (err != nil) != tt.wantErr {
				t.Errorf("checkPieceContent() error = %v, wantErr %v", err, tt.wantErr)
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
