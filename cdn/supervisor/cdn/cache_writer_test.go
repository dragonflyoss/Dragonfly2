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
	"io"
	"io/ioutil"
	"os"
	"strings"
	"testing"
	"time"

	"d7y.io/dragonfly/v2/cdn/config"
	"d7y.io/dragonfly/v2/cdn/plugins"
	"d7y.io/dragonfly/v2/cdn/storedriver"
	"d7y.io/dragonfly/v2/cdn/storedriver/local"
	"d7y.io/dragonfly/v2/cdn/supervisor/cdn/storage"
	"d7y.io/dragonfly/v2/cdn/supervisor/cdn/storage/disk"
	"d7y.io/dragonfly/v2/cdn/supervisor/progress"
	"d7y.io/dragonfly/v2/cdn/types"
	"d7y.io/dragonfly/v2/pkg/unit"
	"github.com/stretchr/testify/suite"
)

func TestCacheWriterSuite(t *testing.T) {
	suite.Run(t, new(CacheWriterTestSuite))
}

type CacheWriterTestSuite struct {
	workHome string
	writer   *cacheWriter
	suite.Suite
}

func NewPlugins(workHome string) map[plugins.PluginType][]*plugins.PluginProperties {
	return map[plugins.PluginType][]*plugins.PluginProperties{
		plugins.StorageDriverPlugin: {
			{
				Name:   local.DiskDriverName,
				Enable: true,
				Config: &storedriver.Config{
					BaseDir: workHome,
				},
			},
		}, plugins.StorageManagerPlugin: {
			{
				Name:   disk.StorageMode,
				Enable: true,
				Config: &storage.Config{
					GCInitialDelay: 0 * time.Second,
					GCInterval:     15 * time.Second,
					DriverConfigs: map[string]*storage.DriverConfig{
						local.DiskDriverName: {
							GCConfig: &storage.GCConfig{
								YoungGCThreshold:  100 * unit.GB,
								FullGCThreshold:   5 * unit.GB,
								CleanRatio:        1,
								IntervalThreshold: 2 * time.Hour,
							}},
					},
				},
			},
		},
	}
}

func (suite *CacheWriterTestSuite) SetupSuite() {
	suite.workHome, _ = ioutil.TempDir("/tmp", "cdn-CacheWriterDetectorTestSuite-")
	suite.T().Log("workHome:", suite.workHome)
	suite.Nil(plugins.Initialize(NewPlugins(suite.workHome)))
	storeMgr, ok := storage.Get(config.DefaultStorageMode)
	if !ok {
		suite.Failf("failed to get storage mode %s", config.DefaultStorageMode)
	}
	cacheDataManager := newCacheDataManager(storeMgr)
	progressMgr, _ := progress.NewManager()
	cdnReporter := newReporter(progressMgr)
	suite.writer = newCacheWriter(cdnReporter, cacheDataManager)
}

func (suite *CacheWriterTestSuite) TearDownSuite() {
	if suite.workHome != "" {
		if err := os.RemoveAll(suite.workHome); err != nil {
			fmt.Printf("remove path: %s error", suite.workHome)
		}
	}
}

func (suite *CacheWriterTestSuite) TestStartWriter() {
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
				pieceTotalCount:      int32((contentLen + 49) / 50),
				pieceMd5Sign:         "3f4585787609b0d7d4c9fc800db61655a74494f83507c8acd2818d0461d9cdc5",
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
				},
			},
			result: &downloadMetadata{
				backSourceLength:     contentLen,
				realCdnFileLength:    contentLen,
				realSourceFileLength: contentLen,
				pieceTotalCount:      int32((contentLen + 49) / 50),
				pieceMd5Sign:         "3f4585787609b0d7d4c9fc800db61655a74494f83507c8acd2818d0461d9cdc5",
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
				},
			},
			result: &downloadMetadata{
				backSourceLength:     contentLen,
				realCdnFileLength:    contentLen,
				realSourceFileLength: contentLen,
				pieceTotalCount:      int32((contentLen + 49) / 50),
				pieceMd5Sign:         "3f4585787609b0d7d4c9fc800db61655a74494f83507c8acd2818d0461d9cdc5",
			},
		},
	}
	for _, tt := range tests {
		suite.Run(tt.name, func() {
			suite.writer.cdnReporter.progress.InitSeedProgress(context.Background(), tt.args.task.TaskID)
			downloadMetadata, err := suite.writer.startWriter(context.Background(), tt.args.reader, tt.args.task, tt.args.detectResult)
			suite.Equal(tt.wantErr, err != nil)
			suite.Equal(tt.result, downloadMetadata)
			suite.checkFileSize(suite.writer.cacheDataManager, tt.args.task.TaskID, contentLen)
		})
	}
}

func (suite *CacheWriterTestSuite) checkFileSize(cacheDataMgr *cacheDataManager, taskID string, expectedSize int64) {
	storageInfo, err := cacheDataMgr.statDownloadFile(taskID)
	suite.Nil(err)
	suite.Equal(expectedSize, storageInfo.Size)
}

func (suite *CacheWriterTestSuite) TestCalculateRoutineCount() {
	type args struct {
		remainingFileLength int64
		pieceSize           int32
	}
	tests := []struct {
		name string
		args args
		want int
	}{
		{
			name: "Exact equal default goroutine count",
			args: args{
				remainingFileLength: 200,
				pieceSize:           50,
			},
			want: 4,
		},
		{
			name: "larger than default goroutine count",
			args: args{
				remainingFileLength: 2222,
				pieceSize:           50,
			},
			want: 4,
		},
		{
			name: "remainingFileLength is zero",
			args: args{
				remainingFileLength: 0,
				pieceSize:           50,
			},
			want: 1,
		},
		{
			name: "smaller than 1",
			args: args{
				remainingFileLength: 10,
				pieceSize:           50,
			},
			want: 1,
		},
		{
			name: "piece size is zero",
			args: args{
				remainingFileLength: 10,
				pieceSize:           0,
			},
			want: 4,
		},
	}
	for _, tt := range tests {
		suite.Run(tt.name, func() {
			if got := calculateRoutineCount(tt.args.remainingFileLength, tt.args.pieceSize); got != tt.want {
				suite.Equal(tt.want, got)
			}
		})
	}
}
