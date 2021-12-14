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
	"os"
	"strings"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"

	"d7y.io/dragonfly/v2/cdn/plugins"
	"d7y.io/dragonfly/v2/cdn/storedriver"
	"d7y.io/dragonfly/v2/cdn/storedriver/local"
	"d7y.io/dragonfly/v2/cdn/supervisor/cdn/storage"
	progressMock "d7y.io/dragonfly/v2/cdn/supervisor/mocks/progress"
	taskMock "d7y.io/dragonfly/v2/cdn/supervisor/mocks/task"
	"d7y.io/dragonfly/v2/cdn/supervisor/task"
	"d7y.io/dragonfly/v2/pkg/ratelimiter/limitreader"
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
		"storagedriver": {
			{
				Name:   local.DiskDriverName,
				Enable: true,
				Config: &storedriver.Config{
					BaseDir: workHome,
				},
			},
		},
	}
}

func (suite *CacheWriterTestSuite) SetupSuite() {
	suite.workHome, _ = os.MkdirTemp("/tmp", "cdn-CacheWriterDetectorTestSuite-")
	suite.T().Log("workHome:", suite.workHome)
	suite.Nil(plugins.Initialize(NewPlugins(suite.workHome)))
	ctrl := gomock.NewController(suite.T())
	progressManager := progressMock.NewMockManager(ctrl)
	progressManager.EXPECT().PublishPiece(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes()
	progressManager.EXPECT().PublishTask(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes()
	taskManager := taskMock.NewMockManager(ctrl)
	storageManager, err := storage.NewManager(storage.DefaultConfig(), taskManager)
	suite.Require().Nil(err)
	metadataManager := newMetadataManager(storageManager)
	cdnReporter := newReporter(progressManager)
	suite.writer = newCacheWriter(cdnReporter, metadataManager, storageManager)
}

func (suite *CacheWriterTestSuite) TearDownSuite() {
	if suite.workHome != "" {
		if err := os.RemoveAll(suite.workHome); err != nil {
			fmt.Printf("remove path: %s error", suite.workHome)
		}
	}
}

func (suite *CacheWriterTestSuite) TestStartWriter() {
	content, err := os.ReadFile("../../testdata/cdn/go.html")
	suite.Nil(err)
	contentLen := int64(len(content))
	type args struct {
		reader             *limitreader.LimitReader
		task               *task.SeedTask
		breakPoint         int64
		writerRoutineLimit int
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
				reader: limitreader.NewLimitReader(bufio.NewReader(strings.NewReader(string(content))), 100),
				task: &task.SeedTask{
					ID:        "5806501c3bb92f0b645918c5a4b15495a63259e3e0363008f97e186509e9e",
					PieceSize: 50,
				},
				writerRoutineLimit: 4,
			},
			result: &downloadMetadata{
				backSourceLength:     contentLen,
				realCdnFileLength:    contentLen,
				realSourceFileLength: contentLen,
				totalPieceCount:      int32((contentLen + 49) / 50),
				pieceMd5Sign:         "3f4585787609b0d7d4c9fc800db61655a74494f83507c8acd2818d0461d9cdc5",
			},
		}, {
			name: "write with non nil detectResult",
			args: args{
				reader: limitreader.NewLimitReader(bufio.NewReader(strings.NewReader(string(content))), 100),
				task: &task.SeedTask{
					ID:        "5816501c3bb92f0b645918c5a4b15495a63259e3e0363008f97e186509e9e",
					PieceSize: 50,
				},
				writerRoutineLimit: 4,
			},
			result: &downloadMetadata{
				backSourceLength:     contentLen,
				realCdnFileLength:    contentLen,
				realSourceFileLength: contentLen,
				totalPieceCount:      int32((contentLen + 49) / 50),
				pieceMd5Sign:         "3f4585787609b0d7d4c9fc800db61655a74494f83507c8acd2818d0461d9cdc5",
			},
		}, {
			name: "write with task length",
			args: args{
				reader: limitreader.NewLimitReader(bufio.NewReader(strings.NewReader(string(content))), 100),
				task: &task.SeedTask{
					ID:               "5826501c3bb92f0b645918c5a4b15495a63259e3e0363008f97e186509e93",
					PieceSize:        50,
					SourceFileLength: contentLen,
				},
				writerRoutineLimit: 4,
			},
			result: &downloadMetadata{
				backSourceLength:     contentLen,
				realCdnFileLength:    contentLen,
				realSourceFileLength: contentLen,
				totalPieceCount:      int32((contentLen + 49) / 50),
				pieceMd5Sign:         "3f4585787609b0d7d4c9fc800db61655a74494f83507c8acd2818d0461d9cdc5",
			},
		},
	}
	for _, tt := range tests {
		suite.Run(tt.name, func() {
			downloadMetadata, err := suite.writer.startWriter(context.Background(), tt.args.reader, tt.args.task, tt.args.breakPoint, tt.args.writerRoutineLimit)
			suite.Equal(tt.wantErr, err != nil)
			suite.Equal(tt.result, downloadMetadata)
			suite.checkFileSize(suite.writer.cacheStore, tt.args.task.ID, contentLen)
		})
	}
}

func (suite *CacheWriterTestSuite) checkFileSize(cacheStore storage.Manager, taskID string, expectedSize int64) {
	storageInfo, err := cacheStore.StatDownloadFile(taskID)
	suite.Nil(err)
	suite.Equal(expectedSize, storageInfo.Size)
}

func (suite *CacheWriterTestSuite) TestCalculateRoutineCount() {
	type args struct {
		remainingFileLength int64
		pieceSize           int32
		writerRoutineLimit  int
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
				writerRoutineLimit:  4,
			},
			want: 4,
		},
		{
			name: "larger than default goroutine count",
			args: args{
				remainingFileLength: 2222,
				pieceSize:           50,
				writerRoutineLimit:  4,
			},
			want: 4,
		},
		{
			name: "remainingFileLength is zero",
			args: args{
				remainingFileLength: 0,
				pieceSize:           50,
				writerRoutineLimit:  4,
			},
			want: 1,
		},
		{
			name: "smaller than 1",
			args: args{
				remainingFileLength: 10,
				pieceSize:           50,
				writerRoutineLimit:  4,
			},
			want: 1,
		},
		{
			name: "piece size is zero",
			args: args{
				remainingFileLength: 10,
				pieceSize:           0,
				writerRoutineLimit:  4,
			},
			want: 4,
		},
	}
	for _, tt := range tests {
		suite.Run(tt.name, func() {
			if got := calculateRoutineCount(tt.args.remainingFileLength, tt.args.pieceSize, tt.args.writerRoutineLimit); got != tt.want {
				suite.Equal(tt.want, got)
			}
		})
	}
}
