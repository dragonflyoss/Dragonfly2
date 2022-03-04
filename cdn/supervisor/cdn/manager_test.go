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
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"

	"d7y.io/dragonfly/v2/cdn/supervisor/cdn/storage"
	_ "d7y.io/dragonfly/v2/cdn/supervisor/cdn/storage/disk"
	progressMock "d7y.io/dragonfly/v2/cdn/supervisor/mocks/progress"
	taskMock "d7y.io/dragonfly/v2/cdn/supervisor/mocks/task"
	"d7y.io/dragonfly/v2/cdn/supervisor/task"
	"d7y.io/dragonfly/v2/pkg/idgen"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	"d7y.io/dragonfly/v2/pkg/source"
	"d7y.io/dragonfly/v2/pkg/source/httpprotocol"
	sourceMock "d7y.io/dragonfly/v2/pkg/source/mock"
	"d7y.io/dragonfly/v2/pkg/util/net/urlutils"
	"d7y.io/dragonfly/v2/pkg/util/rangeutils"
	"d7y.io/dragonfly/v2/pkg/util/timeutils"
)

func TestCDNManagerSuite(t *testing.T) {
	suite.Run(t, new(CDNManagerTestSuite))
}

type CDNManagerTestSuite struct {
	workHome string
	cm       Manager
	suite.Suite
}

func (suite *CDNManagerTestSuite) SetupSuite() {
	suite.workHome, _ = os.MkdirTemp("/tmp", "cdn-ManagerTestSuite-")
	fmt.Printf("workHome: %s", suite.workHome)
	ctrl := gomock.NewController(suite.T())
	taskManager := taskMock.NewMockManager(ctrl)
	storageConfig := storage.DefaultConfig()
	storageConfig.DriverConfigs["disk"].BaseDir = suite.workHome
	storageManager, err := storage.NewManager(storageConfig, taskManager)
	suite.Require().Nil(err)

	progressManager := progressMock.NewMockManager(ctrl)
	progressManager.EXPECT().PublishPiece(gomock.Any(), md5TaskID, gomock.Any()).Return(nil).Times(98 * 2)
	progressManager.EXPECT().PublishPiece(gomock.Any(), sha256TaskID, gomock.Any()).Return(nil).Times(98 * 2)
	progressManager.EXPECT().PublishTask(gomock.Any(), md5TaskID, gomock.Any()).Return(nil).Times(2)
	progressManager.EXPECT().PublishTask(gomock.Any(), sha256TaskID, gomock.Any()).Return(nil).Times(2)
	suite.cm, _ = NewManager(Config{}.applyDefaults(), storageManager, progressManager, taskManager)
}

var (
	dragonflyRawURL = "http://dragonfly.io.com?a=a&b=b&c=c"
	md5TaskID       = idgen.TaskID(dragonflyRawURL, &base.UrlMeta{Digest: "md5:f1e2488bba4d1267948d9e2f7008571c", Tag: "dragonfly", Filter: "a&b"})
	sha256TaskID    = idgen.TaskID(dragonflyRawURL, &base.UrlMeta{Digest: "sha256:b9907b9a5ba2b0223868c201b9addfe2ec1da1b90325d57c34f192966b0a68c5", Tag: "dragonfly", Filter: "a&b"})
)

func (suite *CDNManagerTestSuite) TearDownSuite() {
	if suite.workHome != "" {
		if err := os.RemoveAll(suite.workHome); err != nil {
			fmt.Printf("remove path: %s error", suite.workHome)
		}
	}
}

func (suite *CDNManagerTestSuite) TestTriggerCDN() {
	ctrl := gomock.NewController(suite.T())
	sourceClient := sourceMock.NewMockResourceClient(ctrl)
	source.UnRegister("http")
	suite.Require().Nil(source.Register("http", sourceClient, httpprotocol.Adapter))
	defer source.UnRegister("http")

	sourceClient.EXPECT().IsSupportRange(gomock.Any()).Return(true, nil).AnyTimes()
	sourceClient.EXPECT().IsExpired(gomock.Any(), gomock.Any()).Return(false, nil).AnyTimes()
	sourceClient.EXPECT().Download(gomock.Any()).DoAndReturn(
		func(request *source.Request) (*source.Response, error) {
			content, _ := os.ReadFile("../../testdata/cdn/go.html")
			if request.Header.Get(source.Range) != "" {
				parsed, _ := rangeutils.GetRange(request.Header.Get(source.Range))
				return source.NewResponse(
					io.NopCloser(io.NewSectionReader(strings.NewReader(string(content)), int64(parsed.StartIndex), int64(parsed.EndIndex))),
					source.WithExpireInfo(source.ExpireInfo{
						LastModified: "Sun, 06 Jun 2021 12:52:30 GMT",
						ETag:         "etag",
					})), nil
			}
			return source.NewResponse(io.NopCloser(strings.NewReader(string(content))),
				source.WithExpireInfo(source.ExpireInfo{
					LastModified: "Sun, 06 Jun 2021 12:52:30 GMT",
					ETag:         "etag",
				})), nil
		},
	).AnyTimes()
	sourceClient.EXPECT().GetLastModified(gomock.Any()).Return(
		timeutils.UnixMillis("Sun, 06 Jun 2021 12:52:30 GMT"), nil).AnyTimes()
	sourceClient.EXPECT().GetContentLength(gomock.Any()).DoAndReturn(
		func(request *source.Request) (int64, error) {
			if request.Header.Get(source.Range) != "" {
				parsed, _ := rangeutils.GetRange(request.Header.Get(source.Range))
				return int64(parsed.EndIndex-parsed.StartIndex) + 1, nil
			}
			fileInfo, _ := os.Stat("../../testdata/cdn/go.html")
			return fileInfo.Size(), nil
		},
	).AnyTimes()

	tests := []struct {
		name       string
		sourceTask *task.SeedTask
		targetTask *task.SeedTask
	}{
		{
			name: "trigger_md5",
			sourceTask: &task.SeedTask{
				ID:               md5TaskID,
				RawURL:           dragonflyRawURL,
				TaskURL:          urlutils.FilterURLParam(dragonflyRawURL, []string{"a", "b"}),
				SourceFileLength: 9789,
				CdnFileLength:    0,
				PieceSize:        100,
				Header:           map[string]string{"md5": "f1e2488bba4d1267948d9e2f7008571c"},
				CdnStatus:        task.StatusRunning,
				TotalPieceCount:  98,
				Digest:           "md5:f1e2488bba4d1267948d9e2f7008571c",
				SourceRealDigest: "",
				PieceMd5Sign:     "",
				Pieces:           new(sync.Map),
			},
			targetTask: &task.SeedTask{
				ID:               md5TaskID,
				RawURL:           dragonflyRawURL,
				TaskURL:          urlutils.FilterURLParam(dragonflyRawURL, []string{"a", "b"}),
				SourceFileLength: 9789,
				CdnFileLength:    9789,
				PieceSize:        100,
				Header:           map[string]string{"md5": "f1e2488bba4d1267948d9e2f7008571c"},
				CdnStatus:        task.StatusSuccess,
				TotalPieceCount:  98,
				Digest:           "md5:f1e2488bba4d1267948d9e2f7008571c",
				SourceRealDigest: "md5:f1e2488bba4d1267948d9e2f7008571c",
				PieceMd5Sign:     "bb138842f338fff90af737e4a6b2c6f8e2a7031ca9d5900bc9b646f6406d890f",
				Pieces:           new(sync.Map),
			},
		},
		{
			name: "trigger_sha256",
			sourceTask: &task.SeedTask{
				ID:               sha256TaskID,
				RawURL:           dragonflyRawURL,
				TaskURL:          urlutils.FilterURLParam(dragonflyRawURL, []string{"a", "b"}),
				SourceFileLength: 9789,
				CdnFileLength:    0,
				PieceSize:        100,
				Header:           map[string]string{"sha256": "b9907b9a5ba2b0223868c201b9addfe2ec1da1b90325d57c34f192966b0a68c5"},
				CdnStatus:        task.StatusRunning,
				TotalPieceCount:  98,
				Digest:           "sha256:b9907b9a5ba2b0223868c201b9addfe2ec1da1b90325d57c34f192966b0a68c5",
				SourceRealDigest: "",
				PieceMd5Sign:     "",
				Pieces:           new(sync.Map),
			},
			targetTask: &task.SeedTask{
				ID:               sha256TaskID,
				RawURL:           dragonflyRawURL,
				TaskURL:          urlutils.FilterURLParam(dragonflyRawURL, []string{"a", "b"}),
				SourceFileLength: 9789,
				CdnFileLength:    9789,
				PieceSize:        100,
				Header:           map[string]string{"sha256": "b9907b9a5ba2b0223868c201b9addfe2ec1da1b90325d57c34f192966b0a68c5"},
				CdnStatus:        task.StatusSuccess,
				TotalPieceCount:  98,
				Digest:           "sha256:b9907b9a5ba2b0223868c201b9addfe2ec1da1b90325d57c34f192966b0a68c5",
				SourceRealDigest: "sha256:b9907b9a5ba2b0223868c201b9addfe2ec1da1b90325d57c34f192966b0a68c5",
				PieceMd5Sign:     "bb138842f338fff90af737e4a6b2c6f8e2a7031ca9d5900bc9b646f6406d890f",
				Pieces:           new(sync.Map),
			},
		},
	}

	for _, tt := range tests {
		suite.Run(tt.name, func() {
			gotSeedTask, err := suite.cm.TriggerCDN(context.Background(), tt.sourceTask)
			suite.Nil(err)
			suite.True(task.IsEqual(*tt.targetTask, *gotSeedTask))
			cacheSeedTask, err := suite.cm.TriggerCDN(context.Background(), gotSeedTask)
			suite.Nil(err)
			suite.True(task.IsEqual(*tt.targetTask, *cacheSeedTask))
		})
	}

	// TODO test range download
}
