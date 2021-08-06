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
	"io/ioutil"
	"os"
	"strings"
	"testing"

	"d7y.io/dragonfly/v2/cdnsystem/config"
	"d7y.io/dragonfly/v2/cdnsystem/daemon/cdn/storage"
	"d7y.io/dragonfly/v2/cdnsystem/daemon/mock"
	"d7y.io/dragonfly/v2/cdnsystem/plugins"
	"d7y.io/dragonfly/v2/cdnsystem/types"
	"d7y.io/dragonfly/v2/internal/idgen"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	"d7y.io/dragonfly/v2/pkg/source"
	sourceMock "d7y.io/dragonfly/v2/pkg/source/mock"
	"d7y.io/dragonfly/v2/pkg/util/net/urlutils"
	"d7y.io/dragonfly/v2/pkg/util/rangeutils"
	"d7y.io/dragonfly/v2/pkg/util/timeutils"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
)

func TestCDNManagerSuite(t *testing.T) {
	suite.Run(t, new(CDNManagerTestSuite))
}

type CDNManagerTestSuite struct {
	workHome string
	cm       *Manager
	suite.Suite
}

func (suite *CDNManagerTestSuite) SetupSuite() {
	suite.workHome, _ = ioutil.TempDir("/tmp", "cdn-ManagerTestSuite-")
	fmt.Printf("workHome: %s", suite.workHome)
	suite.Nil(plugins.Initialize(NewPlugins(suite.workHome)))
	storeMgr, ok := storage.Get(config.DefaultStorageMode)
	if !ok {
		suite.Failf("failed to get storage mode %s", config.DefaultStorageMode)
	}
	ctrl := gomock.NewController(suite.T())
	progressMgr := mock.NewMockSeedProgressMgr(ctrl)
	progressMgr.EXPECT().PublishPiece(md5TaskID, gomock.Any()).Return(nil).Times(98 * 2)
	progressMgr.EXPECT().PublishPiece(sha256TaskID, gomock.Any()).Return(nil).Times(98 * 2)
	suite.cm, _ = newManager(config.New(), storeMgr, progressMgr)
}

var (
	dragonflyURL = "http://dragonfly.io.com?a=a&b=b&c=c"
	md5TaskID    = idgen.TaskID(dragonflyURL, &base.UrlMeta{Digest: "md5:f1e2488bba4d1267948d9e2f7008571c", Tag: "dragonfly", Filter: "a&b"})
	sha256TaskID = idgen.TaskID(dragonflyURL, &base.UrlMeta{Digest: "sha256:b9907b9a5ba2b0223868c201b9addfe2ec1da1b90325d57c34f192966b0a68c5", Tag: "dragonfly", Filter: "a&b"})
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
	source.Register("http", sourceClient)
	defer source.UnRegister("http")
	sourceClient.EXPECT().IsSupportRange(gomock.Any(), dragonflyURL, gomock.Any()).Return(true, nil).AnyTimes()
	sourceClient.EXPECT().IsExpired(gomock.Any(), dragonflyURL, gomock.Any(), gomock.Any()).Return(false, nil).AnyTimes()
	sourceClient.EXPECT().Download(gomock.Any(), dragonflyURL, gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, url string, header source.RequestHeader, rang *rangeutils.Range) (io.ReadCloser, error) {
			content, _ := ioutil.ReadFile("../../testdata/cdn/go.html")
			if rang != nil {
				return ioutil.NopCloser(io.NewSectionReader(strings.NewReader(string(content)), int64(rang.StartIndex), int64(rang.EndIndex))), nil
			}
			return ioutil.NopCloser(strings.NewReader(string(content))), nil
		},
	).AnyTimes()
	sourceClient.EXPECT().DownloadWithResponseHeader(gomock.Any(), dragonflyURL, gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, url string, header source.RequestHeader, rang *rangeutils.Range) (io.ReadCloser, source.ResponseHeader, error) {
			content, _ := ioutil.ReadFile("../../testdata/cdn/go.html")
			if rang != nil {
				return ioutil.NopCloser(io.NewSectionReader(strings.NewReader(string(content)), int64(rang.StartIndex), int64(rang.EndIndex))),
					map[string]string{
						source.LastModified: "Sun, 06 Jun 2021 12:52:30 GMT",
						source.ETag:         "etag",
					}, nil
			}
			return ioutil.NopCloser(strings.NewReader(string(content))), map[string]string{
				source.LastModified: "Sun, 06 Jun 2021 12:52:30 GMT",
				source.ETag:         "etag",
			}, nil
		},
	).AnyTimes()
	sourceClient.EXPECT().GetLastModifiedMillis(gomock.Any(), dragonflyURL, gomock.Any()).Return(
		timeutils.UnixMillis("Sun, 06 Jun 2021 12:52:30 GMT"), nil).AnyTimes()
	sourceClient.EXPECT().GetContentLength(gomock.Any(), dragonflyURL, gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, url string, header source.RequestHeader, rang *rangeutils.Range) (int64, error) {
			if rang != nil {
				return int64(rang.EndIndex-rang.StartIndex) + 1, nil
			}
			fileInfo, _ := os.Stat("../../testdata/cdn/go.html")
			return fileInfo.Size(), nil
		},
	).AnyTimes()

	tests := []struct {
		name       string
		sourceTask *types.SeedTask
		targetTask *types.SeedTask
	}{
		{
			name: "trigger_md5",
			sourceTask: &types.SeedTask{
				TaskID:           md5TaskID,
				URL:              dragonflyURL,
				TaskURL:          urlutils.FilterURLParam(dragonflyURL, []string{"a", "b"}),
				SourceFileLength: 9789,
				CdnFileLength:    0,
				PieceSize:        100,
				Header:           map[string]string{"md5": "f1e2488bba4d1267948d9e2f7008571c"},
				CdnStatus:        types.TaskInfoCdnStatusRunning,
				PieceTotal:       0,
				RequestDigest:    "md5:f1e2488bba4d1267948d9e2f7008571c",
				SourceRealDigest: "",
				PieceMd5Sign:     "",
			},
			targetTask: &types.SeedTask{
				TaskID:           md5TaskID,
				URL:              dragonflyURL,
				TaskURL:          urlutils.FilterURLParam(dragonflyURL, []string{"a", "b"}),
				SourceFileLength: 9789,
				CdnFileLength:    9789,
				PieceSize:        100,
				Header:           map[string]string{"md5": "f1e2488bba4d1267948d9e2f7008571c"},
				CdnStatus:        types.TaskInfoCdnStatusSuccess,
				PieceTotal:       0,
				RequestDigest:    "md5:f1e2488bba4d1267948d9e2f7008571c",
				SourceRealDigest: "md5:f1e2488bba4d1267948d9e2f7008571c",
				PieceMd5Sign:     "bb138842f338fff90af737e4a6b2c6f8e2a7031ca9d5900bc9b646f6406d890f",
			},
		},
		{
			name: "trigger_sha256",
			sourceTask: &types.SeedTask{
				TaskID:           sha256TaskID,
				URL:              dragonflyURL,
				TaskURL:          urlutils.FilterURLParam(dragonflyURL, []string{"a", "b"}),
				SourceFileLength: 9789,
				CdnFileLength:    0,
				PieceSize:        100,
				Header:           map[string]string{"sha256": "b9907b9a5ba2b0223868c201b9addfe2ec1da1b90325d57c34f192966b0a68c5"},
				CdnStatus:        types.TaskInfoCdnStatusRunning,
				PieceTotal:       0,
				RequestDigest:    "sha256:b9907b9a5ba2b0223868c201b9addfe2ec1da1b90325d57c34f192966b0a68c5",
				SourceRealDigest: "",
				PieceMd5Sign:     "",
			},
			targetTask: &types.SeedTask{
				TaskID:           sha256TaskID,
				URL:              dragonflyURL,
				TaskURL:          urlutils.FilterURLParam(dragonflyURL, []string{"a", "b"}),
				SourceFileLength: 9789,
				CdnFileLength:    9789,
				PieceSize:        100,
				Header:           map[string]string{"sha256": "b9907b9a5ba2b0223868c201b9addfe2ec1da1b90325d57c34f192966b0a68c5"},
				CdnStatus:        types.TaskInfoCdnStatusSuccess,
				PieceTotal:       0,
				RequestDigest:    "sha256:b9907b9a5ba2b0223868c201b9addfe2ec1da1b90325d57c34f192966b0a68c5",
				SourceRealDigest: "sha256:b9907b9a5ba2b0223868c201b9addfe2ec1da1b90325d57c34f192966b0a68c5",
				PieceMd5Sign:     "bb138842f338fff90af737e4a6b2c6f8e2a7031ca9d5900bc9b646f6406d890f",
			},
		},
	}

	for _, tt := range tests {
		suite.Run(tt.name, func() {
			gotSeedTask, err := suite.cm.TriggerCDN(context.Background(), tt.sourceTask)
			suite.Nil(err)
			suite.Equal(tt.targetTask, gotSeedTask)
			cacheSeedTask, err := suite.cm.TriggerCDN(context.Background(), gotSeedTask)
			suite.Nil(err)
			suite.Equal(tt.targetTask, cacheSeedTask)
		})
	}

	// TODO test range download
}
