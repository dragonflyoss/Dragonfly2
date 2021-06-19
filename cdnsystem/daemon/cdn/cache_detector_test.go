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
	"crypto/md5"
	"hash"
	"io"
	"io/ioutil"
	"strings"
	"testing"

	"d7y.io/dragonfly/v2/cdnsystem/daemon/cdn/storage"
	storageMock "d7y.io/dragonfly/v2/cdnsystem/daemon/cdn/storage/mock"
	cdnerrors "d7y.io/dragonfly/v2/cdnsystem/errors"
	"d7y.io/dragonfly/v2/cdnsystem/types"
	"d7y.io/dragonfly/v2/pkg/source"
	sourceMock "d7y.io/dragonfly/v2/pkg/source/mock"
	"d7y.io/dragonfly/v2/pkg/util/rangeutils"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
)

func TestCacheDetectorSuite(t *testing.T) {
	suite.Run(t, new(CacheDetectorTestSuite))
}

type CacheDetectorTestSuite struct {
	detector *cacheDetector
	suite.Suite
}

func (suite *CacheDetectorTestSuite) SetupSuite() {
	ctrl := gomock.NewController(suite.T())
	sourceClient := sourceMock.NewMockResourceClient(ctrl)
	source.Register("http", sourceClient)
	storageMgr := storageMock.NewMockManager(ctrl)
	cacheDataManager := newCacheDataManager(storageMgr)
	suite.detector = newCacheDetector(cacheDataManager)
	storageMgr.EXPECT().ReadFileMetaData(fullExpiredCache.taskID).Return(fullExpiredCache.fileMeta, nil)
	storageMgr.EXPECT().ReadFileMetaData(fullNoExpiredCache.taskID).Return(fullNoExpiredCache.fileMeta, nil)
	storageMgr.EXPECT().ReadFileMetaData(partialNotSupportRangeCache.taskID).Return(partialNotSupportRangeCache.fileMeta, nil)
	storageMgr.EXPECT().ReadFileMetaData(partialSupportRangeCache.taskID).Return(partialSupportRangeCache.fileMeta, nil)
	storageMgr.EXPECT().ReadFileMetaData(noCache.taskID).Return(noCache.fileMeta, cdnerrors.ErrFileNotExist)
	storageMgr.EXPECT().ReadDownloadFile(fullNoExpiredCache.taskID).DoAndReturn(
		func(taskID string) (io.ReadCloser, error) {
			content, err := ioutil.ReadFile("../../testdata/cdn/go.html")
			suite.Nil(err)
			return ioutil.NopCloser(strings.NewReader(string(content))), nil
		})
	storageMgr.EXPECT().ReadDownloadFile(partialNotSupportRangeCache.taskID).DoAndReturn(
		func(taskID string) (io.ReadCloser, error) {
			content, err := ioutil.ReadFile("../../testdata/cdn/go.html")
			suite.Nil(err)
			return ioutil.NopCloser(strings.NewReader(string(content))), nil
		})
	storageMgr.EXPECT().ReadDownloadFile(partialSupportRangeCache.taskID).DoAndReturn(
		func(taskID string) (io.ReadCloser, error) {
			content, err := ioutil.ReadFile("../../testdata/cdn/go.html")
			suite.Nil(err)
			return ioutil.NopCloser(strings.NewReader(string(content))), nil
		})
	storageMgr.EXPECT().ReadDownloadFile(noCache.taskID).Return(nil, cdnerrors.ErrFileNotExist)
	storageMgr.EXPECT().ReadPieceMetaRecords(fullNoExpiredCache.taskID).Return(fullNoExpiredCache.pieces, nil)
	storageMgr.EXPECT().ReadPieceMetaRecords(partialNotSupportRangeCache.taskID).Return(partialNotSupportRangeCache.pieces, nil)
	storageMgr.EXPECT().ReadPieceMetaRecords(partialSupportRangeCache.taskID).Return(partialSupportRangeCache.pieces, nil)
	storageMgr.EXPECT().ReadPieceMetaRecords(noCache.taskID).Return(nil, cdnerrors.ErrFileNotExist)

	sourceClient.EXPECT().IsExpired(gomock.Any(), expiredAndSupportURL, gomock.Any(), gomock.Any()).Return(true, nil)
	sourceClient.EXPECT().IsSupportRange(gomock.Any(), expiredAndSupportURL, gomock.Any()).Return(true, nil)

	sourceClient.EXPECT().IsExpired(gomock.Any(), expiredAndNotSupportURL, gomock.Any(), gomock.Any()).Return(true, nil)
	sourceClient.EXPECT().IsSupportRange(gomock.Any(), expiredAndNotSupportURL, gomock.Any()).Return(false, nil)

	sourceClient.EXPECT().IsExpired(gomock.Any(), noExpiredAndSupportURL, gomock.Any(), gomock.Any()).Return(false, nil)
	sourceClient.EXPECT().IsSupportRange(gomock.Any(), noExpiredAndSupportURL, gomock.Any()).Return(true, nil)

	sourceClient.EXPECT().IsExpired(gomock.Any(), noExpiredAndNotSupportURL, gomock.Any(), gomock.Any()).Return(false, nil)
	sourceClient.EXPECT().IsSupportRange(gomock.Any(), noExpiredAndNotSupportURL, gomock.Any()).Return(false, nil)
}

var noCacheTask, partialAndSupportCacheTask, partialAndNotSupportCacheTask, fullCacheExpiredTask, fullCacheNotExpiredTask = "noCache", "partialSupportCache",
	"partialNotSupportCache", "fullCache", "fullCacheNotExpired"

var expiredAndSupportURL, expiredAndNotSupportURL, noExpiredAndSupportURL, noExpiredAndNotSupportURL = "http://expiredsupport.com",
	"http://expiredNotsupport.com", "http://noexpiredAndsupport.com", "http://noexpiredAndnotsupport.com"

type mockData struct {
	taskID   string
	pieces   []*storage.PieceMetaRecord
	fileMeta *storage.FileMetaData
	reader   io.ReadCloser
}

var fullNoExpiredCache = mockData{
	taskID:   fullCacheNotExpiredTask,
	pieces:   fullPieceMetaRecords,
	fileMeta: newCompletedFileMeta(fullCacheNotExpiredTask, noExpiredAndSupportURL, true),
}

var fullExpiredCache = mockData{
	taskID:   fullCacheExpiredTask,
	pieces:   fullPieceMetaRecords,
	fileMeta: newCompletedFileMeta(fullCacheExpiredTask, noExpiredAndSupportURL, true),
}

var partialSupportRangeCache = mockData{
	taskID:   partialAndSupportCacheTask,
	pieces:   partialPieceMetaRecords,
	fileMeta: newPartialFileMeta(partialAndSupportCacheTask, noExpiredAndSupportURL),
}

var partialNotSupportRangeCache = mockData{
	taskID:   partialAndNotSupportCacheTask,
	pieces:   partialPieceMetaRecords,
	fileMeta: newPartialFileMeta(partialAndNotSupportCacheTask, noExpiredAndNotSupportURL),
}

var noCache = mockData{
	taskID:   noCacheTask,
	pieces:   nil,
	fileMeta: nil,
}

var partialPieceMetaRecords = []*storage.PieceMetaRecord{
	{
		PieceNum: 1,
		PieceLen: 2000,
		Md5:      "67e186642cc5d1b43713379955af82bd",
		Range: &rangeutils.Range{
			StartIndex: 2000,
			EndIndex:   3999,
		},
		OriginRange: &rangeutils.Range{
			StartIndex: 2000,
			EndIndex:   3999,
		},
		PieceStyle: 1,
	}, {
		PieceNum: 0,
		PieceLen: 2000,
		Md5:      "4a6cf46821d4fb237bc2179bb5bedfa6",
		Range: &rangeutils.Range{
			StartIndex: 0,
			EndIndex:   1999,
		},
		OriginRange: &rangeutils.Range{
			StartIndex: 0,
			EndIndex:   1999,
		},
		PieceStyle: 1,
	},
}
var fullPieceMetaRecords = append(partialPieceMetaRecords, &storage.PieceMetaRecord{
	PieceNum: 2,
	PieceLen: 479,
	Md5:      "d6c0ab4b25887f295c54c041212e1aca",
	Range: &rangeutils.Range{
		StartIndex: 1000,
		EndIndex:   1480,
	},
	OriginRange: &rangeutils.Range{
		StartIndex: 1000,
		EndIndex:   1480,
	},
	PieceStyle: 1,
})

func newCompletedFileMeta(taskID string, URL string, success bool) *storage.FileMetaData {
	return &storage.FileMetaData{
		TaskID:          taskID,
		TaskURL:         URL,
		PieceSize:       2000,
		SourceFileLen:   9789,
		AccessTime:      1624126443284,
		Interval:        0,
		CdnFileLength:   9789,
		SourceRealMd5:   "",
		PieceMd5Sign:    "",
		ExpireInfo:      nil,
		Finish:          true,
		Success:         success,
		TotalPieceCount: 3,
	}
}

func newPartialFileMeta(taskID string, URL string) *storage.FileMetaData {
	return &storage.FileMetaData{
		TaskID:          taskID,
		TaskURL:         URL,
		PieceSize:       2000,
		SourceFileLen:   9789,
		AccessTime:      1624126443284,
		Interval:        0,
		CdnFileLength:   0,
		SourceRealMd5:   "",
		PieceMd5Sign:    "",
		ExpireInfo:      nil,
		Finish:          false,
		Success:         false,
		TotalPieceCount: 0,
	}
}

func (suite *CacheDetectorTestSuite) TestDetectCache() {
	type args struct {
		task *types.SeedTask
	}
	tests := []struct {
		name    string
		args    args
		want    *cacheResult
		wantErr bool
	}{
		{
			name: "no cache",
			args: args{
				task: &types.SeedTask{
					TaskID:  noCacheTask,
					URL:     noExpiredAndSupportURL,
					TaskURL: noExpiredAndSupportURL,
				},
			},
			want:    nil,
			wantErr: true,
		}, {
			name: "partial cache and support range",
			args: args{
				task: &types.SeedTask{
					TaskID:           partialAndSupportCacheTask,
					URL:              noExpiredAndSupportURL,
					TaskURL:          noExpiredAndSupportURL,
					SourceFileLength: 9789,
					PieceSize:        2000,
				},
			},
			want: &cacheResult{
				breakPoint:       4000,
				pieceMetaRecords: partialPieceMetaRecords,
				fileMetaData:     newPartialFileMeta(partialAndSupportCacheTask, noExpiredAndSupportURL),
			},
			wantErr: false,
		}, {
			name: "partial cache and not support range",
			args: args{
				task: &types.SeedTask{
					TaskID:           partialAndNotSupportCacheTask,
					URL:              noExpiredAndNotSupportURL,
					TaskURL:          noExpiredAndNotSupportURL,
					SourceFileLength: 9789,
					PieceSize:        2000,
				},
			},
			want: &cacheResult{
				breakPoint:       0,
				pieceMetaRecords: nil,
				fileMetaData:     nil,
			},
			wantErr: false,
		}, {
			name: "full cache and not expire",
			args: args{
				task: &types.SeedTask{
					TaskID:           fullCacheNotExpiredTask,
					URL:              noExpiredAndSupportURL,
					TaskURL:          noExpiredAndNotSupportURL,
					SourceFileLength: 9789,
				},
			},
			want: &cacheResult{
				breakPoint:       -1,
				pieceMetaRecords: fullPieceMetaRecords,
				fileMetaData:     newCompletedFileMeta(fullCacheNotExpiredTask, noExpiredAndNotSupportURL, true),
			},
			wantErr: false,
		}, {
			name: "full cache and expired",
			args: args{
				task: &types.SeedTask{
					TaskID:  fullCacheExpiredTask,
					URL:     expiredAndSupportURL,
					TaskURL: expiredAndNotSupportURL,
				},
			},
			want: &cacheResult{
				breakPoint:       0,
				pieceMetaRecords: nil,
				fileMetaData:     newCompletedFileMeta(fullCacheExpiredTask, noExpiredAndNotSupportURL, true),
			},
		}, {
			name: "data corruption",
			args: args{
				task: nil,
			},
			want: &cacheResult{
				breakPoint:       0,
				pieceMetaRecords: nil,
				fileMetaData:     nil,
			},
		},
	}
	for _, tt := range tests {
		suite.Run(tt.name, func() {
			digest := md5.New()
			got, err := suite.detector.doDetect(tt.args.task, digest)
			suite.Equal(tt.want, got)
			suite.Equal(err != nil, tt.wantErr)
		})
	}
}

func (suite *CacheDetectorTestSuite) TestParseByReadFile() {
	type args struct {
		taskID   string
		metaData *storage.FileMetaData
	}
	tests := []struct {
		name    string
		args    args
		want    *cacheResult
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		suite.Run(tt.name, func() {
			got, err := suite.detector.parseByReadFile(tt.args.taskID, tt.args.metaData, md5.New())
			suite.Equal(tt.wantErr, err)
			suite.Equal(tt.want, got)
		})
	}
}

func (suite *CacheDetectorTestSuite) TestParseByReadMetaFile() {
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
		suite.Run(tt.name, func() {
			got, err := suite.detector.parseByReadMetaFile(tt.args.taskID, tt.args.fileMetaData)
			suite.Equal(tt.wantErr, err)
			suite.Equal(tt.want, got)
		})
	}
}

func (suite *CacheDetectorTestSuite) TestCheckPieceContent() {
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
		suite.Run(tt.name, func() {
			err := checkPieceContent(tt.args.reader, tt.args.pieceRecord, tt.args.fileMd5)
			suite.Equal(tt.wantErr, err)
		})
	}
}

func (suite *CacheDetectorTestSuite) TestCheckSameFile(t *testing.T) {
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
