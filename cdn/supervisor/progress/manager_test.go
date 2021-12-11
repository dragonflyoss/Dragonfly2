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

package progress

import (
	"context"
	"sync"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"

	"d7y.io/dragonfly/v2/cdn/config"
	"d7y.io/dragonfly/v2/cdn/supervisor/task"
	"d7y.io/dragonfly/v2/pkg/source"
	"d7y.io/dragonfly/v2/pkg/source/httpprotocol"
	sourcemock "d7y.io/dragonfly/v2/pkg/source/mock"
	"d7y.io/dragonfly/v2/pkg/util/rangeutils"
)

func TestProgressManagerSuite(t *testing.T) {
	suite.Run(t, new(ProgressManagerTestSuite))
}

type ProgressManagerTestSuite struct {
	manager *manager
	suite.Suite
}

var (
	testTaskID = "testTaskID"
	testTask   = task.NewSeedTask(testTaskID, "https://www.drgonfly.com", nil)
	taskPieces = map[uint32]*task.PieceInfo{
		0: {
			PieceNum: 0,
			PieceMd5: "md50",
			PieceRange: &rangeutils.Range{
				StartIndex: 0,
				EndIndex:   99,
			},
			OriginRange: &rangeutils.Range{
				StartIndex: 0,
				EndIndex:   99,
			},
			PieceLen:   100,
			PieceStyle: 0,
		},
		1: {
			PieceNum: 1,
			PieceMd5: "md51",
			PieceRange: &rangeutils.Range{
				StartIndex: 100,
				EndIndex:   199,
			},
			OriginRange: &rangeutils.Range{
				StartIndex: 100,
				EndIndex:   199,
			},
			PieceLen:   100,
			PieceStyle: 0,
		},
		2: {
			PieceNum: 2,
			PieceMd5: "md52",
			PieceRange: &rangeutils.Range{
				StartIndex: 200,
				EndIndex:   299,
			},
			OriginRange: &rangeutils.Range{
				StartIndex: 200,
				EndIndex:   299,
			},
			PieceLen:   100,
			PieceStyle: 0,
		},
		3: {
			PieceNum: 3,
			PieceMd5: "md53",
			PieceRange: &rangeutils.Range{
				StartIndex: 300,
				EndIndex:   399,
			},
			OriginRange: &rangeutils.Range{
				StartIndex: 300,
				EndIndex:   399,
			},
			PieceLen:   100,
			PieceStyle: 0,
		},
	}
)

func (suite *ProgressManagerTestSuite) SetupSuite() {
	ctl := gomock.NewController(suite.T())
	sourceClient := sourcemock.NewMockResourceClient(ctl)
	source.UnRegister("https")
	suite.Nil(source.Register("https", sourceClient, httpprotocol.Adapter))
	sourceClient.EXPECT().GetContentLength(source.RequestEq(testTask.RawURL)).Return(int64(1024*1024*500+1000), nil).Times(1)
	taskManager, err := task.NewManager(config.New())
	suite.Nil(err)
	seedTask, err := taskManager.AddOrUpdate(testTask)
	suite.Nil(err)
	suite.Equal(int64(1024*1024*500+1000), seedTask.SourceFileLength)
	manager, err := newManager(taskManager)
	suite.Nil(err)
	suite.manager = manager
}

func (suite *ProgressManagerTestSuite) TestWatchSeedProgress() {
	// watch not exit task
	got, err := suite.manager.WatchSeedProgress(context.Background(), "clientAddr", "notExistTask")
	suite.NotNil(err)
	suite.Nil(got)
	// testTaskID has not pieces currently
	wg := sync.WaitGroup{}
	wg.Add(5)
	got1, err := suite.manager.WatchSeedProgress(context.Background(), "clientAddr1", testTaskID)
	suite.Nil(err)
	suite.NotNil(got1)
	go func() {
		defer wg.Done()
		var pieceCount uint32 = 0
		for info := range got1 {
			suite.Equal(taskPieces[pieceCount], info)
			pieceCount++
		}
		suite.Equal(len(taskPieces), int(pieceCount))
	}()
	// publish first piece
	suite.Nil(suite.manager.PublishPiece(context.Background(), testTaskID, taskPieces[0]))
	// testTaskID has one-piece currently
	got2, err := suite.manager.WatchSeedProgress(context.Background(), "clientAddr2", testTaskID)
	suite.Nil(err)
	suite.NotNil(got2)
	go func() {
		defer wg.Done()
		var pieceCount uint32 = 0
		for info := range got2 {
			suite.Equal(taskPieces[pieceCount], info)
			pieceCount++
		}
		suite.Equal(len(taskPieces), int(pieceCount))
	}()
	// publish secondary piece
	suite.Nil(suite.manager.PublishPiece(context.Background(), testTaskID, taskPieces[1]))
	// testTaskID has two-piece currently
	got3, err := suite.manager.WatchSeedProgress(context.Background(), "clientAddr3", testTaskID)
	suite.Nil(err)
	suite.NotNil(got3)
	go func() {
		defer wg.Done()
		var pieceCount uint32 = 0
		for info := range got3 {
			suite.Equal(taskPieces[pieceCount], info)
			pieceCount++
		}
		suite.Equal(len(taskPieces), int(pieceCount))
	}()
	// publish third piece
	suite.Nil(suite.manager.PublishPiece(context.Background(), testTaskID, taskPieces[2]))
	// testTaskID has three-piece currently
	got4, err := suite.manager.WatchSeedProgress(context.Background(), "clientAddr4", testTaskID)
	suite.Nil(err)
	suite.NotNil(got4)
	go func() {
		defer wg.Done()
		var pieceCount uint32 = 0
		for info := range got4 {
			suite.Equal(taskPieces[pieceCount], info)
			pieceCount++
		}
		suite.Equal(len(taskPieces), int(pieceCount))
	}()
	// publish forth piece
	suite.Nil(suite.manager.PublishPiece(context.Background(), testTaskID, taskPieces[3]))
	// publish task
	testTask.CdnStatus = task.StatusSuccess
	suite.Nil(suite.manager.PublishTask(context.Background(), testTaskID, testTask))
	// testTaskID has done currently
	got5, err := suite.manager.WatchSeedProgress(context.Background(), "clientAddr5", testTaskID)
	suite.Nil(err)
	suite.NotNil(got5)
	go func() {
		defer wg.Done()
		var pieceCount uint32 = 0
		for info := range got5 {
			suite.Equal(taskPieces[pieceCount], info)
			pieceCount++
		}
		suite.Equal(len(taskPieces), int(pieceCount))
	}()
	wg.Wait()
}
