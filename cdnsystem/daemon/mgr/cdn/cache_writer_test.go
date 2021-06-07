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
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"testing"

	"d7y.io/dragonfly/v2/cdnsystem/config"
	"d7y.io/dragonfly/v2/cdnsystem/daemon/mgr/cdn/storage"
	"d7y.io/dragonfly/v2/cdnsystem/daemon/mgr/progress"
	"d7y.io/dragonfly/v2/cdnsystem/plugins"
	"d7y.io/dragonfly/v2/cdnsystem/types"
	"github.com/stretchr/testify/suite"
)

func TestCacheWriterSuite(t *testing.T) {
	suite.Run(t, new(CacheWriterTestSuite))
}

type CacheWriterTestSuite struct {
	workHome string
	writer   *cacheWriter
	cfg      *config.Config
	suite.Suite
}

func (s *CacheWriterTestSuite) SetupSuite() {
	s.workHome, _ = ioutil.TempDir("/tmp", "cdn-CacheWriterTestSuite-")
	plugins.Initialize(config.NewDefaultPlugins())
	storeMgr, ok := storage.Get(config.DefaultStorageMode)
	if !ok {
		s.Failf("failed to get storage mode %s", config.DefaultStorageMode)
	}
	progressMgr, _ := progress.NewManager()
	cacheDataManager := newCacheDataManager(storeMgr)
	cdnReporter := newReporter(progressMgr)
	s.writer = newCacheWriter(cdnReporter, cacheDataManager)
}

func (s *CacheWriterTestSuite) TeardownSuite() {
	if s.workHome != "" {
		if err := os.RemoveAll(s.workHome); err != nil {
			fmt.Printf("remove path: %s error", s.workHome)
		}
	}
}

func (s *CacheWriterTestSuite) TestStartWriter() {
	testStr := "hello dragonfly2"
	var httpFileLen = int64(len(testStr))
	f := strings.NewReader(testStr)

	task := &types.SeedTask{
		TaskID: "5806501cbcc3bb92f0b645918c5a4b15495a63259e3e0363008f97e186509e9e",
	}

	detectResult := &cacheResult{
		breakPoint:       0,
		pieceMetaRecords: nil,
		fileMetaData:     nil,
		fileMd5:          nil,
	}

	downloadMetadata, err := s.writer.startWriter(context.TODO(), f, task, detectResult)
	s.Nil(err)
	s.Equal(httpFileLen, downloadMetadata.realSourceFileLength)
}

func (s *CacheWriterTestSuite) TestWriteToFile() {
	testStr := "hello dragonfly"

	var bb = bytes.NewBufferString(testStr)

	task := &types.SeedTask{
		TaskID: "5816501cbcc3bb92f0b645918c5a4b15495a63259e3e0363008f97e186509e9e",
	}

	err := s.writer.writeToFile(task.TaskID, bb, 0, nil)
	s.Nil(err)

	s.checkFileSize(s.writer.cacheDataManager.storage, task.TaskID, int64(len(testStr)))
}

func (s *CacheWriterTestSuite) checkFileSize(cdnStore storage.Manager, taskID string, expectedSize int64) {
	storageInfo, err := cdnStore.StatDownloadFile(taskID)
	s.Nil(err)
	s.Equal(expectedSize, storageInfo.Size)
}
