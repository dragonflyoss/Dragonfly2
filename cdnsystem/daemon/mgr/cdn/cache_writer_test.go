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
	"d7y.io/dragonfly/v2/cdnsystem/store"
	"d7y.io/dragonfly/v2/cdnsystem/store/disk"
	"fmt"
	"github.com/stretchr/testify/suite"
	"io/ioutil"
	"os"
	"testing"
)

func TestCacheWriter(t *testing.T) {
	suite.Run(t, new(CacheWriterTestSuite))
}

type CacheWriterTestSuite struct {
	workHome string
	config   string
	writer   *cacheWriter
	suite.Suite
}

func (s *CacheWriterTestSuite) SetupSuite() {
	s.workHome, _ = ioutil.TempDir("/tmp", "cdn-CacheWriterTestSuite-")
	s.config = "baseDir: " + s.workHome
	fileStore, err := store.NewStore(disk.StorageDriver, disk.NewStorage, s.config)
	s.Nil(err)
	s.writer = newCacheWriter(fileStore, nil, nil)
}

func (s *CacheWriterTestSuite) TeardownSuite() {
	if s.workHome != "" {
		if err := os.RemoveAll(s.workHome); err != nil {
			fmt.Printf("remove path: %s error", s.workHome)
		}
	}
}

func (s *CacheWriterTestSuite) TestStartWriter() {

}

func (s *CacheWriterTestSuite) TestWriteToFile() {

}

func (s *CacheWriterTestSuite) TestAppendPieceMetaDataToFile() {

}
