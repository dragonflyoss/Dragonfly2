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

package test

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"d7y.io/dragonfly/v2/pkg/basic"
	"d7y.io/dragonfly/v2/pkg/util/fileutils"
	"d7y.io/dragonfly/v2/pkg/util/fileutils/filerw"
	"d7y.io/dragonfly/v2/pkg/util/fileutils/fsize"
	"d7y.io/dragonfly/v2/pkg/util/statutils"
	"d7y.io/dragonfly/v2/pkg/util/stringutils"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type FileUtilsTestSuite struct {
	suite.Suite
	workDir  string
	username string
	testDir  string
	testFile string
}

func Test(t *testing.T) {
	suite.Run(t, new(FileUtilsTestSuite))
}

func (s *FileUtilsTestSuite) SetupSuite() {
	s.workDir, _ = ioutil.TempDir(basic.TmpDir, "DF-FileUtilsTestSuite-")
	s.username = basic.Username
}

func (s *FileUtilsTestSuite) TearDownSuite() {
	if !stringutils.IsBlank(s.workDir) {
		if err := os.RemoveAll(s.workDir); err != nil {
			fmt.Printf("remove path %s error:%v\n", s.workDir, err)
		}
	}
}

func (s *FileUtilsTestSuite) BeforeTest(suiteName, testName string) () {
	s.testDir = filepath.Join(s.workDir, testName)
	s.testFile = filepath.Join(s.testDir, uuid.New().String())
}

func (s *FileUtilsTestSuite) TestMkdirAll() {
	err := fileutils.MkdirAll(s.testDir)
	s.Assert().Nil(err)
}

func (s *FileUtilsTestSuite) TestDeleteFile() {
	err := fileutils.DeleteFile(s.testFile)
	s.Require().Nil(err)

	_, err = fileutils.OpenFile(s.testFile, syscall.O_CREAT|syscall.O_RDONLY, 0644)
	s.Require().Nil(err)

	s.Require().True(fileutils.PathExist(s.testFile))
	err = fileutils.DeleteFile(s.testFile)
	s.Require().False(fileutils.PathExist(s.testFile))
}

func (s *FileUtilsTestSuite) TestOpenFile() {
	_, err := fileutils.OpenFile(s.testFile, syscall.O_CREAT|syscall.O_RDONLY, 0644)
	s.Assert().Nil(err)
}

func (s *FileUtilsTestSuite) TestLink() {
	_, err := fileutils.OpenFile(s.testFile, syscall.O_CREAT|syscall.O_RDONLY, 0644)
	s.Require().Nil(err)

	err = fileutils.Link(s.testFile, s.testFile+".link")
	s.Require().Nil(err)

	info, err := os.Stat(s.testFile + ".link")
	s.Require().Nil(err)
	st := statutils.GetSysStat(info)
	s.Require().NotNil(st)

	s.Require().Equal(uint16(2), st.Nlink)

	s.Require().True(fileutils.PathExist(s.testFile + ".link"))

	s.Require().NotNil(fileutils.Link(s.testDir, s.testDir+".link"))
}

func (s *FileUtilsTestSuite) TestSymbolicLink() {
	_, err := fileutils.OpenFile(s.testFile, syscall.O_CREAT|syscall.O_RDONLY, 0644)
	s.Require().Nil(err)

	err = fileutils.SymbolicLink(s.testFile, s.testFile+".symbol")
	s.Require().Nil(err)

	info, err := os.Lstat(s.testFile + ".symbol")
	s.Require().Nil(err)

	st := statutils.GetSysStat(info)
	s.Require().NotNil(st)

	err = fileutils.DeleteFile(s.testFile + ".symbol")
	s.Require().Nil(err)

	s.Require().True(fileutils.PathExist(s.testFile))
}

func (s *FileUtilsTestSuite) TestIsEmptyDir() {
	_, err := fileutils.IsEmptyDir(s.testFile)
	s.Require().NotNil(err)
	_, err = fileutils.IsEmptyDir(s.testDir)
	s.Require().NotNil(err)

	fileutils.MkdirAll(s.testDir)
	b, err := fileutils.IsEmptyDir(s.testDir)
	s.Require().Nil(err)
	s.Require().True(b)
}

func (s *FileUtilsTestSuite) TestCopyFile() {
	_, err := filerw.CopyFile(s.testFile, s.testFile+".new")
	s.Require().NotNil(err)

	f, err := fileutils.OpenFile(s.testFile, syscall.O_WRONLY|syscall.O_CREAT, 0644)
	s.Require().Nil(err)
	f.WriteString("hello,world")
	f.Close()

	_, err = filerw.CopyFile(s.testFile, s.testFile+".new")
	s.Require().Nil(err)

	content, err := ioutil.ReadFile(s.testFile + ".new")
	s.Require().Nil(err)
	s.Require().Equal("hello,world", string(content))
}

func (s *FileUtilsTestSuite) TestTryLock() {
	f1, err := fileutils.NewFileLock(s.testFile)
	s.Require().Nil(err)

	f2, err := fileutils.NewFileLock(s.testFile)
	s.Require().Nil(err)

	f1.Lock()

	err = f2.TryLock()
	s.Require().NotNil(err)

	f1.Unlock()

	err = f2.TryLock()
	s.Require().Nil(err)
}

func TestFSizeSet(t *testing.T) {
	var num = fsize.ToFsize(0)
	var sizePtr = &num
	err := sizePtr.Set("11m")
	assert.Nil(t, err)
	assert.Equal(t, 11*fsize.MB, *sizePtr)
}
