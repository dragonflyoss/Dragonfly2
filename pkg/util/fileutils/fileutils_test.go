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

package fileutils

import (
	"fmt"
	"io/ioutil"
	"os"
	"os/user"
	"path/filepath"
	"testing"

	"d7y.io/dragonfly/v2/pkg/basic/env"
	logger "d7y.io/dragonfly/v2/pkg/dflog"
	"github.com/stretchr/testify/suite"
)

func Test(t *testing.T) {
	os.Setenv(env.ActiveProfile,"local")
	logger.InitCdnSystem()
	suite.Run(t, new(FileUtilTestSuite))
}

type FileUtilTestSuite struct {
	tmpDir   string
	username string
	suite.Suite
}

func TestDeleteFile(t *testing.T) {
	os.Remove("/Users/zuozheng.hzz/yyyyyy")




}

func (s *FileUtilTestSuite) SetupSuite() {
	s.tmpDir, _ = ioutil.TempDir("/tmp", "dfget-FileUtilTestSuite-")
	if u, e := user.Current(); e == nil {
		s.username = u.Username
	}
}

func (s *FileUtilTestSuite) TeardownSuite() {
	if s.tmpDir != "" {
		if err := os.RemoveAll(s.tmpDir); err != nil {
			fmt.Printf("remove path:%s error", s.tmpDir)
		}
	}
}

func (s *FileUtilTestSuite) TestCreateDirectory() {
	dirPath := filepath.Join(s.tmpDir, "TestCreateDirectory")
	err := MkdirAll(dirPath)
	s.Nil(err)

	f, _ := os.Create(filepath.Join(dirPath, "createFile"))
	err = MkdirAll(f.Name())
	s.NotNil(err)

	os.Chmod(dirPath, 0555)
	defer os.Chmod(dirPath, 0755)
	err = MkdirAll(filepath.Join(dirPath, "1"))
	if s.username != "root" {
		s.NotNil(err)
	} else {
		s.NotNil(err)
	}
}

func (s *FileUtilTestSuite) TestPathExists() {
	pathStr := filepath.Join(s.tmpDir, "TestPathExists")
	s.Equal(PathExist(pathStr), false)

	os.Create(pathStr)
	s.Equal(PathExist(pathStr), true)
}

func (s *FileUtilTestSuite) TestIsDir() {
	pathStr := filepath.Join(s.tmpDir, "TestIsDir")
	s.Equal(IsDir(pathStr), false)

	os.Create(pathStr)
	s.Equal(IsDir(pathStr), false)
	os.Remove(pathStr)

	os.Mkdir(pathStr, 0000)
	s.Equal(IsDir(pathStr), true)
}

func (s *FileUtilTestSuite) TestDeleteFile() {
	pathStr := filepath.Join(s.tmpDir, "TestDeleteFile")
	os.Create(pathStr)
	err := DeleteFile(pathStr)
	s.Nil(err)

	dirStr := filepath.Join(s.tmpDir, "test_delete_file")
	os.Mkdir(dirStr, 0000)
	err = DeleteFile(dirStr)
	s.NotNil(err)

	f := filepath.Join(s.tmpDir, "test", "empty", "file")
	err = DeleteFile(f)
	s.NotNil(err)
}



func (s *FileUtilTestSuite) TestOpenFile() {
	f1 := filepath.Join(s.tmpDir, "dir1", "TestOpenFile")
	_, err := OpenFile(f1, os.O_WRONLY|os.O_TRUNC|os.O_CREATE, 0755)
	s.Nil(err)

	f2 := filepath.Join(s.tmpDir, "TestOpenFile")
	os.Create(f2)
	_, err = OpenFile(f2, os.O_RDONLY, 0666)
	s.Nil(err)
}

func (s *FileUtilTestSuite) TestLink() {
	pathStr := filepath.Join(s.tmpDir, "TestLinkFile")
	os.Create(pathStr)
	linkStr := filepath.Join(s.tmpDir, "TestLinkName")

	err := Link(pathStr, linkStr)
	s.Nil(err)
	s.Equal(PathExist(linkStr), true)

	linkStr = filepath.Join(s.tmpDir, "TestLinkNameExist")
	os.Create(linkStr)
	err = Link(pathStr, linkStr)
	s.Nil(err)
	s.Equal(PathExist(linkStr), true)

	linkStr = filepath.Join(s.tmpDir, "testLinkNonExistDir")
	os.Mkdir(linkStr, 0755)
	err = Link(pathStr, linkStr)
	s.NotNil(err)
}

func (s *FileUtilTestSuite) TestSymbolicLink() {
	pathStr := filepath.Join(s.tmpDir, "TestSymLinkFileNonExist")
	linkStr := filepath.Join(s.tmpDir, "TestSymLinkNameFileNonExist")
	err := SymbolicLink(pathStr, linkStr)
	s.NotNil(err)
	s.Equal(PathExist(linkStr), false)

	pathStr = filepath.Join(s.tmpDir, "TestSymLinkDir")
	os.Mkdir(pathStr, 0755)
	linkStr = filepath.Join(s.tmpDir, "TestSymLinkNameDir")
	err = SymbolicLink(pathStr, linkStr)
	s.Nil(err)
	s.Equal(PathExist(linkStr), true)

	pathStr = filepath.Join(s.tmpDir, "TestSymLinkFile")
	os.Create(pathStr)
	linkStr = filepath.Join(s.tmpDir, "TestSymLinkNameFile")
	err = SymbolicLink(pathStr, linkStr)
	s.Nil(err)
	s.Equal(PathExist(linkStr), true)

	linkStr = filepath.Join(s.tmpDir, "TestSymLinkNameDirExist")
	os.Mkdir(linkStr, 0755)
	err = SymbolicLink(pathStr, linkStr)
	s.NotNil(err)

	linkStr = filepath.Join(s.tmpDir, "TestSymLinkNameFileExist")
	os.Create(linkStr)
	err = SymbolicLink(pathStr, linkStr)
	s.Nil(err)
}

func (s *FileUtilTestSuite) TestCopyFile() {
	srcPath := filepath.Join(s.tmpDir, "TestCopyFileSrc")
	dstPath := filepath.Join(s.tmpDir, "TestCopyFileDst")
	_, err := CopyFile(dstPath, srcPath)
	s.NotNil(err)

	os.Create(srcPath)
	os.Create(dstPath)
	ioutil.WriteFile(srcPath, []byte("Test copy file"), 0755)
	_, err = CopyFile(dstPath, srcPath)
	s.NotNil(err)

	tmpPath := filepath.Join(s.tmpDir, "TestCopyFileTmp")
	_, err = CopyFile(tmpPath, srcPath)
	s.Nil(err)
}




func (s *FileUtilTestSuite) TestIsRegularFile() {
	pathStr := filepath.Join(s.tmpDir, "TestIsRegularFile")
	s.Equal(IsRegular(pathStr), false)

	os.Create(pathStr)
	s.Equal(IsRegular(pathStr), true)
	os.Remove(pathStr)

	// Don't set mode to create a non-regular file
	os.OpenFile(pathStr, 0, 0666)
	s.Equal(IsRegular(pathStr), false)
	os.Remove(pathStr)
}

func (s *FileUtilTestSuite) TestIsEmptyDir() {
	pathStr := filepath.Join(s.tmpDir, "TestIsEmptyDir")

	// not exist
	empty, err := IsEmptyDir(pathStr)
	s.Equal(empty, false)
	s.NotNil(err)

	// not a directory
	_, _ = os.Create(pathStr)
	empty, err = IsEmptyDir(pathStr)
	s.Equal(empty, false)
	s.NotNil(err)
	_ = os.Remove(pathStr)

	// empty
	_ = os.Mkdir(pathStr, 0755)
	empty, err = IsEmptyDir(pathStr)
	s.Equal(empty, true)
	s.Nil(err)

	// not empty
	childPath := filepath.Join(pathStr, "child")
	_ = os.Mkdir(childPath, 0755)
	empty, err = IsEmptyDir(pathStr)
	s.Equal(empty, false)
	s.Nil(err)
	_ = os.Remove(pathStr)
}
