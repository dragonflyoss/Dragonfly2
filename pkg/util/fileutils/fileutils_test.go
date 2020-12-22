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
	"github.com/stretchr/testify/suite"
	"io/ioutil"
	"os"
	"os/user"
	"path/filepath"
	"testing"
)

func Test(t *testing.T) {
	suite.Run(t, new(FileUtilTestSuite))
}

type FileUtilTestSuite struct {
	tmpDir   string
	username string
	suite.Suite
}

func (s *FileUtilTestSuite) SetUpSuite() {
	s.tmpDir, _ = ioutil.TempDir("/tmp", "dfget-FileUtilTestSuite-")
	if u, e := user.Current(); e == nil {
		s.username = u.Username
	}
}

func (s *FileUtilTestSuite) TearDownSuite() {
	if s.tmpDir != "" {
		if err := os.RemoveAll(s.tmpDir); err != nil {
			fmt.Printf("remove path:%s error", s.tmpDir)
		}
	}
}

func (s *FileUtilTestSuite) TestCreateDirectory() {
	dirPath := filepath.Join(s.tmpDir, "TestCreateDirectory")
	err := CreateDirectory(dirPath)
	s.Nil(err)

	f, _ := os.Create(filepath.Join(dirPath, "createFile"))
	err = CreateDirectory(f.Name())
	s.NotNil(err)

	os.Chmod(dirPath, 0555)
	defer os.Chmod(dirPath, 0755)
	err = CreateDirectory(filepath.Join(dirPath, "1"))
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

func (s *FileUtilTestSuite) TestDeleteFiles() {
	f1 := filepath.Join(s.tmpDir, "TestDeleteFile001")
	f2 := filepath.Join(s.tmpDir, "TestDeleteFile002")
	os.Create(f1)
	DeleteFiles(f1, f2)
	s.Equal(PathExist(f1) || PathExist(f2), false)
}

func (s *FileUtilTestSuite) TestMoveFile() {

	f1 := filepath.Join(s.tmpDir, "TestMovefileSrc01")
	f2 := filepath.Join(s.tmpDir, "TestMovefileDstExist")
	os.Create(f1)
	os.Create(f2)
	ioutil.WriteFile(f1, []byte("Test move file src"), 0755)
	f1Md5 := Md5Sum(f1)
	err := MoveFile(f1, f2)
	s.Nil(err)

	f2Md5 := Md5Sum(f2)
	s.Equal(f1Md5, f2Md5)

	f3 := filepath.Join(s.tmpDir, "TestMovefileSrc02")
	f4 := filepath.Join(s.tmpDir, "TestMovefileDstNonExist")
	os.Create(f3)
	ioutil.WriteFile(f3, []byte("Test move file src when dst not exist"), 0755)
	f3Md5 := Md5Sum(f3)
	err = MoveFile(f3, f4)
	s.Nil(err)
	f4Md5 := Md5Sum(f4)
	s.Equal(f3Md5, f4Md5)

	f1 = filepath.Join(s.tmpDir, "TestMovefileSrcDir")
	os.Mkdir(f1, 0755)
	err = MoveFile(f1, f2)
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
	s.Equal(PathExist(linkStr),false)

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

func (s *FileUtilTestSuite) TestMoveFileAfterCheckMd5() {
	srcPath := filepath.Join(s.tmpDir, "TestMoveFileAfterCheckMd5Src")
	dstPath := filepath.Join(s.tmpDir, "TestMoveFileAfterCheckMd5Dst")
	os.Create(srcPath)
	ioutil.WriteFile(srcPath, []byte("Test move file after check md5"), 0755)
	srcPathMd5 := Md5Sum(srcPath)
	err := MoveFileAfterCheckMd5(srcPath, dstPath, srcPathMd5)
	s.Nil(err)
	dstPathMd5 := Md5Sum(dstPath)
	s.Equal(srcPathMd5, dstPathMd5)

	ioutil.WriteFile(srcPath, []byte("Test move file afte md5, change content"), 0755)
	err = MoveFileAfterCheckMd5(srcPath, dstPath, srcPathMd5)
	s.NotNil(err)

	srcPath = filepath.Join(s.tmpDir, "TestMoveFileAfterCheckMd5Dir")
	os.Mkdir(srcPath, 0755)
	err = MoveFileAfterCheckMd5(srcPath, dstPath, srcPathMd5)
	s.NotNil(err)
}

func (s *FileUtilTestSuite) TestMd5sum() {
	pathStr := filepath.Join(s.tmpDir, "TestMd5Sum")
	_, _ = OpenFile(pathStr, os.O_WRONLY|os.O_TRUNC|os.O_CREATE, 0000)
	pathStrMd5 := Md5Sum(pathStr)
	if s.username != "root" {
		s.Equal(pathStrMd5, "")
	} else {
		s.Equal(pathStrMd5, "d41d8cd98f00b204e9800998ecf8427e")
	}

	pathStr = filepath.Join(s.tmpDir, "TestMd5SumDir")
	os.Mkdir(pathStr, 0755)
	pathStrMd5 = Md5Sum(pathStr)
	s.Equal(pathStrMd5, "")
}

func (s *FileUtilTestSuite) TestLoadYaml() {
	type T struct {
		A int    `yaml:"a"`
		B string `yaml:"b"`
	}
	var cases = []struct {
		create   bool
		content  string
		errMsg   string
		expected *T
	}{
		{create: false, content: "", errMsg: ".*no such file or directory", expected: nil},
		{create: true, content: "a: x",
			errMsg: ".*yaml: unmarshal.*(\n.*)*", expected: nil},
		{create: true, content: "a: 1", errMsg: "", expected: &T{1, ""}},
		{
			create:   true,
			content:  "a: 1\nb: x",
			errMsg:   "",
			expected: &T{1, "x"},
		},
	}

	for idx, v := range cases {
		filename := filepath.Join(s.tmpDir, fmt.Sprintf("test-%d", idx))
		if v.create {
			ioutil.WriteFile(filename, []byte(v.content), os.ModePerm)
		}
		var t T
		err := LoadYaml(filename, &t)
		if v.expected == nil {
			s.NotNil(err)
			s.EqualError(err, v.errMsg)
		} else {
			s.Nil(err)
			s.Equal(&t, v.expected)
		}

	}
}

func (s *FileUtilTestSuite) TestIsRegularFile() {
	pathStr := filepath.Join(s.tmpDir, "TestIsRegularFile")
	s.Equal(IsRegularFile(pathStr), false)

	os.Create(pathStr)
	s.Equal(IsRegularFile(pathStr), true)
	os.Remove(pathStr)

	// Don't set mode to create a non-regular file
	os.OpenFile(pathStr, 0, 0666)
	s.Equal(IsRegularFile(pathStr), false)
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
