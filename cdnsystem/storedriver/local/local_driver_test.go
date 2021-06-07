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

package local

import (
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"

	"d7y.io/dragonfly/v2/cdnsystem/cdnerrors"
	"d7y.io/dragonfly/v2/cdnsystem/plugins"
	"d7y.io/dragonfly/v2/cdnsystem/storedriver"
	"d7y.io/dragonfly/v2/pkg/unit"
	"d7y.io/dragonfly/v2/pkg/util/statutils"
	"github.com/stretchr/testify/suite"
)

func TestLocalDriverTestSuite(t *testing.T) {
	suite.Run(t, new(LocalDriverTestSuite))
}

type LocalDriverTestSuite struct {
	suite.Suite
	workHome string
	storedriver.Driver
}

func (s *LocalDriverTestSuite) SetupSuite() {
	s.workHome, _ = ioutil.TempDir("/tmp", "cdn-local-driver-repo")
	pluginProps := map[plugins.PluginType][]*plugins.PluginProperties{
		plugins.StorageDriverPlugin: {
			&plugins.PluginProperties{
				Name:   DiskDriverName,
				Enable: true,
				Config: map[string]string{"baseDir": filepath.Join(s.workHome, "repo")},
			},
		},
	}
	s.Nil(plugins.Initialize(pluginProps))
	gotDriver, ok := storedriver.Get(DiskDriverName)
	s.True(ok)
	s.Equal(&driver{BaseDir: filepath.Join(s.workHome, "repo")}, gotDriver)
	s.Driver = gotDriver
}

func (s *LocalDriverTestSuite) TeardownSuite() {
	if s.workHome != "" {
		s.Nil(os.RemoveAll(s.workHome))
	}
}

func (s *LocalDriverTestSuite) TestGetPutBytes() {
	var cases = []struct {
		name        string
		putRaw      *storedriver.Raw
		getRaw      *storedriver.Raw
		data        []byte
		getErrCheck func(error) bool
		putErrCheck func(error) bool
		expected    string
	}{
		{
			name: "get put full",
			putRaw: &storedriver.Raw{
				Bucket: "GetPut",
				Key:    "foo1",
			},
			getRaw: &storedriver.Raw{
				Bucket: "GetPut",
				Key:    "foo1",
			},
			data:        []byte("hello foo"),
			putErrCheck: isNil,
			getErrCheck: isNil,
			expected:    "hello foo",
		}, {
			name: "get specific length",
			putRaw: &storedriver.Raw{
				Bucket: "GetPut",
				Key:    "foo2",
			},
			getRaw: &storedriver.Raw{
				Bucket: "GetPut",
				Key:    "foo2",
				Offset: 0,
				Length: 5,
			},
			putErrCheck: isNil,
			getErrCheck: isNil,
			data:        []byte("hello foo"),
			expected:    "hello",
		}, {
			name: "get full length",
			putRaw: &storedriver.Raw{
				Bucket: "GetPut",
				Key:    "foo3",
			},
			getRaw: &storedriver.Raw{
				Bucket: "GetPut",
				Key:    "foo3",
				Offset: 0,
				Length: 0,
			},
			putErrCheck: isNil,
			getErrCheck: isNil,
			data:        []byte("hello foo"),
			expected:    "hello foo",
		}, {
			name: "get invalid length",
			putRaw: &storedriver.Raw{
				Bucket: "GetPut",
				Key:    "foo4",
			},
			getRaw: &storedriver.Raw{
				Bucket: "GetPut",
				Key:    "foo4",
				Offset: 0,
				Length: -1,
			},
			putErrCheck: isNil,
			getErrCheck: cdnerrors.IsInvalidValue,
			data:        []byte("hello foo"),
			expected:    "",
		}, {
			name: "put specific length",
			putRaw: &storedriver.Raw{
				Bucket: "GetPut",
				Key:    "foo5",
				Length: 5,
			},
			getRaw: &storedriver.Raw{
				Bucket: "GetPut",
				Key:    "foo5",
			},
			putErrCheck: isNil,
			getErrCheck: isNil,
			data:        []byte("hello foo"),
			expected:    "hello",
		}, {
			name: "get invalid offset",
			putRaw: &storedriver.Raw{
				Bucket: "GetPut",
				Key:    "foo6",
			},
			getRaw: &storedriver.Raw{
				Bucket: "GetPut",
				Key:    "foo6",
				Offset: -1,
			},
			data:        []byte("hello foo"),
			putErrCheck: isNil,
			getErrCheck: cdnerrors.IsInvalidValue,
			expected:    "",
		}, {
			name: "put/get data from specific offset",
			putRaw: &storedriver.Raw{
				Bucket: "GetPut",
				Key:    "foo7",
				Offset: 3,
			},
			getRaw: &storedriver.Raw{
				Bucket: "GetPut",
				Key:    "foo7",
				Offset: 3,
			},
			data:        []byte("hello foo"),
			putErrCheck: isNil,
			getErrCheck: isNil,
			expected:    "hello foo",
		},
	}

	for _, v := range cases {
		s.Run(v.name, func() {
			// put
			err := s.PutBytes(v.putRaw, v.data)
			s.True(v.putErrCheck(err))
			// get
			result, err := s.GetBytes(v.getRaw)
			s.True(v.getErrCheck(err))
			s.Equal(v.expected, string(result))
			// stat
			s.checkStat(v.getRaw)
			// remove
			s.checkRemove(v.putRaw)
		})
	}
}

func (s *LocalDriverTestSuite) TestGetPut() {
	var cases = []struct {
		name        string
		putRaw      *storedriver.Raw
		getRaw      *storedriver.Raw
		data        io.Reader
		getErrCheck func(error) bool
		expected    string
	}{
		{
			putRaw: &storedriver.Raw{
				Key:    "foo0.meta",
				Length: 15,
			},
			getRaw: &storedriver.Raw{
				Key: "foo0.meta",
			},
			data:        strings.NewReader("hello meta file"),
			getErrCheck: isNil,
			expected:    "hello meta file",
		}, {
			putRaw: &storedriver.Raw{
				Key: "foo1.meta",
			},
			getRaw: &storedriver.Raw{
				Key: "foo1.meta",
			},
			data:        strings.NewReader("hello meta file"),
			getErrCheck: isNil,
			expected:    "hello meta file",
		}, {
			putRaw: &storedriver.Raw{
				Key:    "foo2.meta",
				Length: 6,
			},
			getRaw: &storedriver.Raw{
				Key: "foo2.meta",
			},
			data:        strings.NewReader("hello meta file"),
			getErrCheck: isNil,
			expected:    "hello ",
		}, {
			putRaw: &storedriver.Raw{
				Key: "foo3.meta",
			},
			getRaw: &storedriver.Raw{
				Key:    "foo3.meta",
				Offset: 2,
				Length: 5,
			},
			data:        strings.NewReader("hello meta file"),
			getErrCheck: isNil,
			expected:    "llo m",
		}, {
			putRaw: &storedriver.Raw{
				Key: "foo4.meta",
			},
			getRaw: &storedriver.Raw{
				Key:    "foo4.meta",
				Offset: 2,
				Length: -1,
			},
			getErrCheck: cdnerrors.IsInvalidValue,
			data:        strings.NewReader("hello meta file"),
			expected:    "",
		}, {
			putRaw: &storedriver.Raw{
				Key: "foo5.meta",
			},
			getRaw: &storedriver.Raw{
				Key:    "foo5.meta",
				Offset: 30,
				Length: 5,
			},
			getErrCheck: cdnerrors.IsInvalidValue,
			data:        strings.NewReader("hello meta file"),
			expected:    "",
		},
	}

	for _, v := range cases {
		s.Run(v.name, func() {
			// put
			err := s.Put(v.putRaw, v.data)
			s.Nil(err)
			// get
			r, err := s.Get(v.getRaw)
			s.True(v.getErrCheck(err))
			if err == nil {
				result, err := ioutil.ReadAll(r)
				s.Nil(err)
				s.Equal(v.expected, string(result))
			}

			// stat
			s.checkStat(v.putRaw)

			// remove
			s.checkRemove(v.putRaw)
		})
	}
}

func (s *LocalDriverTestSuite) TestAppendBytes() {
	var cases = []struct {
		name        string
		putRaw      *storedriver.Raw
		appendRaw   *storedriver.Raw
		getRaw      *storedriver.Raw
		data        io.Reader
		appData     io.Reader
		getErrCheck func(error) bool
		expected    string
	}{
		{
			name: "foo0.meta",
			putRaw: &storedriver.Raw{
				Key:    "foo0.meta",
				Length: 13,
			},
			appendRaw: &storedriver.Raw{
				Key:    "foo0.meta",
				Length: 3,
				Append: true,
			},
			getRaw: &storedriver.Raw{
				Key: "foo0.meta",
			},
			data:        strings.NewReader("hello meta fi"),
			appData:     strings.NewReader("append data"),
			getErrCheck: isNil,
			expected:    "hello meta fiapp",
		}, {
			name: "foo1.meta",
			putRaw: &storedriver.Raw{
				Key: "foo1.meta",
			},
			appendRaw: &storedriver.Raw{
				Key:    "foo1.meta",
				Length: 3,
				Append: true,
			},
			getRaw: &storedriver.Raw{
				Key: "foo1.meta",
			},
			data:        strings.NewReader("hello meta file"),
			appData:     strings.NewReader("append data"),
			getErrCheck: isNil,
			expected:    "hello meta fileapp",
		}, {
			name: "foo2.meta",
			putRaw: &storedriver.Raw{
				Key:    "foo2.meta",
				Length: 6,
			},
			appendRaw: &storedriver.Raw{
				Key:    "foo0.meta",
				Length: 3,
				Append: true,
			},
			getRaw: &storedriver.Raw{
				Key: "foo2.meta",
			},
			data:        strings.NewReader("hello meta file"),
			getErrCheck: isNil,
			expected:    "hello ",
		},
	}
	for _, v := range cases {
		s.Run(v.name, func() {
			// put
			err := s.Put(v.putRaw, v.data)
			s.Nil(err)
			err = s.Put(v.appendRaw, v.appData)
			s.Nil(err)
			// get
			r, err := s.Get(v.getRaw)
			s.True(v.getErrCheck(err))
			if err == nil {
				result, err := ioutil.ReadAll(r)
				s.Nil(err)
				s.Equal(v.expected, string(result))
			}

			// stat
			s.checkStat(v.putRaw)

			// remove
			s.checkRemove(v.putRaw)
		})
	}
}

func (s *LocalDriverTestSuite) TestPutTrunc() {
	originRaw := &storedriver.Raw{
		Key:    "fooTrunc.meta",
		Offset: 0,
		Trunc:  true,
	}
	originData := "hello world"

	var cases = []struct {
		truncRaw     *storedriver.Raw
		getErrCheck  func(error) bool
		data         io.Reader
		expectedData string
	}{
		{
			truncRaw: &storedriver.Raw{
				Key:    "fooTrunc.meta",
				Offset: 0,
				Trunc:  true,
			},
			data:         strings.NewReader("hello"),
			getErrCheck:  isNil,
			expectedData: "hello",
		}, {
			truncRaw: &storedriver.Raw{
				Key:    "fooTrunc.meta",
				Offset: 6,
				Trunc:  true,
			},
			data:         strings.NewReader("golang"),
			getErrCheck:  isNil,
			expectedData: "\x00\x00\x00\x00\x00\x00golang",
		}, {
			truncRaw: &storedriver.Raw{
				Key:    "fooTrunc.meta",
				Offset: 0,
				Trunc:  false,
			},
			data:         strings.NewReader("foo"),
			getErrCheck:  isNil,
			expectedData: "foolo world",
		}, {
			truncRaw: &storedriver.Raw{
				Key:    "fooTrunc.meta",
				Offset: 6,
				Trunc:  false,
			},
			data:         strings.NewReader("foo"),
			getErrCheck:  isNil,
			expectedData: "hello foold",
		},
	}

	for _, v := range cases {
		err := s.Put(originRaw, strings.NewReader(originData))
		s.Nil(err)

		err = s.Put(v.truncRaw, v.data)
		s.Nil(err)

		r, err := s.Get(&storedriver.Raw{
			Key: "fooTrunc.meta",
		})
		s.Nil(err)

		if err == nil {
			result, err := ioutil.ReadAll(r)
			s.Nil(err)
			s.Equal(string(result[:]), v.expectedData)
		}
	}
}

func (s *LocalDriverTestSuite) TestPutParallel() {
	var key = "fooPutParallel"
	var routineCount = 4
	var testStr = "hello"
	var testStrLength = len(testStr)

	var wg sync.WaitGroup
	for k := 0; k < routineCount; k++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			s.Nil(s.Put(&storedriver.Raw{
				Key:    key,
				Offset: int64(i) * int64(testStrLength),
			}, strings.NewReader(testStr)))
		}(k)
	}
	wg.Wait()

	info, err := s.Stat(&storedriver.Raw{Key: key})
	s.Nil(err)
	s.Equal(info.Size, int64(routineCount)*int64(testStrLength))
}

func (s *LocalDriverTestSuite) TestLocalDriverExitsAndRemove() {
	raw := &storedriver.Raw{
		Bucket: "existsTest",
		Key:    "test",
	}
	s.Nil(s.PutBytes(raw, []byte("hello world")))
	s.True(s.Exits(raw))
	s.Nil(s.Remove(raw))
	s.False(s.Exits(raw))
}

func (s *LocalDriverTestSuite) TestLocalDriverGetHomePath() {
	s.Equal(filepath.Join(s.workHome, "repo"), s.GetHomePath())
}

func (s *LocalDriverTestSuite) TestLocalDriverGetPath() {
	raw := &storedriver.Raw{
		Bucket: "dir",
		Key:    "path",
	}
	path := filepath.Join(s.workHome, "repo", "dir", "path")
	s.Equal(path, s.GetPath(raw))
}

func (s *LocalDriverTestSuite) TestLocalDriverGetTotalAndFreeSpace() {
	fs := syscall.Statfs_t{}
	s.Nil(syscall.Statfs(s.GetHomePath(), &fs))
	total := unit.Bytes(fs.Blocks * uint64(fs.Bsize))
	free := unit.Bytes(fs.Bavail * uint64(fs.Bsize))
	got, got1, err := s.GetTotalAndFreeSpace()
	s.Nil(err)
	s.Equal(total, got)
	s.Equal(free, got1)
}

func (s *LocalDriverTestSuite) BenchmarkPutParallel() {
	var wg sync.WaitGroup
	for k := 0; k < 1000; k++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			s.Nil(s.Put(&storedriver.Raw{
				Key:    "foo.bench",
				Offset: int64(i) * 5,
			}, strings.NewReader("hello")))
		}(k)
	}
	wg.Wait()
}

func (s *LocalDriverTestSuite) BenchmarkPutSerial() {
	for k := 0; k < 1000; k++ {
		s.Nil(s.Put(&storedriver.Raw{
			Key:    "foo1.bench",
			Offset: int64(k) * 5,
		}, strings.NewReader("hello")))
	}
}

func (s *LocalDriverTestSuite) checkStat(raw *storedriver.Raw) {
	info, err := s.Stat(raw)
	s.Equal(isNil(err), true)

	pathTemp := filepath.Join(s.Driver.GetHomePath(), raw.Bucket, raw.Key)
	f, _ := os.Stat(pathTemp)

	s.EqualValues(info, &storedriver.StorageInfo{
		Path:       filepath.Join(raw.Bucket, raw.Key),
		Size:       f.Size(),
		ModTime:    f.ModTime(),
		CreateTime: statutils.Ctime(f),
	})
}

func (s *LocalDriverTestSuite) checkRemove(raw *storedriver.Raw) {
	err := s.Remove(raw)
	s.Equal(isNil(err), true)

	_, err = s.Stat(raw)
	s.Equal(cdnerrors.IsFileNotExist(err), true)
}

func isNil(err error) bool {
	return err == nil
}
