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

package memory

import (
	"context"
	"d7y.io/dragonfly/v2/cdnsystem/cdnerrors"
	"d7y.io/dragonfly/v2/cdnsystem/config"
	"d7y.io/dragonfly/v2/cdnsystem/plugins"
	"d7y.io/dragonfly/v2/cdnsystem/store"
	"fmt"
	"github.com/stretchr/testify/suite"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func Test(t *testing.T) {
	suite.Run(t, new(LocalStorageSuite))
}

type LocalStorageSuite struct {
	workHome   string
	storeLocal *store.Store
	suite.Suite
}

func (s *LocalStorageSuite) SetupSuite() {
	s.workHome, _ = ioutil.TempDir("/tmp", "cdn-storageDriver-StoreTestSuite-")
	pluginProps := map[config.PluginType][]*config.PluginProperties{
		config.StoragePlugin: {
			&config.PluginProperties{
				Name:    StorageDriver,
				Enabled: true,
				Config:  "baseDir: " + filepath.Join(s.workHome, "repo"),
			},
		},
	}
	cfg := &config.Config{
		Plugins: pluginProps,
	}
	plugins.Initialize(cfg)

	// init StorageManager
	sm, err := store.NewManager(nil)
	s.Nil(err)

	// init store with local storage
	s.storeLocal, err = sm.Get(StorageDriver)
	s.Nil(err)
}

func (s *LocalStorageSuite) TearDownSuite() {
	if s.workHome != "" {
		if err := os.RemoveAll(s.workHome); err != nil {
			fmt.Printf("remove path:%s error", s.workHome)
		}
	}
}

func (s *LocalStorageSuite) TestGetPutBytes() {
	var cases = []struct {
		putRaw      *store.Raw
		getRaw      *store.Raw
		data        []byte
		getErrCheck func(error) bool
		expected    string
	}{
		{
			putRaw: &store.Raw{
				Key: "foo1",
			},
			getRaw: &store.Raw{
				Key: "foo1",
			},
			data:        []byte("hello foo"),
			getErrCheck: cdnerrors.IsNilError,
			expected:    "hello foo",
		},
		{
			putRaw: &store.Raw{
				Key: "foo2",
			},
			getRaw: &store.Raw{
				Key:    "foo2",
				Offset: 0,
				Length: 5,
			},
			getErrCheck: cdnerrors.IsNilError,
			data:        []byte("hello foo"),
			expected:    "hello",
		},
		{
			putRaw: &store.Raw{
				Key: "foo3",
			},
			getRaw: &store.Raw{
				Key:    "foo3",
				Offset: 0,
				Length: -1,
			},
			getErrCheck: cdnerrors.IsInvalidValue,
			data:        []byte("hello foo"),
			expected:    "",
		},
		{
			putRaw: &store.Raw{
				Bucket: "download",
				Key:    "foo4",
				Length: 5,
			},
			getRaw: &store.Raw{
				Bucket: "download",
				Key:    "foo4",
			},
			getErrCheck: cdnerrors.IsNilError,
			data:        []byte("hello foo"),
			expected:    "hello",
		},
		{
			putRaw: &store.Raw{
				Bucket: "download",
				Key:    "foo0/foo.txt",
			},
			getRaw: &store.Raw{
				Bucket: "download",
				Key:    "foo0/foo.txt",
			},
			data:        []byte("hello foo"),
			getErrCheck: cdnerrors.IsNilError,
			expected:    "hello foo",
		},
	}

	for _, v := range cases {
		// put
		err := s.storeLocal.PutBytes(context.Background(), v.putRaw, v.data)
		s.Nil(err)

		// get
		result, err := s.storeLocal.GetBytes(context.Background(), v.getRaw)
		s.Equal(v.getErrCheck(err), true)
		if err == nil {
			s.Equal(string(result), v.expected)
		}

	}

}

func (s *LocalStorageSuite) TestGetPut() {
	var cases = []struct {
		putRaw      *store.Raw
		getRaw      *store.Raw
		data        io.Reader
		getErrCheck func(error) bool
		expected    string
	}{
		{
			putRaw: &store.Raw{
				Key:    "foo0.meta",
				Length: 15,
			},
			getRaw: &store.Raw{
				Key: "foo0.meta",
			},
			data:        strings.NewReader("hello meta file"),
			getErrCheck: cdnerrors.IsNilError,
			expected:    "hello meta file",
		},
		{
			putRaw: &store.Raw{
				Key: "foo1.meta",
			},
			getRaw: &store.Raw{
				Key: "foo1.meta",
			},
			data:        strings.NewReader("hello meta file"),
			getErrCheck: cdnerrors.IsNilError,
			expected:    "hello meta file",
		},
		{
			putRaw: &store.Raw{
				Key: "foo2.meta",
			},
			getRaw: &store.Raw{
				Key:    "foo2.meta",
				Offset: 2,
				Length: 5,
			},
			data:        strings.NewReader("hello meta file"),
			getErrCheck: cdnerrors.IsNilError,
			expected:    "llo m",
		},
		{
			putRaw: &store.Raw{
				Key: "foo3.meta",
			},
			getRaw: &store.Raw{
				Key:    "foo3.meta",
				Offset: 2,
				Length: -1,
			},
			getErrCheck: cdnerrors.IsInvalidValue,
			data:        strings.NewReader("hello meta file"),
			expected:    "llo meta file",
		},
		{
			putRaw: &store.Raw{
				Key: "foo4.meta",
			},
			getRaw: &store.Raw{
				Key:    "foo4.meta",
				Offset: 30,
				Length: 5,
			},
			getErrCheck: cdnerrors.IsRangeNotSatisfiable,
			data:        strings.NewReader("hello meta file"),
			expected:    "",
		},
	}

	for _, v := range cases {
		// put
		s.storeLocal.Put(context.Background(), v.putRaw, v.data)

		// get
		r, err := s.storeLocal.Get(context.Background(), v.getRaw)
		s.Equal(v.getErrCheck(err), true)
		if err == nil {
			result, err := ioutil.ReadAll(r)
			s.Nil(err)
			s.Equal(string(result[:]), v.expected)
		}

	}

}

func (s *LocalStorageSuite) TestPutTrunc() {
	originRaw := &store.Raw{
		Key:    "fooTrunc.meta",
		Offset: 0,
		Trunc:  true,
	}
	originData := "hello world"

	var cases = []struct {
		truncRaw     *store.Raw
		getErrCheck  func(error) bool
		data         io.Reader
		expectedData string
	}{
		{
			truncRaw: &store.Raw{
				Key:    "fooTrunc.meta",
				Offset: 0,
				Trunc:  true,
			},
			data:         strings.NewReader("hello"),
			getErrCheck:  cdnerrors.IsNilError,
			expectedData: "hello",
		},
		{
			truncRaw: &store.Raw{
				Key:    "fooTrunc.meta",
				Offset: 6,
				Trunc:  true,
			},
			data:         strings.NewReader("golang"),
			getErrCheck:  cdnerrors.IsNilError,
			expectedData: "\x00\x00\x00\x00\x00\x00golang",
		},
		{
			truncRaw: &store.Raw{
				Key:    "fooTrunc.meta",
				Offset: 0,
				Trunc:  false,
			},
			data:         strings.NewReader("foo"),
			getErrCheck:  cdnerrors.IsNilError,
			expectedData: "foolo world",
		},
		{
			truncRaw: &store.Raw{
				Key:    "fooTrunc.meta",
				Offset: 6,
				Trunc:  false,
			},
			data:         strings.NewReader("foo"),
			getErrCheck:  cdnerrors.IsNilError,
			expectedData: "hello foold",
		},
	}

	for _, v := range cases {
		err := s.storeLocal.Put(context.Background(), originRaw, strings.NewReader(originData))
		s.Nil(err)

		err = s.storeLocal.Put(context.Background(), v.truncRaw, v.data)
		s.Nil(err)

		r, err := s.storeLocal.Get(context.Background(), &store.Raw{
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

func (s *LocalStorageSuite) TestPutParallel() {
	var key = "fooPutParallel"
	var routineCount = 4
	var testStr = "hello"
	var testStrLength = len(testStr)

	var wg sync.WaitGroup
	for k := 0; k < routineCount; k++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			s.storeLocal.Put(context.TODO(), &store.Raw{
				Key:    key,
				Offset: int64(i) * int64(testStrLength),
			}, strings.NewReader(testStr))
		}(k)
	}
	wg.Wait()

	info, err := s.storeLocal.Stat(context.TODO(), &store.Raw{Key: key})
	s.Nil(err)
	s.Equal(info.Size, int64(routineCount)*int64(testStrLength))
}

func (s *LocalStorageSuite) BenchmarkPutParallel() {
	var wg sync.WaitGroup
	for k := 0; k < 1000; k++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			s.storeLocal.Put(context.Background(), &store.Raw{
				Key:    "foo.bech",
				Offset: int64(i) * 5,
			}, strings.NewReader("hello"))
		}(k)
	}
	wg.Wait()
}

func (s *LocalStorageSuite) BenchmarkPutSerial() {
	for k := 0; k < 1000; k++ {
		s.storeLocal.Put(context.Background(), &store.Raw{
			Key:    "foo1.bech",
			Offset: int64(k) * 5,
		}, strings.NewReader("hello"))
	}
}
