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

package disk

import (
	"context"
	"d7y.io/dragonfly/v2/cdnsystem/cdnerrors"
	"d7y.io/dragonfly/v2/cdnsystem/store"
	"d7y.io/dragonfly/v2/pkg/util/fileutils/fsize"
	"d7y.io/dragonfly/v2/pkg/util/statutils"
	"fmt"
	"github.com/stretchr/testify/suite"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
)

func TestStorageSuite(t *testing.T) {
	suite.Run(t, new(StorageTestSuite))
}

type StorageTestSuite struct {
	workHome string
	store.StorageDriver
	suite.Suite
}

func (s *StorageTestSuite) SetupSuite() {
	s.workHome, _ = ioutil.TempDir("/tmp", "cdn-StoreTestSuite-repo")
	store, err := NewStorage(map[string]interface{}{
		"baseDir": s.workHome,
		//"gcConfig": map[string]interface{}{
		//	"youngGCThreshold":  "100G",
		//	"fullGCThreshold":   "5G",
		//	"cleanRatio":        1,
		//	"intervalThreshold": "2h",
		//},
	})
	s.Nil(err)
	s.NotNil(store)
	s.StorageDriver = store
}

func (s *StorageTestSuite) TeardownSuite() {
	if s.workHome != "" {
		if err := os.RemoveAll(s.workHome); err != nil {
			fmt.Errorf("remove path:%s error", s.workHome)
		}
	}
}

func (s *StorageTestSuite) TestGetPutBytes() {
	var cases = []struct {
		name        string
		putRaw      *store.Raw
		getRaw      *store.Raw
		data        []byte
		getErrCheck func(error) bool
		expected    string
	}{
		{
			putRaw: &store.Raw{
				Bucket: "GetPut",
				Key:    "foo1",
			},
			getRaw: &store.Raw{
				Bucket: "GetPut",
				Key:    "foo1",
			},
			data:        []byte("hello foo"),
			getErrCheck: cdnerrors.IsNilError,
			expected:    "hello foo",
		}, {
			putRaw: &store.Raw{
				Bucket: "GetPut",
				Key:    "foo2",
			},
			getRaw: &store.Raw{
				Bucket: "GetPut",
				Key:    "foo2",
				Offset: 0,
				Length: 5,
			},
			getErrCheck: cdnerrors.IsNilError,
			data:        []byte("hello foo"),
			expected:    "hello",
		}, {
			putRaw: &store.Raw{
				Bucket: "GetPut",
				Key:    "foo3",
			},
			getRaw: &store.Raw{
				Bucket: "GetPut",
				Key:    "foo3",
				Offset: 0,
				Length: 0,
			},
			getErrCheck: cdnerrors.IsNilError,
			data:        []byte("hello foo"),
			expected:    "hello foo",
		}, {
			putRaw: &store.Raw{
				Bucket: "GetPut",
				Key:    "foo4",
			},
			getRaw: &store.Raw{
				Bucket: "GetPut",
				Key:    "foo4",
				Offset: 0,
				Length: -1,
			},
			getErrCheck: cdnerrors.IsInvalidValue,
			data:        []byte("hello foo"),
			expected:    "",
		}, {
			putRaw: &store.Raw{
				Bucket: "GetPut",
				Key:    "foo5",
				Length: 5,
			},
			getRaw: &store.Raw{
				Bucket: "GetPut",
				Key:    "foo5",
			},
			getErrCheck: cdnerrors.IsNilError,
			data:        []byte("hello foo"),
			expected:    "hello",
		}, {
			putRaw: &store.Raw{
				Bucket: "GetPut",
				Key:    "foo6",
			},
			getRaw: &store.Raw{
				Bucket: "GetPut",
				Key:    "foo6",
				Offset: -1,
			},
			data:        []byte("hello foo"),
			getErrCheck: cdnerrors.IsInvalidValue,
			expected:    "",
		}, {
			name: "offset不从0开始",
			putRaw: &store.Raw{
				Bucket: "GetPut",
				Key:    "foo7",
				Offset: 3,
			},
			getRaw: &store.Raw{
				Bucket: "GetPut",
				Key:    "foo7",
				Offset: 3,
			},
			data:        []byte("hello foo"),
			getErrCheck: cdnerrors.IsNilError,
			expected:    "hello foo",
		},
	}

	for _, v := range cases {
		s.Run(v.name, func() {
			// put
			err := s.PutBytes(context.Background(), v.putRaw, v.data)
			s.Nil(err)

			// get
			result, err := s.GetBytes(context.Background(), v.getRaw)
			s.True(v.getErrCheck(err))
			s.Equal(v.expected, string(result))
			// stat
			s.checkStat(v.getRaw)
			// remove
			s.checkRemove(v.putRaw)
		})
	}
}

func (s *StorageTestSuite) TestGetPut() {
	var cases = []struct {
		name        string
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
		}, {
			putRaw: &store.Raw{
				Key: "foo1.meta",
			},
			getRaw: &store.Raw{
				Key: "foo1.meta",
			},
			data:        strings.NewReader("hello meta file"),
			getErrCheck: cdnerrors.IsNilError,
			expected:    "hello meta file",
		}, {
			putRaw: &store.Raw{
				Key:    "foo2.meta",
				Length: 6,
			},
			getRaw: &store.Raw{
				Key: "foo2.meta",
			},
			data:        strings.NewReader("hello meta file"),
			getErrCheck: cdnerrors.IsNilError,
			expected:    "hello ",
		}, {
			putRaw: &store.Raw{
				Key: "foo3.meta",
			},
			getRaw: &store.Raw{
				Key:    "foo3.meta",
				Offset: 2,
				Length: 5,
			},
			data:        strings.NewReader("hello meta file"),
			getErrCheck: cdnerrors.IsNilError,
			expected:    "llo m",
		}, {
			putRaw: &store.Raw{
				Key: "foo4.meta",
			},
			getRaw: &store.Raw{
				Key:    "foo4.meta",
				Offset: 2,
				Length: -1,
			},
			getErrCheck: cdnerrors.IsInvalidValue,
			data:        strings.NewReader("hello meta file"),
			expected:    "",
		}, {
			putRaw: &store.Raw{
				Key: "foo5.meta",
			},
			getRaw: &store.Raw{
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
			err := s.Put(context.Background(), v.putRaw, v.data)
			s.Nil(err)
			// get
			r, err := s.Get(context.Background(), v.getRaw)
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

func (s *StorageTestSuite) TestAppendBytes() {
	var cases = []struct {
		name        string
		putRaw      *store.Raw
		appendRaw   *store.Raw
		getRaw      *store.Raw
		data        io.Reader
		appData     io.Reader
		getErrCheck func(error) bool
		expected    string
	}{
		{
			putRaw: &store.Raw{
				Key:    "foo0.meta",
				Length: 13,
			},
			appendRaw: &store.Raw{
				Key:    "foo0.meta",
				Offset: 2,
				Length: 3,
				Append: true,
			},
			getRaw: &store.Raw{
				Key: "foo0.meta",
			},
			data:        strings.NewReader("hello meta fi"),
			appData:     strings.NewReader("append data"),
			getErrCheck: cdnerrors.IsNilError,
			expected:    "hello meta fiapp",
		}, {
			putRaw: &store.Raw{
				Key: "foo1.meta",
			},
			appendRaw: &store.Raw{
				Key:    "foo1.meta",
				Offset: 29,
				Length: 3,
				Append: true,
			},
			getRaw: &store.Raw{
				Key: "foo1.meta",
			},
			data:        strings.NewReader("hello meta file"),
			appData:     strings.NewReader("append data"),
			getErrCheck: cdnerrors.IsNilError,
			expected:    "hello meta fileapp",
		},
		//{
		//	putRaw: &store.Raw{
		//		Key:    "foo2.meta",
		//		Length: 6,
		//	},
		//	appendRaw: &store.Raw{
		//		Key:    "foo0.meta",
		//		Offset: 29,
		//		Length: 3,
		//		Append: true,
		//	},
		//	getRaw: &store.Raw{
		//		Key: "foo2.meta",
		//	},
		//	data:        strings.NewReader("hello meta file"),
		//	getErrCheck: cdnerrors.IsNilError,
		//	expected:    "hello ",
		//}, {
		//	putRaw: &store.Raw{
		//		Key: "foo3.meta",
		//	},
		//	getRaw: &store.Raw{
		//		Key:    "foo3.meta",
		//		Offset: 2,
		//		Length: 5,
		//	},
		//	data:        strings.NewReader("hello meta file"),
		//	getErrCheck: cdnerrors.IsNilError,
		//	expected:    "llo m",
		//}, {
		//	putRaw: &store.Raw{
		//		Key: "foo4.meta",
		//	},
		//	getRaw: &store.Raw{
		//		Key:    "foo4.meta",
		//		Offset: 2,
		//		Length: -1,
		//	},
		//	getErrCheck: cdnerrors.IsInvalidValue,
		//	data:        strings.NewReader("hello meta file"),
		//	expected:    "",
		//}, {
		//	putRaw: &store.Raw{
		//		Key: "foo5.meta",
		//	},
		//	getRaw: &store.Raw{
		//		Key:    "foo5.meta",
		//		Offset: 30,
		//		Length: 5,
		//	},
		//	getErrCheck: cdnerrors.IsInvalidValue,
		//	data:        strings.NewReader("hello meta file"),
		//	expected:    "",
		//},
	}
	for _, v := range cases {
		s.Run(v.name, func() {
			// put
			err := s.Put(context.Background(), v.putRaw, v.data)
			s.Nil(err)
			err = s.Put(context.Background(), v.appendRaw, v.appData)
			s.Nil(err)
			// get
			r, err := s.Get(context.Background(), v.getRaw)
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

func (s *StorageTestSuite) TestPutTrunc() {
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
		}, {
			truncRaw: &store.Raw{
				Key:    "fooTrunc.meta",
				Offset: 6,
				Trunc:  true,
			},
			data:         strings.NewReader("golang"),
			getErrCheck:  cdnerrors.IsNilError,
			expectedData: "\x00\x00\x00\x00\x00\x00golang",
		}, {
			truncRaw: &store.Raw{
				Key:    "fooTrunc.meta",
				Offset: 0,
				Trunc:  false,
			},
			data:         strings.NewReader("foo"),
			getErrCheck:  cdnerrors.IsNilError,
			expectedData: "foolo world",
		}, {
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
		err := s.Put(context.Background(), originRaw, strings.NewReader(originData))
		s.Nil(err)

		err = s.Put(context.Background(), v.truncRaw, v.data)
		s.Nil(err)

		r, err := s.Get(context.Background(), &store.Raw{
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

func (s *StorageTestSuite) TestPutParallel() {
	var key = "fooPutParallel"
	var routineCount = 4
	var testStr = "hello"
	var testStrLength = len(testStr)

	var wg sync.WaitGroup
	for k := 0; k < routineCount; k++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			s.Put(context.TODO(), &store.Raw{
				Key:    key,
				Offset: int64(i) * int64(testStrLength),
			}, strings.NewReader(testStr))
		}(k)
	}
	wg.Wait()

	info, err := s.Stat(context.TODO(), &store.Raw{Key: key})
	s.Nil(err)
	s.Equal(info.Size, int64(routineCount)*int64(testStrLength))
}

func (s *StorageTestSuite) TestRemove() {
	type fields struct {
		BaseDir string
	}
	type args struct {
		ctx context.Context
		raw *store.Raw
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{

		},
	}
	for _, tt := range tests {
		err := s.Remove(tt.args.ctx, tt.args.raw)
		s.Equal(err != nil, tt.wantErr)
	}
}

func (s *StorageTestSuite) TestStat() {
	type fields struct {
		BaseDir string
	}
	type args struct {
		ctx context.Context
		raw *store.Raw
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *store.StorageInfo
		wantErr bool
	}{
		{

		},
	}
	for _, tt := range tests {
		got, err := s.Stat(tt.args.ctx, tt.args.raw)
		s.Equal(err, tt.wantErr)
		s.EqualValues(got, tt.want)
	}
}

func Test_diskStorage_CreateBaseDir(t *testing.T) {
	type fields struct {
		BaseDir  string
		GcConfig *store.GcConfig
	}
	type args struct {
		ctx context.Context
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ds := &diskStorage{
				BaseDir:  tt.fields.BaseDir,
				GcConfig: tt.fields.GcConfig,
			}
			if err := ds.CreateBaseDir(tt.args.ctx); (err != nil) != tt.wantErr {
				t.Errorf("CreateBaseDir() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_diskStorage_Exits(t *testing.T) {
	type fields struct {
		BaseDir  string
		GcConfig *store.GcConfig
	}
	type args struct {
		ctx context.Context
		raw *store.Raw
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ds := &diskStorage{
				BaseDir:  tt.fields.BaseDir,
				GcConfig: tt.fields.GcConfig,
			}
			if got := ds.Exits(tt.args.ctx, tt.args.raw); got != tt.want {
				t.Errorf("Exits() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_diskStorage_GetGcConfig(t *testing.T) {
	type fields struct {
		BaseDir  string
		GcConfig *store.GcConfig
	}
	type args struct {
		ctx context.Context
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *store.GcConfig
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ds := &diskStorage{
				BaseDir:  tt.fields.BaseDir,
				GcConfig: tt.fields.GcConfig,
			}
			if got := ds.GetGcConfig(tt.args.ctx); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetGcConfig() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_diskStorage_GetHomePath(t *testing.T) {
	type fields struct {
		BaseDir  string
		GcConfig *store.GcConfig
	}
	type args struct {
		ctx context.Context
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   string
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ds := &diskStorage{
				BaseDir:  tt.fields.BaseDir,
				GcConfig: tt.fields.GcConfig,
			}
			if got := ds.GetHomePath(tt.args.ctx); got != tt.want {
				t.Errorf("GetHomePath() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_diskStorage_GetPath(t *testing.T) {
	type fields struct {
		BaseDir  string
		GcConfig *store.GcConfig
	}
	type args struct {
		raw *store.Raw
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   string
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ds := &diskStorage{
				BaseDir:  tt.fields.BaseDir,
				GcConfig: tt.fields.GcConfig,
			}
			if got := ds.GetPath(tt.args.raw); got != tt.want {
				t.Errorf("GetPath() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_diskStorage_GetTotalAndFreeSpace(t *testing.T) {
	type fields struct {
		BaseDir  string
		GcConfig *store.GcConfig
	}
	type args struct {
		ctx context.Context
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    fsize.Size
		want1   fsize.Size
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ds := &diskStorage{
				BaseDir:  tt.fields.BaseDir,
				GcConfig: tt.fields.GcConfig,
			}
			got, got1, err := ds.GetTotalAndFreeSpace(tt.args.ctx)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetTotalAndFreeSpace() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("GetTotalAndFreeSpace() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("GetTotalAndFreeSpace() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func Test_diskStorage_GetTotalSpace(t *testing.T) {
	type fields struct {
		BaseDir  string
		GcConfig *store.GcConfig
	}
	type args struct {
		ctx context.Context
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    fsize.Size
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ds := &diskStorage{
				BaseDir:  tt.fields.BaseDir,
				GcConfig: tt.fields.GcConfig,
			}
			got, err := ds.GetTotalSpace(tt.args.ctx)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetTotalSpace() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("GetTotalSpace() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func (s *StorageTestSuite) BenchmarkPutParallel() {
	var wg sync.WaitGroup
	for k := 0; k < 1000; k++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			s.Put(context.Background(), &store.Raw{
				Key:    "foo.bech",
				Offset: int64(i) * 5,
			}, strings.NewReader("hello"))
		}(k)
	}
	wg.Wait()
}

func (s *StorageTestSuite) BenchmarkPutSerial() {
	for k := 0; k < 1000; k++ {
		s.Put(context.Background(), &store.Raw{
			Key:    "foo1.bech",
			Offset: int64(k) * 5,
		}, strings.NewReader("hello"))
	}
}

// -----------------------------------------------------------------------------
// helper function

func (s *StorageTestSuite) checkStat(raw *store.Raw) {
	info, err := s.Stat(context.Background(), raw)
	s.Equal(cdnerrors.IsNilError(err), true)

	pathTemp := filepath.Join(s.workHome, raw.Bucket, raw.Key)
	f, _ := os.Stat(pathTemp)

	s.EqualValues(info, &store.StorageInfo{
		Path:       filepath.Join(raw.Bucket, raw.Key),
		Size:       f.Size(),
		ModTime:    f.ModTime(),
		CreateTime: statutils.Ctime(f),
	})
}

func (s *StorageTestSuite) checkRemove(raw *store.Raw) {
	err := s.Remove(context.Background(), raw)
	s.Equal(cdnerrors.IsNilError(err), true)

	_, err = s.Stat(context.Background(), raw)
	s.Equal(cdnerrors.IsFileNotExist(err), true)
}
