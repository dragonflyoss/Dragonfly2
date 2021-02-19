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
	"fmt"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/cdnerrors"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/store"
	"github.com/dragonflyoss/Dragonfly2/pkg/util/fileutils"
	"github.com/dragonflyoss/Dragonfly2/pkg/util/stat"
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

func Test(t *testing.T) {
	suite.Run(t, new(StorageSuite))
}

type StorageSuite struct {
	workHome string
	store    store.StorageDriver
	suite.Suite
}

func (s *StorageSuite) SetupSuite() {
	s.workHome, _ = ioutil.TempDir("/tmp", "cdn-storageDriver-StoreTestSuite-repo")
	store, err := NewStorage("baseDir: " + s.workHome)
	s.Nil(err)
	s.NotNil(store)
	s.store = store
}

func (s *StorageSuite) TearDownSuite() {
	if s.workHome != "" {
		if err := os.RemoveAll(s.workHome); err != nil {
			fmt.Printf("remove path:%s error", s.workHome)
		}
	}
}

func (s *StorageSuite) TestGetPutBytes() {
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
		err := s.store.PutBytes(context.Background(), v.putRaw, v.data)
		s.Nil(err)

		// get
		result, err := s.store.GetBytes(context.Background(), v.getRaw)
		s.Equal(v.getErrCheck(err), true)
		if err == nil {
			s.Equal(string(result), v.expected)
		}

		// stat
		s.checkStat(v.putRaw)

		// remove
		s.checkRemove(v.putRaw)
	}

}

func (s *StorageSuite) TestGetPut() {
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
		s.store.Put(context.Background(), v.putRaw, v.data)

		// get
		r, err := s.store.Get(context.Background(), v.getRaw)
		s.Equal(v.getErrCheck(err), true)
		if err == nil {
			result, err := ioutil.ReadAll(r)
			s.Nil(err)
			s.Equal(string(result[:]), v.expected)
		}

		// stat
		s.checkStat(v.putRaw)

		// remove
		s.checkRemove(v.putRaw)
	}

}

func (s *StorageSuite) TestPutTrunc() {
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
		err := s.store.Put(context.Background(), originRaw, strings.NewReader(originData))
		s.Nil(err)

		err = s.store.Put(context.Background(), v.truncRaw, v.data)
		s.Nil(err)

		r, err := s.store.Get(context.Background(), &store.Raw{
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

func (s *StorageSuite) TestPutParallel() {
	var key = "fooPutParallel"
	var routineCount = 4
	var testStr = "hello"
	var testStrLength = len(testStr)

	var wg sync.WaitGroup
	for k := 0; k < routineCount; k++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			s.store.Put(context.TODO(), &store.Raw{
				Key:    key,
				Offset: int64(i) * int64(testStrLength),
			}, strings.NewReader(testStr))
		}(k)
	}
	wg.Wait()

	info, err := s.store.Stat(context.TODO(), &store.Raw{Key: key})
	s.Nil(err)
	s.Equal(info.Size, int64(routineCount)*int64(testStrLength))
}

func (s *StorageSuite) BenchmarkPutParallel() {
	var wg sync.WaitGroup
	for k := 0; k < 1000; k++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			s.store.Put(context.Background(), &store.Raw{
				Key:    "foo.bech",
				Offset: int64(i) * 5,
			}, strings.NewReader("hello"))
		}(k)
	}
	wg.Wait()
}

func (s *StorageSuite) BenchmarkPutSerial() {
	for k := 0; k < 1000; k++ {
		s.store.Put(context.Background(), &store.Raw{
			Key:    "foo1.bech",
			Offset: int64(k) * 5,
		}, strings.NewReader("hello"))
	}
}

// -----------------------------------------------------------------------------
// helper function

func (s *StorageSuite) checkStat(raw *store.Raw) {
	info, err := s.store.Stat(context.Background(), raw)
	s.Equal(cdnerrors.IsNilError(err), true)

	pathTemp := filepath.Join(s.workHome, raw.Bucket, raw.Key)
	f, _ := os.Stat(pathTemp)
	sys, _ := fileutils.GetSys(f)

	s.EqualValues(info, &store.StorageInfo{
		Path:       filepath.Join(raw.Bucket, raw.Key),
		Size:       f.Size(),
		ModTime:    f.ModTime(),
		CreateTime: statutils.Ctime(sys),
	})
}

func (s *StorageSuite) checkRemove(raw *store.Raw) {
	err := s.store.Remove(context.Background(), raw)
	s.Equal(cdnerrors.IsNilError(err), true)

	_, err = s.store.Stat(context.Background(), raw)
	s.Equal(cdnerrors.IsKeyNotFound(err), true)
}

func TestNewStorage(t *testing.T) {
	type args struct {
		conf string
	}
	tests := []struct {
		name    string
		args    args
		want    store.StorageDriver
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewStorage(tt.args.conf)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewStorage() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewStorage() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_diskStorage_AppendBytes(t *testing.T) {
	type fields struct {
		BaseDir string
	}
	type args struct {
		ctx  context.Context
		raw  *store.Raw
		data []byte
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
			ls := &diskStorage{
				BaseDir: tt.fields.BaseDir,
			}
			if err := ls.AppendBytes(tt.args.ctx, tt.args.raw, tt.args.data); (err != nil) != tt.wantErr {
				t.Errorf("AppendBytes() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_diskStorage_Get(t *testing.T) {
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
		want    io.Reader
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ls := &diskStorage{
				BaseDir: tt.fields.BaseDir,
			}
			got, err := ls.Get(tt.args.ctx, tt.args.raw)
			if (err != nil) != tt.wantErr {
				t.Errorf("Get() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Get() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_diskStorage_GetAvailSpace(t *testing.T) {
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
		want    fileutils.Fsize
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ls := &diskStorage{
				BaseDir: tt.fields.BaseDir,
			}
			got, err := ls.GetAvailSpace(tt.args.ctx, tt.args.raw)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetAvailSpace() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("GetAvailSpace() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_diskStorage_GetBytes(t *testing.T) {
	type fields struct {
		BaseDir string
	}
	type args struct {
		ctx context.Context
		raw *store.Raw
	}
	tests := []struct {
		name     string
		fields   fields
		args     args
		wantData []byte
		wantErr  bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ls := &diskStorage{
				BaseDir: tt.fields.BaseDir,
			}
			gotData, err := ls.GetBytes(tt.args.ctx, tt.args.raw)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetBytes() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(gotData, tt.wantData) {
				t.Errorf("GetBytes() gotData = %v, want %v", gotData, tt.wantData)
			}
		})
	}
}

func Test_diskStorage_Put(t *testing.T) {
	type fields struct {
		BaseDir string
	}
	type args struct {
		ctx  context.Context
		raw  *store.Raw
		data io.Reader
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
			ls := &diskStorage{
				BaseDir: tt.fields.BaseDir,
			}
			if err := ls.Put(tt.args.ctx, tt.args.raw, tt.args.data); (err != nil) != tt.wantErr {
				t.Errorf("Put() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_diskStorage_PutBytes(t *testing.T) {
	type fields struct {
		BaseDir string
	}
	type args struct {
		ctx  context.Context
		raw  *store.Raw
		data []byte
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
			ls := &diskStorage{
				BaseDir: tt.fields.BaseDir,
			}
			if err := ls.PutBytes(tt.args.ctx, tt.args.raw, tt.args.data); (err != nil) != tt.wantErr {
				t.Errorf("PutBytes() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_diskStorage_Remove(t *testing.T) {
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
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ls := &diskStorage{
				BaseDir: tt.fields.BaseDir,
			}
			if err := ls.Remove(tt.args.ctx, tt.args.raw); (err != nil) != tt.wantErr {
				t.Errorf("Remove() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_diskStorage_Stat(t *testing.T) {
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
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ls := &diskStorage{
				BaseDir: tt.fields.BaseDir,
			}
			got, err := ls.Stat(tt.args.ctx, tt.args.raw)
			if (err != nil) != tt.wantErr {
				t.Errorf("Stat() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Stat() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_diskStorage_Walk(t *testing.T) {
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
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ls := &diskStorage{
				BaseDir: tt.fields.BaseDir,
			}
			if err := ls.Walk(tt.args.ctx, tt.args.raw); (err != nil) != tt.wantErr {
				t.Errorf("Walk() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_diskStorage_preparePath(t *testing.T) {
	type fields struct {
		BaseDir string
	}
	type args struct {
		bucket string
		key    string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    string
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ls := &diskStorage{
				BaseDir: tt.fields.BaseDir,
			}
			got, err := ls.preparePath(tt.args.bucket, tt.args.key)
			if (err != nil) != tt.wantErr {
				t.Errorf("preparePath() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("preparePath() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_diskStorage_statPath(t *testing.T) {
	type fields struct {
		BaseDir string
	}
	type args struct {
		bucket string
		key    string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    string
		want1   os.FileInfo
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ls := &diskStorage{
				BaseDir: tt.fields.BaseDir,
			}
			got, got1, err := ls.statPath(tt.args.bucket, tt.args.key)
			if (err != nil) != tt.wantErr {
				t.Errorf("statPath() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("statPath() got = %v, want %v", got, tt.want)
			}
			if !reflect.DeepEqual(got1, tt.want1) {
				t.Errorf("statPath() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}
