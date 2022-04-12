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

package storage

import (
	"io/fs"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"testing"
	"time"

	"github.com/gocarina/gocsv"
	"github.com/stretchr/testify/assert"
)

func TestStorage_New(t *testing.T) {
	tests := []struct {
		name    string
		baseDir string
		options []Option
		expect  func(t *testing.T, s Storage, err error)
	}{
		{
			name:    "new storage",
			baseDir: os.TempDir(),
			options: []Option{},
			expect: func(t *testing.T, s Storage, err error) {
				assert := assert.New(t)
				assert.Equal(reflect.TypeOf(s).Elem().Name(), "storage")
				assert.NoError(err)

				if err := s.Clear(); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name:    "new storage with maxSize",
			baseDir: os.TempDir(),
			options: []Option{WithMaxSize(100)},
			expect: func(t *testing.T, s Storage, err error) {
				assert := assert.New(t)
				assert.Equal(reflect.TypeOf(s).Elem().Name(), "storage")
				assert.NoError(err)

				if err := s.Clear(); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name:    "new storage with maxBackups",
			baseDir: os.TempDir(),
			options: []Option{WithMaxBackups(100)},
			expect: func(t *testing.T, s Storage, err error) {
				assert := assert.New(t)
				assert.Equal(reflect.TypeOf(s).Elem().Name(), "storage")
				assert.NoError(err)

				if err := s.Clear(); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name:    "new storage failed",
			baseDir: "/foo",
			options: []Option{WithMaxBackups(100)},
			expect: func(t *testing.T, s Storage, err error) {
				assert := assert.New(t)
				assert.Error(err)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s, err := New(tc.baseDir, tc.options...)
			tc.expect(t, s, err)
		})
	}
}

func TestStorage_Create(t *testing.T) {
	tests := []struct {
		name    string
		baseDir string
		options []Option
		mock    func(s Storage)
		expect  func(t *testing.T, s Storage, baseDir string)
	}{
		{
			name:    "create record",
			baseDir: os.TempDir(),
			options: []Option{},
			mock:    func(s Storage) {},
			expect: func(t *testing.T, s Storage, baseDir string) {
				assert := assert.New(t)
				err := s.Create(Record{})
				assert.NoError(err)
			},
		},
		{
			name:    "open file failed",
			baseDir: os.TempDir(),
			options: []Option{},
			mock: func(s Storage) {
				s.(*storage).baseDir = "foo"
			},
			expect: func(t *testing.T, s Storage, baseDir string) {
				assert := assert.New(t)
				err := s.Create(Record{})
				assert.Error(err)
				s.(*storage).baseDir = baseDir
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s, err := New(tc.baseDir, tc.options...)
			if err != nil {
				t.Fatal(err)
			}

			tc.mock(s)
			tc.expect(t, s, tc.baseDir)
			if err := s.Clear(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestStorage_List(t *testing.T) {
	tests := []struct {
		name    string
		baseDir string
		options []Option
		record  Record
		mock    func(t *testing.T, s Storage, baseDir string, record Record)
		expect  func(t *testing.T, s Storage, baseDir string, record Record)
	}{
		{
			name:    "empty csv file given",
			baseDir: os.TempDir(),
			options: []Option{},
			mock:    func(t *testing.T, s Storage, baseDir string, record Record) {},
			expect: func(t *testing.T, s Storage, baseDir string, record Record) {
				assert := assert.New(t)
				_, err := s.List()
				assert.Error(err)
			},
		},
		{
			name:    "get file infos failed",
			baseDir: os.TempDir(),
			options: []Option{},
			mock: func(t *testing.T, s Storage, baseDir string, record Record) {
				s.(*storage).baseDir = "foo"
			},
			expect: func(t *testing.T, s Storage, baseDir string, record Record) {
				assert := assert.New(t)
				_, err := s.List()
				assert.Error(err)
				s.(*storage).baseDir = baseDir
			},
		},
		{
			name:    "open file failed",
			baseDir: os.TempDir(),
			options: []Option{},
			mock: func(t *testing.T, s Storage, baseDir string, record Record) {
				file, err := os.OpenFile(filepath.Join(baseDir, "record-test.csv"), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0300)
				if err != nil {
					t.Fatal(err)
				}
				file.Close()
			},
			expect: func(t *testing.T, s Storage, baseDir string, record Record) {
				assert := assert.New(t)
				_, err := s.List()
				assert.Error(err)
			},
		},
		{
			name:    "list records of a file",
			baseDir: os.TempDir(),
			options: []Option{},
			record:  Record{},
			mock: func(t *testing.T, s Storage, baseDir string, record Record) {
				s.Create(Record{
					ID:                    "1",
					IP:                    "127.0.0.1",
					Hostname:              "hostname",
					BizTag:                "biz_tag",
					Cost:                  1,
					PieceCount:            1,
					TotalPieceCount:       1,
					ContentLength:         1,
					SecurityDomain:        "security_domain",
					IDC:                   "idc",
					NetTopology:           "net_topology",
					Location:              "location",
					FreeUploadLoad:        1,
					State:                 PeerStateSucceeded,
					CreateAt:              time.Now(),
					UpdateAt:              time.Now(),
					ParentID:              "2",
					ParentIP:              "127.0.0.1",
					ParentHostname:        "parent_hostname",
					ParentBizTag:          "parent_biz_tag",
					ParentCost:            1,
					ParentPieceCount:      1,
					ParentTotalPieceCount: 1,
					ParentContentLength:   1,
					ParentSecurityDomain:  "parent_security_domain",
					ParentIDC:             "parent_idc",
					ParentNetTopology:     "parent_net_topology",
					ParentLocation:        "parent_location",
					ParentFreeUploadLoad:  1,
					ParentIsCDN:           true,
					ParentCreateAt:        time.Now(),
					ParentUpdateAt:        time.Now(),
				})
			},
			expect: func(t *testing.T, s Storage, baseDir string, record Record) {
				assert := assert.New(t)
				records, err := s.List()
				assert.NoError(err)
				assert.Equal(len(records), 1)
				assert.EqualValues(records[0], record)
			},
		},
		{
			name:    "list records of multi files",
			baseDir: os.TempDir(),
			options: []Option{},
			record:  Record{},
			mock: func(t *testing.T, s Storage, baseDir string, record Record) {
				file, err := os.OpenFile(filepath.Join(baseDir, "record-test.csv"), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
				if err != nil {
					t.Fatal(err)
				}
				defer file.Close()

				if err := gocsv.MarshalWithoutHeaders([]Record{{ID: "2"}}, file); err != nil {
					t.Fatal(err)
				}

				s.Create(Record{ID: "1"})
			},
			expect: func(t *testing.T, s Storage, baseDir string, record Record) {
				assert := assert.New(t)
				records, err := s.List()
				assert.NoError(err)
				assert.Equal(len(records), 2)
				assert.EqualValues(records[0], record)
				assert.Equal(records[1].ID, "2")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s, err := New(tc.baseDir, tc.options...)
			if err != nil {
				t.Fatal(err)
			}

			tc.mock(t, s, tc.baseDir, tc.record)
			tc.expect(t, s, tc.baseDir, tc.record)
			if err := s.Clear(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestStorage_Clear(t *testing.T) {
	tests := []struct {
		name    string
		baseDir string
		options []Option
		mock    func(s Storage)
		expect  func(t *testing.T, s Storage, baseDir string)
	}{
		{
			name:    "clear file",
			baseDir: os.TempDir(),
			options: []Option{},
			mock:    func(s Storage) {},
			expect: func(t *testing.T, s Storage, baseDir string) {
				assert := assert.New(t)
				assert.NoError(s.Clear())
				fileInfos, err := ioutil.ReadDir(filepath.Join(baseDir))
				assert.NoError(err)

				var backups []fs.FileInfo
				regexp := regexp.MustCompile(RecordFilePrefix)
				for _, fileInfo := range fileInfos {
					if !fileInfo.IsDir() && regexp.MatchString(fileInfo.Name()) {
						backups = append(backups, fileInfo)
					}
				}
				assert.Equal(len(backups), 0)
			},
		},
		{
			name:    "open file failed",
			baseDir: os.TempDir(),
			options: []Option{},
			mock: func(s Storage) {
				s.(*storage).baseDir = "foo"
			},
			expect: func(t *testing.T, s Storage, baseDir string) {
				assert := assert.New(t)
				assert.Error(s.Clear())

				s.(*storage).baseDir = baseDir
				assert.NoError(s.Clear())
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s, err := New(tc.baseDir, tc.options...)
			if err != nil {
				t.Fatal(err)
			}

			tc.mock(s)
			tc.expect(t, s, tc.baseDir)
		})
	}
}
