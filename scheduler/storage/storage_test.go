/*
 *     Copyright 2022 The Dragonfly Authors
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
	"fmt"
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

	"d7y.io/dragonfly/v2/scheduler/config"
	"d7y.io/dragonfly/v2/scheduler/resource"
)

var (
	mockTask = Task{
		ID:                    "1",
		URL:                   "example.com",
		Type:                  "normal",
		ContentLength:         2048,
		TotalPieceCount:       1,
		BackToSourceLimit:     10,
		BackToSourcePeerCount: 2,
		State:                 "Succeeded",
		CreatedAt:             time.Now().UnixNano(),
		UpdatedAt:             time.Now().UnixNano(),
	}

	mockHost = Host{
		ID:                    "2",
		Type:                  "normal",
		Hostname:              "localhost",
		IP:                    "127.0.0.1",
		Port:                  8080,
		DownloadPort:          8081,
		OS:                    "linux",
		Platform:              "ubuntu",
		PlatformFamily:        "debian",
		PlatformVersion:       "1.0.0",
		KernelVersion:         "1.0.0",
		ConcurrentUploadLimit: 100,
		ConcurrentUploadCount: 40,
		UploadCount:           20,
		UploadFailedCount:     3,
		CPU: resource.CPU{
			LogicalCount:   24,
			PhysicalCount:  12,
			Percent:        0.8,
			ProcessPercent: 0.4,
			Times: resource.CPUTimes{
				User:      100,
				System:    101,
				Idle:      102,
				Nice:      103,
				Iowait:    104,
				Irq:       105,
				Softirq:   106,
				Steal:     107,
				Guest:     108,
				GuestNice: 109,
			},
		},
		Memory: resource.Memory{
			Total:              20,
			Available:          19,
			Used:               16,
			UsedPercent:        0.7,
			ProcessUsedPercent: 0.2,
			Free:               15,
		},
		Network: resource.Network{
			TCPConnectionCount:       400,
			UploadTCPConnectionCount: 200,
			SecurityDomain:           "product",
			Location:                 "china",
			IDC:                      "e1",
		},
		Disk: resource.Disk{
			Total:             100,
			Free:              88,
			Used:              56,
			UsedPercent:       0.9,
			InodesTotal:       200,
			InodesUsed:        180,
			InodesFree:        160,
			InodesUsedPercent: 0.6,
		},
		Build: resource.Build{
			GitVersion: "3.0.0",
			GitCommit:  "2bf4d5e",
			GoVersion:  "1.19",
			Platform:   "linux",
		},
		CreatedAt: time.Now().UnixNano(),
		UpdatedAt: time.Now().UnixNano(),
	}

	mockParent = Parent{
		ID:               "3",
		Tag:              "m",
		Application:      "db",
		State:            "Succeeded",
		Cost:             1000,
		UploadPieceCount: 10,
		Host:             mockHost,
		CreatedAt:        time.Now().UnixNano(),
		UpdatedAt:        time.Now().UnixNano(),
	}

	mockParents = append(make([]Parent, 19), mockParent)

	mockRecord = Record{
		ID:          "4",
		Tag:         "d",
		Application: "mq",
		State:       "Succeeded",
		Error: Error{
			Code:    "unknow",
			Message: "unknow",
		},
		Cost:      1000,
		Task:      mockTask,
		Host:      mockHost,
		Parents:   mockParents,
		CreatedAt: time.Now().UnixNano(),
		UpdatedAt: time.Now().UnixNano(),
	}
)

func TestStorage_New(t *testing.T) {
	tests := []struct {
		name    string
		baseDir string
		expect  func(t *testing.T, s Storage, err error)
	}{
		{
			name:    "new storage",
			baseDir: os.TempDir(),
			expect: func(t *testing.T, s Storage, err error) {
				assert := assert.New(t)
				assert.NoError(err)
				assert.Equal(reflect.TypeOf(s).Elem().Name(), "storage")
				assert.Equal(s.(*storage).maxSize, int64(config.DefaultStorageMaxSize*megabyte))
				assert.Equal(s.(*storage).maxBackups, config.DefaultStorageMaxBackups)
				assert.Equal(s.(*storage).bufferSize, config.DefaultStorageBufferSize)
				assert.Equal(cap(s.(*storage).buffer), config.DefaultStorageBufferSize)
				assert.Equal(len(s.(*storage).buffer), 0)
				assert.Equal(s.(*storage).count, int64(0))

				if err := s.Clear(); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name:    "new storage failed",
			baseDir: "/foo",
			expect: func(t *testing.T, s Storage, err error) {
				assert := assert.New(t)
				assert.Error(err)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s, err := New(tc.baseDir, config.DefaultStorageMaxSize, config.DefaultStorageMaxBackups, config.DefaultStorageBufferSize)
			tc.expect(t, s, err)
		})
	}
}

func TestStorage_Create(t *testing.T) {
	tests := []struct {
		name       string
		baseDir    string
		bufferSize int
		mock       func(s Storage)
		expect     func(t *testing.T, s Storage, baseDir string)
	}{
		{
			name:       "create record",
			baseDir:    os.TempDir(),
			bufferSize: 1,
			mock:       func(s Storage) {},
			expect: func(t *testing.T, s Storage, baseDir string) {
				assert := assert.New(t)
				err := s.Create(Record{})
				assert.NoError(err)
				assert.Equal(s.(*storage).count, int64(0))
			},
		},
		{
			name:       "create record without buffer",
			baseDir:    os.TempDir(),
			bufferSize: 0,
			mock: func(s Storage) {
			},
			expect: func(t *testing.T, s Storage, baseDir string) {
				assert := assert.New(t)
				err := s.Create(Record{})
				assert.NoError(err)
				assert.Equal(s.(*storage).count, int64(1))
			},
		},
		{
			name:       "write record to file",
			baseDir:    os.TempDir(),
			bufferSize: 1,
			mock: func(s Storage) {
			},
			expect: func(t *testing.T, s Storage, baseDir string) {
				assert := assert.New(t)
				err := s.Create(Record{})
				assert.NoError(err)
				err = s.Create(Record{})
				assert.NoError(err)
				assert.Equal(s.(*storage).count, int64(1))
			},
		},
		{
			name:       "open file failed",
			baseDir:    os.TempDir(),
			bufferSize: 0,
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
			s, err := New(tc.baseDir, config.DefaultStorageMaxSize, config.DefaultStorageMaxBackups, tc.bufferSize)
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
		name       string
		baseDir    string
		bufferSize int
		record     Record
		mock       func(t *testing.T, s Storage, baseDir string, record Record)
		expect     func(t *testing.T, s Storage, baseDir string, record Record)
	}{
		{
			name:       "empty csv file given",
			baseDir:    os.TempDir(),
			bufferSize: config.DefaultStorageBufferSize,
			mock:       func(t *testing.T, s Storage, baseDir string, record Record) {},
			expect: func(t *testing.T, s Storage, baseDir string, record Record) {
				assert := assert.New(t)
				_, err := s.List()
				assert.Error(err)
			},
		},
		{
			name:       "get file infos failed",
			baseDir:    os.TempDir(),
			bufferSize: config.DefaultStorageBufferSize,
			mock: func(t *testing.T, s Storage, baseDir string, record Record) {
				s.(*storage).baseDir = "bae"
			},
			expect: func(t *testing.T, s Storage, baseDir string, record Record) {
				assert := assert.New(t)
				_, err := s.List()
				assert.Error(err)
				s.(*storage).baseDir = baseDir
			},
		},
		{
			name:       "open file failed",
			baseDir:    os.TempDir(),
			bufferSize: config.DefaultStorageBufferSize,
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
			name:       "list records of a file",
			baseDir:    os.TempDir(),
			bufferSize: 1,
			record:     mockRecord,
			mock: func(t *testing.T, s Storage, baseDir string, record Record) {
				if err := s.Create(record); err != nil {
					t.Fatal(err)
				}
			},
			expect: func(t *testing.T, s Storage, baseDir string, record Record) {
				assert := assert.New(t)
				_, err := s.List()
				assert.Error(err)

				if err := s.Create(record); err != nil {
					t.Fatal(err)
				}
				records, err := s.List()
				assert.NoError(err)
				assert.Equal(len(records), 1)
				assert.EqualValues(records[0], record)
			},
		},
		{
			name:       "list records of multi files",
			baseDir:    os.TempDir(),
			bufferSize: 1,
			record:     Record{},
			mock: func(t *testing.T, s Storage, baseDir string, record Record) {
				file, err := os.OpenFile(filepath.Join(baseDir, "record-test.csv"), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
				if err != nil {
					t.Fatal(err)
				}
				defer file.Close()

				if err := gocsv.MarshalWithoutHeaders([]Record{{ID: "2"}}, file); err != nil {
					t.Fatal(err)
				}

				if err := s.Create(Record{ID: "1"}); err != nil {
					t.Fatal(err)
				}

				if err := s.Create(Record{ID: "3"}); err != nil {
					t.Fatal(err)
				}
			},
			expect: func(t *testing.T, s Storage, baseDir string, record Record) {
				assert := assert.New(t)
				records, err := s.List()
				assert.NoError(err)
				assert.Equal(len(records), 2)
				assert.Equal(records[0].ID, "2")
				assert.Equal(records[1].ID, "1")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s, err := New(tc.baseDir, config.DefaultStorageMaxSize, config.DefaultStorageMaxBackups, tc.bufferSize)
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

func TestStorage_Open(t *testing.T) {
	tests := []struct {
		name       string
		baseDir    string
		bufferSize int
		record     Record
		mock       func(t *testing.T, s Storage, baseDir string, record Record)
		expect     func(t *testing.T, s Storage, baseDir string, record Record)
	}{
		{
			name:       "open storage withempty csv file given",
			baseDir:    os.TempDir(),
			bufferSize: config.DefaultStorageBufferSize,
			mock:       func(t *testing.T, s Storage, baseDir string, record Record) {},
			expect: func(t *testing.T, s Storage, baseDir string, record Record) {
				assert := assert.New(t)
				_, err := s.Open()
				assert.NoError(err)
			},
		},
		{
			name:       "open file infos failed",
			baseDir:    os.TempDir(),
			bufferSize: config.DefaultStorageBufferSize,
			mock: func(t *testing.T, s Storage, baseDir string, record Record) {
				s.(*storage).baseDir = "bas"
			},
			expect: func(t *testing.T, s Storage, baseDir string, record Record) {
				assert := assert.New(t)
				_, err := s.Open()
				assert.Error(err)
				s.(*storage).baseDir = baseDir
			},
		},
		{
			name:       "open storage with records of a file",
			baseDir:    os.TempDir(),
			bufferSize: 1,
			record:     mockRecord,
			mock: func(t *testing.T, s Storage, baseDir string, record Record) {
				if err := s.Create(record); err != nil {
					t.Fatal(err)
				}
			},
			expect: func(t *testing.T, s Storage, baseDir string, record Record) {
				assert := assert.New(t)
				_, err := s.Open()
				assert.NoError(err)

				if err := s.Create(record); err != nil {
					t.Fatal(err)
				}

				readCloser, err := s.Open()
				assert.NoError(err)

				var records []Record
				err = gocsv.UnmarshalWithoutHeaders(readCloser, &records)
				assert.NoError(err)
				assert.Equal(len(records), 1)
				assert.EqualValues(records[0], record)
			},
		},
		{
			name:       "open storage with records of multi files",
			baseDir:    os.TempDir(),
			bufferSize: 1,
			record:     Record{},
			mock: func(t *testing.T, s Storage, baseDir string, record Record) {
				file, err := os.OpenFile(filepath.Join(baseDir, "record-test.csv"), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
				if err != nil {
					t.Fatal(err)
				}
				defer file.Close()

				if err := gocsv.MarshalWithoutHeaders([]Record{{ID: "2"}}, file); err != nil {
					t.Fatal(err)
				}

				if err := s.Create(Record{ID: "1"}); err != nil {
					t.Fatal(err)
				}

				if err := s.Create(Record{ID: "3"}); err != nil {
					t.Fatal(err)
				}
			},
			expect: func(t *testing.T, s Storage, baseDir string, record Record) {
				assert := assert.New(t)
				readCloser, err := s.Open()
				assert.NoError(err)

				var records []Record
				err = gocsv.UnmarshalWithoutHeaders(readCloser, &records)
				assert.NoError(err)
				assert.Equal(len(records), 2)
				assert.Equal(records[0].ID, "2")
				assert.Equal(records[1].ID, "1")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s, err := New(tc.baseDir, config.DefaultStorageMaxSize, config.DefaultStorageMaxBackups, tc.bufferSize)
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
		mock    func(s Storage)
		expect  func(t *testing.T, s Storage, baseDir string)
	}{
		{
			name:    "clear file",
			baseDir: os.TempDir(),
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
			mock: func(s Storage) {
				s.(*storage).baseDir = "baz"
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
			s, err := New(tc.baseDir, config.DefaultStorageMaxSize, config.DefaultStorageMaxBackups, config.DefaultStorageBufferSize)
			if err != nil {
				t.Fatal(err)
			}

			tc.mock(s)
			tc.expect(t, s, tc.baseDir)
		})
	}
}

func TestStorage_create(t *testing.T) {
	tests := []struct {
		name    string
		baseDir string
		mock    func(s Storage)
		expect  func(t *testing.T, s Storage, baseDir string)
	}{
		{
			name:    "create record",
			baseDir: os.TempDir(),
			mock:    func(s Storage) {},
			expect: func(t *testing.T, s Storage, baseDir string) {
				assert := assert.New(t)
				err := s.(*storage).create(Record{})
				assert.NoError(err)
			},
		},
		{
			name:    "open file failed",
			baseDir: os.TempDir(),
			mock: func(s Storage) {
				s.(*storage).baseDir = "foo"
			},
			expect: func(t *testing.T, s Storage, baseDir string) {
				assert := assert.New(t)
				err := s.(*storage).create(Record{})
				assert.Error(err)
				s.(*storage).baseDir = baseDir
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s, err := New(tc.baseDir, config.DefaultStorageMaxSize, config.DefaultStorageMaxBackups, config.DefaultStorageBufferSize)
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

func TestStorage_openFile(t *testing.T) {
	tests := []struct {
		name       string
		baseDir    string
		maxSize    int
		maxBackups int

		bufferSize int
		mock       func(t *testing.T, s Storage)
		expect     func(t *testing.T, s Storage, baseDir string)
	}{
		{
			name:       "open file failed",
			baseDir:    os.TempDir(),
			maxSize:    config.DefaultStorageMaxSize,
			maxBackups: config.DefaultStorageMaxBackups,
			bufferSize: config.DefaultStorageBufferSize,
			mock: func(t *testing.T, s Storage) {
				s.(*storage).baseDir = "bat"
			},
			expect: func(t *testing.T, s Storage, baseDir string) {
				assert := assert.New(t)
				_, err := s.(*storage).openFile()
				assert.Error(err)
				s.(*storage).baseDir = baseDir
			},
		},
		{
			name:       "open new record file",
			baseDir:    os.TempDir(),
			maxSize:    0,
			maxBackups: config.DefaultStorageMaxBackups,
			bufferSize: 1,
			mock: func(t *testing.T, s Storage) {
				if err := s.Create(Record{ID: "1"}); err != nil {
					t.Fatal(err)
				}

				if err := s.Create(Record{ID: "2"}); err != nil {
					t.Fatal(err)
				}
			},
			expect: func(t *testing.T, s Storage, baseDir string) {
				assert := assert.New(t)
				file, err := s.(*storage).openFile()
				assert.NoError(err)
				assert.Equal(file.Name(), filepath.Join(baseDir, fmt.Sprintf("%s.%s", RecordFilePrefix, RecordFileExt)))
				file.Close()
			},
		},
		{
			name:       "remove record file",
			baseDir:    os.TempDir(),
			maxSize:    0,
			maxBackups: 1,
			bufferSize: 1,
			mock: func(t *testing.T, s Storage) {
				if err := s.Create(Record{ID: "1"}); err != nil {
					t.Fatal(err)
				}
			},
			expect: func(t *testing.T, s Storage, baseDir string) {
				assert := assert.New(t)
				file, err := s.(*storage).openFile()
				assert.NoError(err)
				assert.Equal(file.Name(), filepath.Join(baseDir, fmt.Sprintf("%s.%s", RecordFilePrefix, RecordFileExt)))
				file.Close()
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s, err := New(tc.baseDir, tc.maxSize, tc.maxBackups, tc.bufferSize)
			if err != nil {
				t.Fatal(err)
			}

			tc.mock(t, s)
			tc.expect(t, s, tc.baseDir)
			if err := s.Clear(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestStorage_backupFilename(t *testing.T) {
	baseDir := os.TempDir()
	s, err := New(baseDir, config.DefaultStorageMaxSize, config.DefaultStorageMaxBackups, config.DefaultStorageBufferSize)
	if err != nil {
		t.Fatal(err)
	}

	filename := s.(*storage).backupFilename()
	regexp := regexp.MustCompile(fmt.Sprintf("%s-.*.%s$", RecordFilePrefix, RecordFileExt))
	assert := assert.New(t)
	assert.True(regexp.MatchString(filename))

	if err := s.Clear(); err != nil {
		t.Fatal(err)
	}
}

func TestStorage_backups(t *testing.T) {
	tests := []struct {
		name    string
		baseDir string
		mock    func(t *testing.T, s Storage)
		expect  func(t *testing.T, s Storage, baseDir string)
	}{
		{
			name:    "open file failed",
			baseDir: os.TempDir(),
			mock: func(t *testing.T, s Storage) {
				s.(*storage).baseDir = "bar"
			},
			expect: func(t *testing.T, s Storage, baseDir string) {
				assert := assert.New(t)
				_, err := s.(*storage).backups()
				assert.Error(err)
				s.(*storage).baseDir = baseDir
				if err := s.Clear(); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name:    "not found record file",
			baseDir: os.TempDir(),
			mock:    func(t *testing.T, s Storage) {},
			expect: func(t *testing.T, s Storage, baseDir string) {
				assert := assert.New(t)
				if err := s.Clear(); err != nil {
					t.Fatal(err)
				}

				_, err := s.(*storage).backups()
				assert.Error(err)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s, err := New(tc.baseDir, config.DefaultStorageMaxSize, config.DefaultStorageMaxBackups, config.DefaultStorageBufferSize)
			if err != nil {
				t.Fatal(err)
			}

			tc.mock(t, s)
			tc.expect(t, s, tc.baseDir)
		})
	}
}
