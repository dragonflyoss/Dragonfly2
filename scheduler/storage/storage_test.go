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
		CPU:                   mockCPU,
		Memory:                mockMemory,
		Network:               mockNetwork,
		Disk:                  mockDisk,
		Build:                 mockBulid,
		CreatedAt:             time.Now().UnixNano(),
		UpdatedAt:             time.Now().UnixNano(),
	}

	mockSeedHost = Host{
		ID:                    "3",
		Type:                  "super seed",
		Hostname:              "localhost_seed",
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
		CPU:                   mockCPU,
		Memory:                mockMemory,
		Network:               mockNetwork,
		Disk:                  mockDisk,
		Build:                 mockBulid,
		CreatedAt:             time.Now().UnixNano(),
		UpdatedAt:             time.Now().UnixNano(),
	}

	mockCPU = resource.CPU{
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
	}

	mockMemory = resource.Memory{
		Total:              20,
		Available:          19,
		Used:               16,
		UsedPercent:        0.7,
		ProcessUsedPercent: 0.2,
		Free:               15,
	}

	mockNetwork = resource.Network{
		TCPConnectionCount:       400,
		UploadTCPConnectionCount: 200,
		SecurityDomain:           "product",
		Location:                 "china",
		IDC:                      "e1",
	}

	mockDisk = resource.Disk{
		Total:             100,
		Free:              88,
		Used:              56,
		UsedPercent:       0.9,
		InodesTotal:       200,
		InodesUsed:        180,
		InodesFree:        160,
		InodesUsedPercent: 0.6,
	}

	mockBulid = resource.Build{
		GitVersion: "3.0.0",
		GitCommit:  "2bf4d5e",
		GoVersion:  "1.19",
		Platform:   "linux",
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

	mockProbe = Probe{
		Host:      mockHost,
		RTT:       30 * time.Millisecond.Nanoseconds(),
		CreatedAt: time.Now().UnixNano(),
	}

	mockProbes = Probes{
		Host:       mockSeedHost,
		AverageRTT: time.Duration(0).Nanoseconds(),
		CreatedAt:  time.Now().UnixNano(),
		UpdatedAt:  mockProbe.CreatedAt,
		Items:      []Probe{},
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
				assert.Equal(cap(s.(*storage).recordBuffer), config.DefaultStorageBufferSize)
				assert.Equal(cap(s.(*storage).probesBuffer), config.DefaultStorageBufferSize)
				assert.Equal(len(s.(*storage).recordBuffer), 0)
				assert.Equal(len(s.(*storage).probesBuffer), 0)
				assert.Equal(s.(*storage).recordCount, int64(0))
				assert.Equal(s.(*storage).probesCount, int64(0))
				if err := s.ClearRecord(); err != nil {
					t.Fatal(err)
				}
				if err := s.ClearProbes(); err != nil {
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
				assert.Equal(s.(*storage).recordCount, int64(0))
			},
		},
		{
			name:       "create probes",
			baseDir:    os.TempDir(),
			bufferSize: 1,
			mock:       func(s Storage) {},
			expect: func(t *testing.T, s Storage, baseDir string) {
				assert := assert.New(t)
				err := s.Create(Probes{})
				assert.NoError(err)
				assert.Equal(s.(*storage).probesCount, int64(0))
			},
		},
		{
			name:       "create record and probes",
			baseDir:    os.TempDir(),
			bufferSize: 1,
			mock:       func(s Storage) {},
			expect: func(t *testing.T, s Storage, baseDir string) {
				assert := assert.New(t)
				err := s.Create(Record{})
				assert.NoError(err)
				err = s.Create(Probes{})
				assert.NoError(err)
				assert.Equal(s.(*storage).recordCount, int64(0))
				assert.Equal(s.(*storage).probesCount, int64(0))
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
				assert.Equal(s.(*storage).recordCount, int64(1))
			},
		},
		{
			name:       "create probes without buffer",
			baseDir:    os.TempDir(),
			bufferSize: 0,
			mock: func(s Storage) {
			},
			expect: func(t *testing.T, s Storage, baseDir string) {
				assert := assert.New(t)
				err := s.Create(Probes{})
				assert.NoError(err)
				assert.Equal(s.(*storage).probesCount, int64(1))
			},
		},
		{
			name:       "create record and probes without buffer",
			baseDir:    os.TempDir(),
			bufferSize: 0,
			mock: func(s Storage) {
			},
			expect: func(t *testing.T, s Storage, baseDir string) {
				assert := assert.New(t)
				err := s.Create(Record{})
				assert.NoError(err)
				err = s.Create(Probes{})
				assert.NoError(err)
				assert.Equal(s.(*storage).recordCount, int64(1))
				assert.Equal(s.(*storage).probesCount, int64(1))
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
				assert.Equal(s.(*storage).recordCount, int64(1))
			},
		},
		{
			name:       "write probes to file",
			baseDir:    os.TempDir(),
			bufferSize: 1,
			mock: func(s Storage) {
			},
			expect: func(t *testing.T, s Storage, baseDir string) {
				assert := assert.New(t)
				err := s.Create(Probes{})
				assert.NoError(err)
				err = s.Create(Probes{})
				assert.NoError(err)
				assert.Equal(s.(*storage).probesCount, int64(1))
			},
		},
		{
			name:       "write record and probes to file",
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
				err = s.Create(Probes{})
				assert.NoError(err)
				err = s.Create(Probes{})
				assert.NoError(err)
				assert.Equal(s.(*storage).recordCount, int64(1))
				assert.Equal(s.(*storage).probesCount, int64(1))
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
			if err := s.ClearRecord(); err != nil {
				t.Fatal(err)
			}
			if err := s.ClearProbes(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestStorage_ListRecord(t *testing.T) {
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
				_, err := s.ListRecord()
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
				_, err := s.ListRecord()
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
				_, err := s.ListRecord()
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
				_, err := s.ListRecord()
				assert.Error(err)

				if err := s.Create(record); err != nil {
					t.Fatal(err)
				}
				records, err := s.ListRecord()
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
				records, err := s.ListRecord()
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
			if err := s.ClearRecord(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestStorage_ListProbes(t *testing.T) {
	tests := []struct {
		name       string
		baseDir    string
		bufferSize int
		probes     Probes
		mock       func(t *testing.T, s Storage, baseDir string, probes Probes)
		expect     func(t *testing.T, s Storage, baseDir string, probes Probes)
	}{
		{
			name:       "empty csv file given",
			baseDir:    os.TempDir(),
			bufferSize: config.DefaultStorageBufferSize,
			mock:       func(t *testing.T, s Storage, baseDir string, probes Probes) {},
			expect: func(t *testing.T, s Storage, baseDir string, probes Probes) {
				assert := assert.New(t)
				_, err := s.ListProbes()
				assert.Error(err)
			},
		},
		{
			name:       "get file infos failed",
			baseDir:    os.TempDir(),
			bufferSize: config.DefaultStorageBufferSize,
			mock: func(t *testing.T, s Storage, baseDir string, probes Probes) {
				s.(*storage).baseDir = "bae"
			},
			expect: func(t *testing.T, s Storage, baseDir string, probes Probes) {
				assert := assert.New(t)
				_, err := s.ListProbes()
				assert.Error(err)
				s.(*storage).baseDir = baseDir
			},
		},
		{
			name:       "open file failed",
			baseDir:    os.TempDir(),
			bufferSize: config.DefaultStorageBufferSize,
			mock: func(t *testing.T, s Storage, baseDir string, probes Probes) {
				file, err := os.OpenFile(filepath.Join(baseDir, "probes-test.csv"), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0300)
				if err != nil {
					t.Fatal(err)
				}
				file.Close()
			},
			expect: func(t *testing.T, s Storage, baseDir string, probes Probes) {
				assert := assert.New(t)
				_, err := s.ListProbes()
				assert.Error(err)
			},
		},
		{
			name:       "list probes of a file",
			baseDir:    os.TempDir(),
			bufferSize: 1,
			probes:     mockProbes,
			mock: func(t *testing.T, s Storage, baseDir string, probes Probes) {
				if err := s.Create(probes); err != nil {
					t.Fatal(err)
				}
			},
			expect: func(t *testing.T, s Storage, baseDir string, probes Probes) {
				assert := assert.New(t)
				_, err := s.ListProbes()
				assert.Error(err)

				if err := s.Create(probes); err != nil {
					t.Fatal(err)
				}
				records, err := s.ListProbes()
				assert.NoError(err)
				assert.Equal(len(records), 1)
				assert.EqualValues(records[0], probes)
			},
		},
		{
			name:       "list probes of multi files",
			baseDir:    os.TempDir(),
			bufferSize: 1,
			probes:     Probes{},
			mock: func(t *testing.T, s Storage, baseDir string, probes Probes) {
				file, err := os.OpenFile(filepath.Join(baseDir, "probes-test.csv"), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
				if err != nil {
					t.Fatal(err)
				}
				defer file.Close()

				if err := gocsv.MarshalWithoutHeaders([]Probes{{AverageRTT: 2}}, file); err != nil {
					t.Fatal(err)
				}

				if err := s.Create(Probes{AverageRTT: 1}); err != nil {
					t.Fatal(err)
				}

				if err := s.Create(Probes{AverageRTT: 3}); err != nil {
					t.Fatal(err)
				}

			},
			expect: func(t *testing.T, s Storage, baseDir string, probes Probes) {
				assert := assert.New(t)
				multiProbes, err := s.ListProbes()
				assert.NoError(err)
				assert.Equal(len(multiProbes), 2)
				assert.Equal(multiProbes[0].AverageRTT, int64(2))
				assert.Equal(multiProbes[1].AverageRTT, int64(1))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s, err := New(tc.baseDir, config.DefaultStorageMaxSize, config.DefaultStorageMaxBackups, tc.bufferSize)
			if err != nil {
				t.Fatal(err)
			}

			tc.mock(t, s, tc.baseDir, tc.probes)
			tc.expect(t, s, tc.baseDir, tc.probes)
			if err := s.ClearRecord(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestStorage_OpenRecord(t *testing.T) {
	tests := []struct {
		name       string
		baseDir    string
		bufferSize int
		record     Record
		mock       func(t *testing.T, s Storage, baseDir string, record Record)
		expect     func(t *testing.T, s Storage, baseDir string, record Record)
	}{
		{
			name:       "open record storage with empty csv file given",
			baseDir:    os.TempDir(),
			bufferSize: config.DefaultStorageBufferSize,
			mock:       func(t *testing.T, s Storage, baseDir string, record Record) {},
			expect: func(t *testing.T, s Storage, baseDir string, record Record) {
				assert := assert.New(t)
				_, err := s.OpenRecord()
				assert.NoError(err)
			},
		},
		{
			name:       "open record file infos failed",
			baseDir:    os.TempDir(),
			bufferSize: config.DefaultStorageBufferSize,
			mock: func(t *testing.T, s Storage, baseDir string, record Record) {
				s.(*storage).baseDir = "bas"
			},
			expect: func(t *testing.T, s Storage, baseDir string, record Record) {
				assert := assert.New(t)
				_, err := s.OpenRecord()
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
				_, err := s.OpenRecord()
				assert.NoError(err)

				if err := s.Create(record); err != nil {
					t.Fatal(err)
				}

				readCloser, err := s.OpenRecord()
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
				readCloser, err := s.OpenRecord()
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
			if err := s.ClearRecord(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestStorage_OpenProbes(t *testing.T) {
	tests := []struct {
		name       string
		baseDir    string
		bufferSize int
		probes     Probes
		mock       func(t *testing.T, s Storage, baseDir string, probes Probes)
		expect     func(t *testing.T, s Storage, baseDir string, probes Probes)
	}{
		{
			name:       "open probes storage with empty csv file given",
			baseDir:    os.TempDir(),
			bufferSize: config.DefaultStorageBufferSize,
			mock:       func(t *testing.T, s Storage, baseDir string, probes Probes) {},
			expect: func(t *testing.T, s Storage, baseDir string, probes Probes) {
				assert := assert.New(t)
				_, err := s.OpenProbes()
				assert.NoError(err)
			},
		},
		{
			name:       "open probes file infos failed",
			baseDir:    os.TempDir(),
			bufferSize: config.DefaultStorageBufferSize,
			mock: func(t *testing.T, s Storage, baseDir string, probes Probes) {
				s.(*storage).baseDir = "bas"
			},
			expect: func(t *testing.T, s Storage, baseDir string, probes Probes) {
				assert := assert.New(t)
				_, err := s.OpenProbes()
				assert.Error(err)
				s.(*storage).baseDir = baseDir
			},
		},
		{
			name:       "open storage with probes of a file",
			baseDir:    os.TempDir(),
			bufferSize: 1,
			probes:     mockProbes,
			mock: func(t *testing.T, s Storage, baseDir string, probes Probes) {
				if err := s.Create(probes); err != nil {
					t.Fatal(err)
				}
			},
			expect: func(t *testing.T, s Storage, baseDir string, probes Probes) {
				assert := assert.New(t)
				_, err := s.OpenProbes()
				assert.NoError(err)

				if err := s.Create(probes); err != nil {
					t.Fatal(err)
				}

				readCloser, err := s.OpenProbes()
				assert.NoError(err)

				var multiProbes []Probes
				err = gocsv.UnmarshalWithoutHeaders(readCloser, &multiProbes)
				assert.NoError(err)
				assert.Equal(len(multiProbes), 1)
				assert.EqualValues(multiProbes[0], probes)
			},
		},
		{
			name:       "open storage with probes of multi files",
			baseDir:    os.TempDir(),
			bufferSize: 1,
			probes:     Probes{},
			mock: func(t *testing.T, s Storage, baseDir string, probes Probes) {
				file, err := os.OpenFile(filepath.Join(baseDir, "probes-test.csv"), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
				if err != nil {
					t.Fatal(err)
				}
				defer file.Close()

				if err := gocsv.MarshalWithoutHeaders([]Probes{{AverageRTT: 2}}, file); err != nil {
					t.Fatal(err)
				}

				if err := s.Create(Probes{AverageRTT: 1}); err != nil {
					t.Fatal(err)
				}

				if err := s.Create(Probes{AverageRTT: 3}); err != nil {
					t.Fatal(err)
				}
			},
			expect: func(t *testing.T, s Storage, baseDir string, probes Probes) {
				assert := assert.New(t)
				readCloser, err := s.OpenProbes()
				assert.NoError(err)

				var multiProbes []Probes
				err = gocsv.UnmarshalWithoutHeaders(readCloser, &multiProbes)
				assert.NoError(err)
				assert.Equal(len(multiProbes), 2)
				assert.Equal(multiProbes[0].AverageRTT, int64(2))
				assert.Equal(multiProbes[1].AverageRTT, int64(1))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s, err := New(tc.baseDir, config.DefaultStorageMaxSize, config.DefaultStorageMaxBackups, tc.bufferSize)
			if err != nil {
				t.Fatal(err)
			}

			tc.mock(t, s, tc.baseDir, tc.probes)
			tc.expect(t, s, tc.baseDir, tc.probes)
			if err := s.ClearProbes(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestStorage_ClearRecord(t *testing.T) {
	tests := []struct {
		name    string
		baseDir string
		mock    func(s Storage)
		expect  func(t *testing.T, s Storage, baseDir string)
	}{
		{
			name:    "clear record file",
			baseDir: os.TempDir(),
			mock:    func(s Storage) {},
			expect: func(t *testing.T, s Storage, baseDir string) {
				assert := assert.New(t)
				assert.NoError(s.ClearRecord())
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
			name:    "open record file failed",
			baseDir: os.TempDir(),
			mock: func(s Storage) {
				s.(*storage).baseDir = "baz"
			},
			expect: func(t *testing.T, s Storage, baseDir string) {
				assert := assert.New(t)
				assert.Error(s.ClearRecord())

				s.(*storage).baseDir = baseDir
				assert.NoError(s.ClearRecord())
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

func TestStorage_ClearProbes(t *testing.T) {
	tests := []struct {
		name    string
		baseDir string
		mock    func(s Storage)
		expect  func(t *testing.T, s Storage, baseDir string)
	}{
		{
			name:    "clear probes file",
			baseDir: os.TempDir(),
			mock:    func(s Storage) {},
			expect: func(t *testing.T, s Storage, baseDir string) {
				assert := assert.New(t)
				assert.NoError(s.ClearProbes())
				fileInfos, err := ioutil.ReadDir(filepath.Join(baseDir))
				assert.NoError(err)

				var backups []fs.FileInfo
				regexp := regexp.MustCompile(ProbesFilePrefix)
				for _, fileInfo := range fileInfos {
					if !fileInfo.IsDir() && regexp.MatchString(fileInfo.Name()) {
						backups = append(backups, fileInfo)
					}
				}
				assert.Equal(len(backups), 0)
			},
		},
		{
			name:    "open probes file failed",
			baseDir: os.TempDir(),
			mock: func(s Storage) {
				s.(*storage).baseDir = "baz"
			},
			expect: func(t *testing.T, s Storage, baseDir string) {
				assert := assert.New(t)
				assert.Error(s.ClearProbes())

				s.(*storage).baseDir = baseDir
				assert.NoError(s.ClearProbes())
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
				err := s.(*storage).create(Record{}, RecordFilePrefix, RecordFileExt)
				assert.NoError(err)
			},
		},
		{
			name:    "create probes",
			baseDir: os.TempDir(),
			mock:    func(s Storage) {},
			expect: func(t *testing.T, s Storage, baseDir string) {
				assert := assert.New(t)
				err := s.(*storage).create(Probes{}, ProbesFilePrefix, ProbesFileExt)
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
				err := s.(*storage).create(Record{}, RecordFilePrefix, RecordFileExt)
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
			if err := s.ClearRecord(); err != nil {
				t.Fatal(err)
			}
			if err := s.ClearProbes(); err != nil {
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
				_, err := s.(*storage).openFile(RecordFilePrefix, RecordFileExt)
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
				file, err := s.(*storage).openFile(RecordFilePrefix, RecordFileExt)
				assert.NoError(err)
				assert.Equal(file.Name(), filepath.Join(baseDir, fmt.Sprintf("%s.%s", RecordFilePrefix, RecordFileExt)))
				file.Close()
			},
		},
		{
			name:       "open new probes file",
			baseDir:    os.TempDir(),
			maxSize:    0,
			maxBackups: config.DefaultStorageMaxBackups,
			bufferSize: 1,
			mock: func(t *testing.T, s Storage) {
				if err := s.Create(Probes{AverageRTT: 1}); err != nil {
					t.Fatal(err)
				}

				if err := s.Create(Probes{AverageRTT: 2}); err != nil {
					t.Fatal(err)
				}
			},
			expect: func(t *testing.T, s Storage, baseDir string) {
				assert := assert.New(t)
				file, err := s.(*storage).openFile(ProbesFilePrefix, ProbesFileExt)
				assert.NoError(err)
				assert.Equal(file.Name(), filepath.Join(baseDir, fmt.Sprintf("%s.%s", ProbesFilePrefix, ProbesFileExt)))
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
				file, err := s.(*storage).openFile(RecordFilePrefix, RecordFileExt)
				assert.NoError(err)
				assert.Equal(file.Name(), filepath.Join(baseDir, fmt.Sprintf("%s.%s", RecordFilePrefix, RecordFileExt)))
				file.Close()
			},
		},
		{
			name:       "remove probes file",
			baseDir:    os.TempDir(),
			maxSize:    0,
			maxBackups: 1,
			bufferSize: 1,
			mock: func(t *testing.T, s Storage) {
				if err := s.Create(Probes{AverageRTT: 1}); err != nil {
					t.Fatal(err)
				}
			},
			expect: func(t *testing.T, s Storage, baseDir string) {
				assert := assert.New(t)
				file, err := s.(*storage).openFile(ProbesFilePrefix, ProbesFileExt)
				assert.NoError(err)
				assert.Equal(file.Name(), filepath.Join(baseDir, fmt.Sprintf("%s.%s", ProbesFilePrefix, ProbesFileExt)))
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
			if err := s.ClearRecord(); err != nil {
				t.Fatal(err)
			}
			if err := s.ClearProbes(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestStorage_backupFilename(t *testing.T) {
	tests := []struct {
		name   string
		expect func(t *testing.T, s Storage)
	}{
		{
			name: "backup record file",
			expect: func(t *testing.T, s Storage) {
				assert := assert.New(t)
				filename := s.(*storage).backupFilename(RecordFilePrefix, RecordFileExt)
				regexp := regexp.MustCompile(fmt.Sprintf("%s-.*.%s$", RecordFilePrefix, RecordFileExt))
				assert.True(regexp.MatchString(filename))
			},
		},
		{
			name: "backup probes file",
			expect: func(t *testing.T, s Storage) {
				assert := assert.New(t)
				filename := s.(*storage).backupFilename(ProbesFilePrefix, ProbesFileExt)
				regexp := regexp.MustCompile(fmt.Sprintf("%s-.*.%s$", ProbesFilePrefix, ProbesFileExt))
				assert.True(regexp.MatchString(filename))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			baseDir := os.TempDir()
			s, err := New(baseDir, config.DefaultStorageMaxSize, config.DefaultStorageMaxBackups, config.DefaultStorageBufferSize)
			if err != nil {
				t.Fatal(err)
			}
			tc.expect(t, s)
			if err := s.ClearRecord(); err != nil {
				t.Fatal(err)
			}
			if err := s.ClearProbes(); err != nil {
				t.Fatal(err)
			}
		})
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
			name:    "open record file failed",
			baseDir: os.TempDir(),
			mock: func(t *testing.T, s Storage) {
				s.(*storage).baseDir = "bar"
			},
			expect: func(t *testing.T, s Storage, baseDir string) {
				assert := assert.New(t)
				_, err := s.(*storage).backups(RecordFilePrefix)
				assert.Error(err)
				s.(*storage).baseDir = baseDir
				if err := s.ClearRecord(); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name:    "open probes file failed",
			baseDir: os.TempDir(),
			mock: func(t *testing.T, s Storage) {
				s.(*storage).baseDir = "bar"
			},
			expect: func(t *testing.T, s Storage, baseDir string) {
				assert := assert.New(t)
				_, err := s.(*storage).backups(ProbesFilePrefix)
				assert.Error(err)
				s.(*storage).baseDir = baseDir
				if err := s.ClearProbes(); err != nil {
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
				if err := s.ClearRecord(); err != nil {
					t.Fatal(err)
				}

				_, err := s.(*storage).backups(RecordFilePrefix)
				assert.Error(err)
			},
		},
		{
			name:    "not found probes file",
			baseDir: os.TempDir(),
			mock:    func(t *testing.T, s Storage) {},
			expect: func(t *testing.T, s Storage, baseDir string) {
				assert := assert.New(t)
				if err := s.ClearProbes(); err != nil {
					t.Fatal(err)
				}

				_, err := s.(*storage).backups(ProbesFilePrefix)
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
