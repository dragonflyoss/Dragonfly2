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
		SchedulerClusterID: 1,
		CreatedAt:          time.Now().UnixNano(),
		UpdatedAt:          time.Now().UnixNano(),
	}

	mockPiece = Piece{
		Length:    20,
		Cost:      10,
		CreatedAt: time.Now().UnixNano(),
	}

	mockPieces = append(make([]Piece, 9), mockPiece)

	mockParent = Parent{
		ID:                 "4",
		Tag:                "m",
		Application:        "db",
		State:              "Succeeded",
		Cost:               1000,
		UploadPieceCount:   10,
		FinishedPieceCount: 10,
		Host:               mockHost,
		Pieces:             mockPieces,
		CreatedAt:          time.Now().UnixNano(),
		UpdatedAt:          time.Now().UnixNano(),
	}

	mockParents = append(make([]Parent, 19), mockParent)

	mockDownload = Download{
		ID:          "5",
		Tag:         "d",
		Application: "mq",
		State:       "Succeeded",
		Error: Error{
			Code:    "unknow",
			Message: "unknow",
		},
		Cost:               1000,
		FinishedPieceCount: 10,
		Task:               mockTask,
		Host:               mockHost,
		Parents:            mockParents,
		CreatedAt:          time.Now().UnixNano(),
		UpdatedAt:          time.Now().UnixNano(),
	}

	mockSrcHost = SrcHost{
		ID:       "3",
		Type:     "super",
		Hostname: "foo",
		IP:       "127.0.0.1",
		Port:     8080,
		Network: resource.Network{
			TCPConnectionCount:       400,
			UploadTCPConnectionCount: 200,
			Location:                 "china",
			IDC:                      "e1",
		},
	}

	mockDestHost = DestHost{
		ID:       "2",
		Type:     "normal",
		Hostname: "localhost",
		IP:       "127.0.0.1",
		Port:     8080,
		Network: resource.Network{
			TCPConnectionCount:       400,
			UploadTCPConnectionCount: 200,
			Location:                 "china",
			IDC:                      "e1",
		},
		Probes: Probes{
			AverageRTT: 10,
			CreatedAt:  time.Now().UnixNano(),
			UpdatedAt:  time.Now().UnixNano(),
		},
	}

	mockDestHosts = []DestHost{mockDestHost, mockDestHost, mockDestHost, mockDestHost, mockDestHost}

	mockNetworkTopology = NetworkTopology{
		ID:        "6",
		Host:      mockSrcHost,
		DestHosts: mockDestHosts,
		CreatedAt: time.Now().UnixNano(),
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

				assert.Equal(cap(s.(*storage).downloadBuffer), config.DefaultStorageBufferSize)
				assert.Equal(len(s.(*storage).downloadBuffer), 0)
				assert.Equal(s.(*storage).downloadCount, int64(0))

				assert.Equal(cap(s.(*storage).networkTopologyBuffer), config.DefaultStorageBufferSize)
				assert.Equal(len(s.(*storage).networkTopologyBuffer), 0)
				assert.Equal(s.(*storage).networkTopologyCount, int64(0))

				if err := s.ClearDownload(); err != nil {
					t.Fatal(err)
				}

				if err := s.ClearNetworkTopology(); err != nil {
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

func TestStorage_CreateDownload(t *testing.T) {
	tests := []struct {
		name       string
		baseDir    string
		bufferSize int
		mock       func(s Storage)
		expect     func(t *testing.T, s Storage, baseDir string)
	}{
		{
			name:       "create download",
			baseDir:    os.TempDir(),
			bufferSize: 1,
			mock:       func(s Storage) {},
			expect: func(t *testing.T, s Storage, baseDir string) {
				assert := assert.New(t)
				assert.NoError(s.CreateDownload(Download{}))
				assert.Equal(s.(*storage).downloadCount, int64(0))
			},
		},
		{
			name:       "create download without buffer",
			baseDir:    os.TempDir(),
			bufferSize: 0,
			mock: func(s Storage) {
			},
			expect: func(t *testing.T, s Storage, baseDir string) {
				assert := assert.New(t)
				assert.NoError(s.CreateDownload(Download{}))
				assert.Equal(s.(*storage).downloadCount, int64(1))

				downloads, err := s.ListDownload()
				assert.NoError(err)
				assert.Equal(len(downloads), 1)
			},
		},
		{
			name:       "write download to file",
			baseDir:    os.TempDir(),
			bufferSize: 1,
			mock: func(s Storage) {
			},
			expect: func(t *testing.T, s Storage, baseDir string) {
				assert := assert.New(t)
				assert.NoError(s.CreateDownload(Download{}))
				assert.NoError(s.CreateDownload(Download{}))
				assert.Equal(s.(*storage).downloadCount, int64(1))
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
				assert.Error(s.CreateDownload(Download{}))
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
			if err := s.ClearDownload(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestStorage_CreateNetworkTopology(t *testing.T) {
	tests := []struct {
		name       string
		baseDir    string
		bufferSize int
		mock       func(s Storage)
		expect     func(t *testing.T, s Storage, baseDir string)
	}{
		{
			name:       "create network topology",
			baseDir:    os.TempDir(),
			bufferSize: 1,
			mock:       func(s Storage) {},
			expect: func(t *testing.T, s Storage, baseDir string) {
				assert := assert.New(t)
				assert.NoError(s.CreateNetworkTopology(NetworkTopology{}))
				assert.Equal(s.(*storage).networkTopologyCount, int64(0))
			},
		},
		{
			name:       "create network topology without buffer",
			baseDir:    os.TempDir(),
			bufferSize: 0,
			mock: func(s Storage) {
			},
			expect: func(t *testing.T, s Storage, baseDir string) {
				assert := assert.New(t)
				assert.NoError(s.CreateNetworkTopology(NetworkTopology{}))
				assert.Equal(s.(*storage).networkTopologyCount, int64(1))

				networkTopologies, err := s.ListNetworkTopology()
				assert.NoError(err)
				assert.Equal(len(networkTopologies), 1)
			},
		},
		{
			name:       "write network topology to file",
			baseDir:    os.TempDir(),
			bufferSize: 1,
			mock: func(s Storage) {
			},
			expect: func(t *testing.T, s Storage, baseDir string) {
				assert := assert.New(t)
				assert.NoError(s.CreateNetworkTopology(NetworkTopology{}))
				assert.NoError(s.CreateNetworkTopology(NetworkTopology{}))
				assert.Equal(s.(*storage).networkTopologyCount, int64(1))
			},
		},
		{
			name:       "open file failed",
			baseDir:    os.TempDir(),
			bufferSize: 0,
			mock: func(s Storage) {
				s.(*storage).baseDir = "baw"
			},
			expect: func(t *testing.T, s Storage, baseDir string) {
				assert := assert.New(t)
				assert.Error(s.CreateNetworkTopology(NetworkTopology{}))
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
			if err := s.ClearNetworkTopology(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestStorage_ListDownload(t *testing.T) {
	tests := []struct {
		name       string
		baseDir    string
		bufferSize int
		download   Download
		mock       func(t *testing.T, s Storage, baseDir string, download Download)
		expect     func(t *testing.T, s Storage, baseDir string, download Download)
	}{
		{
			name:       "empty csv file given",
			baseDir:    os.TempDir(),
			bufferSize: config.DefaultStorageBufferSize,
			mock:       func(t *testing.T, s Storage, baseDir string, download Download) {},
			expect: func(t *testing.T, s Storage, baseDir string, download Download) {
				assert := assert.New(t)
				_, err := s.ListDownload()
				assert.Error(err)
			},
		},
		{
			name:       "get file infos failed",
			baseDir:    os.TempDir(),
			bufferSize: config.DefaultStorageBufferSize,
			mock: func(t *testing.T, s Storage, baseDir string, download Download) {
				s.(*storage).baseDir = "bae"
			},
			expect: func(t *testing.T, s Storage, baseDir string, download Download) {
				assert := assert.New(t)
				_, err := s.ListDownload()
				assert.Error(err)
				s.(*storage).baseDir = baseDir
			},
		},
		{
			name:       "open file failed",
			baseDir:    os.TempDir(),
			bufferSize: config.DefaultStorageBufferSize,
			mock: func(t *testing.T, s Storage, baseDir string, download Download) {
				file, err := os.OpenFile(filepath.Join(baseDir, "download_test.csv"), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0300)
				if err != nil {
					t.Fatal(err)
				}
				file.Close()
			},
			expect: func(t *testing.T, s Storage, baseDir string, download Download) {
				assert := assert.New(t)
				_, err := s.ListDownload()
				assert.Error(err)
			},
		},
		{
			name:       "list downloads of a file",
			baseDir:    os.TempDir(),
			bufferSize: 1,
			download:   mockDownload,
			mock: func(t *testing.T, s Storage, baseDir string, download Download) {
				if err := s.CreateDownload(download); err != nil {
					t.Fatal(err)
				}
			},
			expect: func(t *testing.T, s Storage, baseDir string, download Download) {
				assert := assert.New(t)
				_, err := s.ListDownload()
				assert.Error(err)

				if err := s.CreateDownload(download); err != nil {
					t.Fatal(err)
				}
				downloads, err := s.ListDownload()
				assert.NoError(err)
				assert.Equal(len(downloads), 1)
				assert.EqualValues(downloads[0].ID, download.ID)
				assert.EqualValues(downloads[0].Tag, download.Tag)
				assert.EqualValues(downloads[0].Application, download.Application)
				assert.EqualValues(downloads[0].State, download.State)
				assert.EqualValues(downloads[0].Error, download.Error)
				assert.EqualValues(downloads[0].Cost, download.Cost)
				assert.EqualValues(downloads[0].Task, download.Task)
				assert.EqualValues(downloads[0].Host, download.Host)
				assert.EqualValues(downloads[0].CreatedAt, download.CreatedAt)
				assert.EqualValues(downloads[0].UpdatedAt, download.UpdatedAt)
			},
		},
		{
			name:       "list downloads of multi files",
			baseDir:    os.TempDir(),
			bufferSize: 1,
			download:   Download{},
			mock: func(t *testing.T, s Storage, baseDir string, download Download) {
				if err := s.CreateDownload(Download{ID: "2"}); err != nil {
					t.Fatal(err)
				}

				if err := s.CreateDownload(Download{ID: "1"}); err != nil {
					t.Fatal(err)
				}

				if err := s.CreateDownload(Download{ID: "3"}); err != nil {
					t.Fatal(err)
				}
			},
			expect: func(t *testing.T, s Storage, baseDir string, download Download) {
				assert := assert.New(t)
				downloads, err := s.ListDownload()
				assert.NoError(err)
				assert.Equal(len(downloads), 2)
				assert.Equal(downloads[0].ID, "2")
				assert.Equal(downloads[1].ID, "1")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s, err := New(tc.baseDir, config.DefaultStorageMaxSize, config.DefaultStorageMaxBackups, tc.bufferSize)
			if err != nil {
				t.Fatal(err)
			}

			tc.mock(t, s, tc.baseDir, tc.download)
			tc.expect(t, s, tc.baseDir, tc.download)
			if err := s.ClearDownload(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestStorage_ListNetworkTopology(t *testing.T) {
	tests := []struct {
		name            string
		baseDir         string
		bufferSize      int
		networkTopology NetworkTopology
		mock            func(t *testing.T, s Storage, baseDir string, networkTopology NetworkTopology)
		expect          func(t *testing.T, s Storage, baseDir string, networkTopology NetworkTopology)
	}{
		{
			name:       "empty csv file given",
			baseDir:    os.TempDir(),
			bufferSize: config.DefaultStorageBufferSize,
			mock:       func(t *testing.T, s Storage, baseDir string, networkTopology NetworkTopology) {},
			expect: func(t *testing.T, s Storage, baseDir string, networkTopology NetworkTopology) {
				assert := assert.New(t)
				_, err := s.ListNetworkTopology()
				assert.Error(err)
			},
		},
		{
			name:       "get file infos failed",
			baseDir:    os.TempDir(),
			bufferSize: config.DefaultStorageBufferSize,
			mock: func(t *testing.T, s Storage, baseDir string, networkTopology NetworkTopology) {
				s.(*storage).baseDir = "bae"
			},
			expect: func(t *testing.T, s Storage, baseDir string, networkTopology NetworkTopology) {
				assert := assert.New(t)
				_, err := s.ListNetworkTopology()
				assert.Error(err)
				s.(*storage).baseDir = baseDir
			},
		},
		{
			name:       "open file failed",
			baseDir:    os.TempDir(),
			bufferSize: config.DefaultStorageBufferSize,
			mock: func(t *testing.T, s Storage, baseDir string, networkTopology NetworkTopology) {
				file, err := os.OpenFile(filepath.Join(baseDir, "networktopology_test.csv"), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0300)
				if err != nil {
					t.Fatal(err)
				}
				file.Close()
			},
			expect: func(t *testing.T, s Storage, baseDir string, networkTopology NetworkTopology) {
				assert := assert.New(t)
				_, err := s.ListNetworkTopology()
				assert.Error(err)
			},
		},
		{
			name:            "list network topologies of a file",
			baseDir:         os.TempDir(),
			bufferSize:      1,
			networkTopology: mockNetworkTopology,
			mock: func(t *testing.T, s Storage, baseDir string, networkTopology NetworkTopology) {
				if err := s.CreateNetworkTopology(networkTopology); err != nil {
					t.Fatal(err)
				}
			},
			expect: func(t *testing.T, s Storage, baseDir string, networkTopology NetworkTopology) {
				assert := assert.New(t)
				_, err := s.ListNetworkTopology()
				assert.Error(err)

				if err := s.CreateNetworkTopology(networkTopology); err != nil {
					t.Fatal(err)
				}
				networkTopologies, err := s.ListNetworkTopology()
				assert.NoError(err)
				assert.Equal(len(networkTopologies), 1)
				assert.EqualValues(networkTopologies[0], networkTopology)
			},
		},
		{
			name:            "list network topologies of multi files",
			baseDir:         os.TempDir(),
			bufferSize:      1,
			networkTopology: NetworkTopology{},
			mock: func(t *testing.T, s Storage, baseDir string, networkTopology NetworkTopology) {
				if err := s.CreateNetworkTopology(NetworkTopology{ID: "2"}); err != nil {
					t.Fatal(err)
				}

				if err := s.CreateNetworkTopology(NetworkTopology{ID: "1"}); err != nil {
					t.Fatal(err)
				}

				if err := s.CreateNetworkTopology(NetworkTopology{ID: "3"}); err != nil {
					t.Fatal(err)
				}
			},
			expect: func(t *testing.T, s Storage, baseDir string, networkTopology NetworkTopology) {
				assert := assert.New(t)
				networkTopologies, err := s.ListNetworkTopology()
				assert.NoError(err)
				assert.Equal(len(networkTopologies), 2)
				assert.Equal(networkTopologies[0].ID, "2")
				assert.Equal(networkTopologies[1].ID, "1")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s, err := New(tc.baseDir, config.DefaultStorageMaxSize, config.DefaultStorageMaxBackups, tc.bufferSize)
			if err != nil {
				t.Fatal(err)
			}

			tc.mock(t, s, tc.baseDir, tc.networkTopology)
			tc.expect(t, s, tc.baseDir, tc.networkTopology)
			if err := s.ClearNetworkTopology(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestStorage_DownloadCount(t *testing.T) {
	tests := []struct {
		name    string
		baseDir string
		mock    func(s Storage)
		expect  func(t *testing.T, s Storage)
	}{
		{
			name:    "get the count of downloads",
			baseDir: os.TempDir(),
			mock: func(s Storage) {
				s.(*storage).downloadCount = 2
			},
			expect: func(t *testing.T, s Storage) {
				assert := assert.New(t)
				assert.Equal(int64(2), s.DownloadCount())
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
			tc.expect(t, s)
		})
	}
}

func TestStorage_NetworkTopologyCount(t *testing.T) {
	tests := []struct {
		name    string
		baseDir string
		mock    func(s Storage)
		expect  func(t *testing.T, s Storage)
	}{
		{
			name:    "get the count of network topologies",
			baseDir: os.TempDir(),
			mock: func(s Storage) {
				s.(*storage).networkTopologyCount = 1
			},
			expect: func(t *testing.T, s Storage) {
				assert := assert.New(t)
				assert.Equal(int64(1), s.NetworkTopologyCount())
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
			tc.expect(t, s)
		})
	}
}

func TestStorage_OpenDownload(t *testing.T) {
	tests := []struct {
		name       string
		baseDir    string
		bufferSize int
		download   Download
		mock       func(t *testing.T, s Storage, baseDir string, download Download)
		expect     func(t *testing.T, s Storage, baseDir string, download Download)
	}{
		{
			name:       "open storage withempty csv file given",
			baseDir:    os.TempDir(),
			bufferSize: config.DefaultStorageBufferSize,
			mock:       func(t *testing.T, s Storage, baseDir string, download Download) {},
			expect: func(t *testing.T, s Storage, baseDir string, download Download) {
				assert := assert.New(t)
				_, err := s.OpenDownload()
				assert.NoError(err)
			},
		},
		{
			name:       "open file infos failed",
			baseDir:    os.TempDir(),
			bufferSize: config.DefaultStorageBufferSize,
			mock: func(t *testing.T, s Storage, baseDir string, download Download) {
				s.(*storage).baseDir = "bas"
			},
			expect: func(t *testing.T, s Storage, baseDir string, download Download) {
				assert := assert.New(t)
				_, err := s.OpenDownload()
				assert.Error(err)
				s.(*storage).baseDir = baseDir
			},
		},
		{
			name:       "open storage with downloads of a file",
			baseDir:    os.TempDir(),
			bufferSize: 1,
			download:   mockDownload,
			mock: func(t *testing.T, s Storage, baseDir string, download Download) {
				if err := s.CreateDownload(download); err != nil {
					t.Fatal(err)
				}
			},
			expect: func(t *testing.T, s Storage, baseDir string, download Download) {
				assert := assert.New(t)
				_, err := s.OpenDownload()
				assert.NoError(err)

				if err := s.CreateDownload(download); err != nil {
					t.Fatal(err)
				}

				readCloser, err := s.OpenDownload()
				assert.NoError(err)

				var downloads []Download
				err = gocsv.UnmarshalWithoutHeaders(readCloser, &downloads)
				assert.NoError(err)
				assert.Equal(len(downloads), 1)
				assert.EqualValues(downloads[0].ID, download.ID)
				assert.EqualValues(downloads[0].Tag, download.Tag)
				assert.EqualValues(downloads[0].Application, download.Application)
				assert.EqualValues(downloads[0].State, download.State)
				assert.EqualValues(downloads[0].Error, download.Error)
				assert.EqualValues(downloads[0].Cost, download.Cost)
				assert.EqualValues(downloads[0].Task, download.Task)
				assert.EqualValues(downloads[0].Host, download.Host)
				assert.EqualValues(downloads[0].CreatedAt, download.CreatedAt)
				assert.EqualValues(downloads[0].UpdatedAt, download.UpdatedAt)
			},
		},
		{
			name:       "open storage with downloads of multi files",
			baseDir:    os.TempDir(),
			bufferSize: 1,
			download:   Download{},
			mock: func(t *testing.T, s Storage, baseDir string, download Download) {
				if err := s.CreateDownload(Download{ID: "2"}); err != nil {
					t.Fatal(err)
				}

				if err := s.CreateDownload(Download{ID: "1"}); err != nil {
					t.Fatal(err)
				}

				if err := s.CreateDownload(Download{ID: "3"}); err != nil {
					t.Fatal(err)
				}
			},
			expect: func(t *testing.T, s Storage, baseDir string, download Download) {
				assert := assert.New(t)
				readCloser, err := s.OpenDownload()
				assert.NoError(err)

				var downloads []Download
				err = gocsv.UnmarshalWithoutHeaders(readCloser, &downloads)
				assert.NoError(err)
				assert.Equal(len(downloads), 2)
				assert.Equal(downloads[0].ID, "2")
				assert.Equal(downloads[1].ID, "1")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s, err := New(tc.baseDir, config.DefaultStorageMaxSize, config.DefaultStorageMaxBackups, tc.bufferSize)
			if err != nil {
				t.Fatal(err)
			}

			tc.mock(t, s, tc.baseDir, tc.download)
			tc.expect(t, s, tc.baseDir, tc.download)
			if err := s.ClearDownload(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestStorage_OpenNetworkTopology(t *testing.T) {
	tests := []struct {
		name            string
		baseDir         string
		bufferSize      int
		networkTopology NetworkTopology
		mock            func(t *testing.T, s Storage, baseDir string, networkTopology NetworkTopology)
		expect          func(t *testing.T, s Storage, baseDir string, networkTopology NetworkTopology)
	}{
		{
			name:       "open storage with empty csv file given",
			baseDir:    os.TempDir(),
			bufferSize: config.DefaultStorageBufferSize,
			mock:       func(t *testing.T, s Storage, baseDir string, networkTopology NetworkTopology) {},
			expect: func(t *testing.T, s Storage, baseDir string, networkTopology NetworkTopology) {
				assert := assert.New(t)
				_, err := s.OpenNetworkTopology()
				assert.NoError(err)
			},
		},
		{
			name:       "open file infos failed",
			baseDir:    os.TempDir(),
			bufferSize: config.DefaultStorageBufferSize,
			mock: func(t *testing.T, s Storage, baseDir string, networkTopology NetworkTopology) {
				s.(*storage).baseDir = "bas"
			},
			expect: func(t *testing.T, s Storage, baseDir string, networkTopology NetworkTopology) {
				assert := assert.New(t)
				_, err := s.OpenNetworkTopology()
				assert.Error(err)
				s.(*storage).baseDir = baseDir
			},
		},
		{
			name:            "open storage with network topologies of a file",
			baseDir:         os.TempDir(),
			bufferSize:      1,
			networkTopology: mockNetworkTopology,
			mock: func(t *testing.T, s Storage, baseDir string, networkTopology NetworkTopology) {
				if err := s.CreateNetworkTopology(networkTopology); err != nil {
					t.Fatal(err)
				}
			},
			expect: func(t *testing.T, s Storage, baseDir string, networkTopology NetworkTopology) {
				assert := assert.New(t)
				_, err := s.OpenNetworkTopology()
				assert.NoError(err)

				if err := s.CreateNetworkTopology(networkTopology); err != nil {
					t.Fatal(err)
				}

				readCloser, err := s.OpenNetworkTopology()
				assert.NoError(err)

				var networkTopologies []NetworkTopology
				err = gocsv.UnmarshalWithoutHeaders(readCloser, &networkTopologies)
				assert.NoError(err)
				assert.Equal(len(networkTopologies), 1)
				assert.EqualValues(networkTopologies[0], networkTopology)
			},
		},
		{
			name:            "open storage with network topologies of multi files",
			baseDir:         os.TempDir(),
			bufferSize:      1,
			networkTopology: NetworkTopology{},
			mock: func(t *testing.T, s Storage, baseDir string, networkTopology NetworkTopology) {
				if err := s.CreateNetworkTopology(NetworkTopology{ID: "2"}); err != nil {
					t.Fatal(err)
				}

				if err := s.CreateNetworkTopology(NetworkTopology{ID: "1"}); err != nil {
					t.Fatal(err)
				}

				if err := s.CreateNetworkTopology(NetworkTopology{ID: "3"}); err != nil {
					t.Fatal(err)
				}
			},
			expect: func(t *testing.T, s Storage, baseDir string, networkTopology NetworkTopology) {
				assert := assert.New(t)
				readCloser, err := s.OpenNetworkTopology()
				assert.NoError(err)

				var networkTopologies []NetworkTopology
				err = gocsv.UnmarshalWithoutHeaders(readCloser, &networkTopologies)
				assert.NoError(err)
				assert.Equal(len(networkTopologies), 2)
				assert.Equal(networkTopologies[0].ID, "2")
				assert.Equal(networkTopologies[1].ID, "1")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s, err := New(tc.baseDir, config.DefaultStorageMaxSize, config.DefaultStorageMaxBackups, tc.bufferSize)
			if err != nil {
				t.Fatal(err)
			}

			tc.mock(t, s, tc.baseDir, tc.networkTopology)
			tc.expect(t, s, tc.baseDir, tc.networkTopology)
			if err := s.ClearNetworkTopology(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestStorage_ClearDownload(t *testing.T) {
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
				assert.NoError(s.ClearDownload())
				fileInfos, err := os.ReadDir(filepath.Join(baseDir))
				assert.NoError(err)

				var backups []os.DirEntry
				regexp := regexp.MustCompile(DownloadFilePrefix)
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
				assert.Error(s.ClearDownload())

				s.(*storage).baseDir = baseDir
				assert.NoError(s.ClearDownload())
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

func TestStorage_ClearNetworkTopology(t *testing.T) {
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
				assert.NoError(s.ClearNetworkTopology())
				fileInfos, err := os.ReadDir(filepath.Join(baseDir))
				assert.NoError(err)

				var backups []fs.FileInfo
				regexp := regexp.MustCompile(NetworkTopologyFilePrefix)
				for _, fileInfo := range fileInfos {
					if !fileInfo.IsDir() && regexp.MatchString(fileInfo.Name()) {
						info, _ := fileInfo.Info()
						backups = append(backups, info)
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
				assert.Error(s.ClearNetworkTopology())

				s.(*storage).baseDir = baseDir
				assert.NoError(s.ClearNetworkTopology())
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

func TestStorage_createDownload(t *testing.T) {
	tests := []struct {
		name    string
		baseDir string
		mock    func(s Storage)
		expect  func(t *testing.T, s Storage, baseDir string)
	}{
		{
			name:    "create download",
			baseDir: os.TempDir(),
			mock:    func(s Storage) {},
			expect: func(t *testing.T, s Storage, baseDir string) {
				assert := assert.New(t)
				assert.NoError(s.(*storage).createDownload(Download{}))
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
				assert.Error(s.(*storage).createDownload(Download{}))
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
			if err := s.ClearDownload(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestStorage_createNetworkTopology(t *testing.T) {
	tests := []struct {
		name    string
		baseDir string
		mock    func(s Storage)
		expect  func(t *testing.T, s Storage, baseDir string)
	}{
		{
			name:    "create network topology",
			baseDir: os.TempDir(),
			mock:    func(s Storage) {},
			expect: func(t *testing.T, s Storage, baseDir string) {
				assert := assert.New(t)
				assert.NoError(s.(*storage).createNetworkTopology(NetworkTopology{}))
			},
		},
		{
			name:    "open file failed",
			baseDir: os.TempDir(),
			mock: func(s Storage) {
				s.(*storage).baseDir = "baw"
			},
			expect: func(t *testing.T, s Storage, baseDir string) {
				assert := assert.New(t)
				assert.Error(s.(*storage).createNetworkTopology(NetworkTopology{}))
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
			if err := s.ClearNetworkTopology(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestStorage_openDownloadFile(t *testing.T) {
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
				_, err := s.(*storage).openDownloadFile()
				assert.Error(err)
				s.(*storage).baseDir = baseDir
			},
		},
		{
			name:       "open new download file",
			baseDir:    os.TempDir(),
			maxSize:    0,
			maxBackups: config.DefaultStorageMaxBackups,
			bufferSize: 1,
			mock: func(t *testing.T, s Storage) {
				if err := s.CreateDownload(Download{ID: "1"}); err != nil {
					t.Fatal(err)
				}

				if err := s.CreateDownload(Download{ID: "2"}); err != nil {
					t.Fatal(err)
				}
			},
			expect: func(t *testing.T, s Storage, baseDir string) {
				assert := assert.New(t)
				file, err := s.(*storage).openDownloadFile()
				assert.NoError(err)
				assert.Equal(file.Name(), filepath.Join(baseDir, fmt.Sprintf("%s.%s", DownloadFilePrefix, CSVFileExt)))
				file.Close()
			},
		},
		{
			name:       "remove download file",
			baseDir:    os.TempDir(),
			maxSize:    0,
			maxBackups: 1,
			bufferSize: 1,
			mock: func(t *testing.T, s Storage) {
				if err := s.CreateDownload(Download{ID: "1"}); err != nil {
					t.Fatal(err)
				}
			},
			expect: func(t *testing.T, s Storage, baseDir string) {
				assert := assert.New(t)
				file, err := s.(*storage).openDownloadFile()
				assert.NoError(err)
				assert.Equal(file.Name(), filepath.Join(baseDir, fmt.Sprintf("%s.%s", DownloadFilePrefix, CSVFileExt)))
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
			if err := s.ClearDownload(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestStorage_openNetworkTopologyFile(t *testing.T) {
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
				_, err := s.(*storage).openNetworkTopologyFile()
				assert.Error(err)
				s.(*storage).baseDir = baseDir
			},
		},
		{
			name:       "open new network topology file",
			baseDir:    os.TempDir(),
			maxSize:    0,
			maxBackups: config.DefaultStorageMaxBackups,
			bufferSize: 1,
			mock: func(t *testing.T, s Storage) {
				if err := s.CreateNetworkTopology(NetworkTopology{ID: "1"}); err != nil {
					t.Fatal(err)
				}

				if err := s.CreateNetworkTopology(NetworkTopology{ID: "2"}); err != nil {
					t.Fatal(err)
				}
			},
			expect: func(t *testing.T, s Storage, baseDir string) {
				assert := assert.New(t)
				file, err := s.(*storage).openNetworkTopologyFile()
				assert.NoError(err)
				assert.Equal(file.Name(), filepath.Join(baseDir, fmt.Sprintf("%s.%s", NetworkTopologyFilePrefix, CSVFileExt)))
				file.Close()
			},
		},
		{
			name:       "remove network topology file",
			baseDir:    os.TempDir(),
			maxSize:    0,
			maxBackups: 1,
			bufferSize: 1,
			mock: func(t *testing.T, s Storage) {
				if err := s.CreateNetworkTopology(NetworkTopology{ID: "1"}); err != nil {
					t.Fatal(err)
				}
			},
			expect: func(t *testing.T, s Storage, baseDir string) {
				assert := assert.New(t)
				file, err := s.(*storage).openNetworkTopologyFile()
				assert.NoError(err)
				assert.Equal(file.Name(), filepath.Join(baseDir, fmt.Sprintf("%s.%s", NetworkTopologyFilePrefix, CSVFileExt)))
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
			if err := s.ClearNetworkTopology(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestStorage_downloadBackupFilename(t *testing.T) {
	baseDir := os.TempDir()
	s, err := New(baseDir, config.DefaultStorageMaxSize, config.DefaultStorageMaxBackups, config.DefaultStorageBufferSize)
	if err != nil {
		t.Fatal(err)
	}

	filename := s.(*storage).downloadBackupFilename()
	regexp := regexp.MustCompile(fmt.Sprintf("%s_.*.%s$", DownloadFilePrefix, CSVFileExt))
	assert := assert.New(t)
	assert.True(regexp.MatchString(filename))

	if err := s.ClearDownload(); err != nil {
		t.Fatal(err)
	}
}

func TestStorage_networkTopologyBackupFilename(t *testing.T) {
	baseDir := os.TempDir()
	s, err := New(baseDir, config.DefaultStorageMaxSize, config.DefaultStorageMaxBackups, config.DefaultStorageBufferSize)
	if err != nil {
		t.Fatal(err)
	}

	filename := s.(*storage).networkTopologyBackupFilename()
	regexp := regexp.MustCompile(fmt.Sprintf("%s_.*.%s$", NetworkTopologyFilePrefix, CSVFileExt))
	assert := assert.New(t)
	assert.True(regexp.MatchString(filename))

	if err := s.ClearNetworkTopology(); err != nil {
		t.Fatal(err)
	}
}

func TestStorage_downloadBackups(t *testing.T) {
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
				_, err := s.(*storage).downloadBackups()
				assert.Error(err)
				s.(*storage).baseDir = baseDir
				if err := s.ClearDownload(); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name:    "not found download file",
			baseDir: os.TempDir(),
			mock:    func(t *testing.T, s Storage) {},
			expect: func(t *testing.T, s Storage, baseDir string) {
				assert := assert.New(t)
				if err := s.ClearDownload(); err != nil {
					t.Fatal(err)
				}

				_, err := s.(*storage).downloadBackups()
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

func TestStorage_networkTopologyBackups(t *testing.T) {
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
				_, err := s.(*storage).networkTopologyBackups()
				assert.Error(err)
				s.(*storage).baseDir = baseDir
				if err := s.ClearNetworkTopology(); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name:    "not found network topology file",
			baseDir: os.TempDir(),
			mock:    func(t *testing.T, s Storage) {},
			expect: func(t *testing.T, s Storage, baseDir string) {
				assert := assert.New(t)
				if err := s.ClearNetworkTopology(); err != nil {
					t.Fatal(err)
				}

				_, err := s.(*storage).networkTopologyBackups()
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
