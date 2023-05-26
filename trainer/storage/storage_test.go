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

	"github.com/gocarina/gocsv"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"d7y.io/dragonfly/v2/scheduler/config"
)

var (
	mockModelKey = "hostname_127.0.0.1_2"
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
				assert.Equal(s.(*storage).maxSize, int64(DefaultStorageMaxSize*megabyte))
				assert.Equal(s.(*storage).maxBackups, DefaultStorageMaxBackups)
				assert.Equal(len(s.(*storage).downloadModelKey), 0)
				assert.Equal(len(s.(*storage).networkTopologyModelKey), 0)

			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s, err := New(tc.baseDir, DefaultStorageMaxSize, DefaultStorageMaxBackups)
			tc.expect(t, s, err)
		})
	}
}

func TestStorage_CreateTempDownload(t *testing.T) {
	require := require.New(t)
	testData, err := os.ReadFile("./testdata/download.csv")
	require.Nil(err, "load test file")

	tests := []struct {
		name    string
		baseDir string
		mock    func(s Storage, baseDir string)
		expect  func(t *testing.T, s Storage, baseDir, modelKey string, download []byte)
	}{
		{
			name:    "write temp download without temp file",
			baseDir: os.TempDir(),
			mock:    func(s Storage, baseDir string) {},
			expect: func(t *testing.T, s Storage, baseDir, modelKey string, download []byte) {
				assert := assert.New(t)
				err := s.CreateTempDownload(download, modelKey)
				assert.NoError(err)
				if err := s.(*storage).clearDownloadTempFile(); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name:    "write temp download with temp file",
			baseDir: os.TempDir(),
			mock: func(s Storage, baseDir string) {
				file, err := os.OpenFile(filepath.Join(baseDir, "download-temp-hostname_127.0.0.1_2.csv"), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
				if err != nil {
					t.Fatal(err)
				}
				file.Close()
			},
			expect: func(t *testing.T, s Storage, baseDir, modelKey string, download []byte) {
				assert := assert.New(t)
				err := s.CreateTempDownload(download, modelKey)
				assert.NoError(err)
				if err := s.(*storage).clearDownloadTempFile(); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name:    "open temp file failed",
			baseDir: os.TempDir(),
			mock: func(s Storage, baseDir string) {
				s.(*storage).baseDir = "baz"
			},
			expect: func(t *testing.T, s Storage, baseDir, modelKey string, download []byte) {
				assert := assert.New(t)
				err := s.CreateTempDownload(download, modelKey)
				assert.Error(err)
				s.(*storage).baseDir = baseDir
				err = s.CreateTempDownload(download, modelKey)
				assert.NoError(err)
				if err := s.(*storage).clearDownloadTempFile(); err != nil {
					t.Fatal(err)
				}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s, err := New(tc.baseDir, DefaultStorageMaxSize, DefaultStorageMaxBackups)
			if err != nil {
				t.Fatal(err)
			}

			tc.mock(s, tc.baseDir)
			tc.expect(t, s, tc.baseDir, mockModelKey, testData)
		})
	}
}

func TestStorage_CreateTempNetworkTopology(t *testing.T) {
	require := require.New(t)
	testData, err := os.ReadFile("./testdata/networktopology.csv")
	require.Nil(err, "load test file")

	tests := []struct {
		name    string
		baseDir string
		mock    func(s Storage, baseDir string)
		expect  func(t *testing.T, s Storage, baseDir, modelKey string, download []byte)
	}{
		{
			name:    "write temp download without temp file initialization",
			baseDir: os.TempDir(),
			mock:    func(s Storage, baseDir string) {},
			expect: func(t *testing.T, s Storage, baseDir, modelKey string, download []byte) {
				assert := assert.New(t)
				err := s.CreateTempNetworkTopology(download, modelKey)
				assert.NoError(err)
				if err := s.(*storage).clearNetworkTopologyTempFile(); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name:    "write temp download with temp file initialization",
			baseDir: os.TempDir(),
			mock: func(s Storage, baseDir string) {
				file, err := os.OpenFile(filepath.Join(baseDir, "download-temp-hostname_127.0.0.1_2.csv"), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
				if err != nil {
					t.Fatal(err)
				}
				file.Close()
			},
			expect: func(t *testing.T, s Storage, baseDir, modelKey string, download []byte) {
				assert := assert.New(t)
				err := s.CreateTempNetworkTopology(download, modelKey)
				assert.NoError(err)
				if err := s.(*storage).clearNetworkTopologyTempFile(); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name:    "open temp file failed",
			baseDir: os.TempDir(),
			mock: func(s Storage, baseDir string) {
				s.(*storage).baseDir = "baz"
			},
			expect: func(t *testing.T, s Storage, baseDir, modelKey string, download []byte) {
				assert := assert.New(t)
				err := s.CreateTempNetworkTopology(download, modelKey)
				assert.Error(err)
				s.(*storage).baseDir = baseDir
				err = s.CreateTempNetworkTopology(download, modelKey)
				assert.NoError(err)
				if err := s.(*storage).clearNetworkTopologyTempFile(); err != nil {
					t.Fatal(err)
				}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s, err := New(tc.baseDir, DefaultStorageMaxSize, DefaultStorageMaxBackups)
			if err != nil {
				t.Fatal(err)
			}

			tc.mock(s, tc.baseDir)
			tc.expect(t, s, tc.baseDir, mockModelKey, testData)
		})
	}
}

func TestStorage_CreateDownload(t *testing.T) {
	require := require.New(t)
	testData, err := os.ReadFile("./testdata/download.csv")
	require.Nil(err, "load test file")

	tests := []struct {
		name    string
		baseDir string
		mock    func(s Storage, baseDir, modelKey string, download []byte)
		expect  func(t *testing.T, s Storage, baseDir, modelKey string)
	}{
		{
			name:    "create download",
			baseDir: os.TempDir(),
			mock: func(s Storage, baseDir, modelKey string, download []byte) {
				if err := s.(*storage).CreateTempDownload(download, modelKey); err != nil {
					t.Fatal(err)
				}
			},
			expect: func(t *testing.T, s Storage, baseDir, modelKey string) {
				assert := assert.New(t)
				assert.Equal(len(s.(*storage).downloadModelKey), 0)
				err := s.CreateDownload(modelKey)
				assert.NoError(err)
				assert.Equal(len(s.(*storage).downloadModelKey), 1)
				err = s.(*storage).clearDownloadTempFile()
				assert.Error(err)

			},
		},
		{
			name:    "open file failed",
			baseDir: os.TempDir(),
			mock: func(s Storage, baseDir, modelKey string, download []byte) {
				if err := s.(*storage).CreateTempDownload(download, modelKey); err != nil {
					t.Fatal(err)
				}
				s.(*storage).baseDir = "bar"
			},
			expect: func(t *testing.T, s Storage, baseDir, modelKey string) {
				assert := assert.New(t)
				err := s.CreateDownload(modelKey)
				assert.Error(err)
				s.(*storage).baseDir = baseDir
				err = s.CreateDownload(modelKey)
				assert.NoError(err)
				assert.Equal(len(s.(*storage).downloadModelKey), 1)
				err = s.(*storage).clearDownloadTempFile()
				assert.Error(err)

			},
		},
		{
			name:    "temp file not found",
			baseDir: os.TempDir(),
			mock:    func(s Storage, baseDir, modelKey string, download []byte) {},

			expect: func(t *testing.T, s Storage, baseDir, modelKey string) {
				assert := assert.New(t)
				err := s.CreateDownload(modelKey)
				assert.Error(err)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s, err := New(tc.baseDir, DefaultStorageMaxSize, DefaultStorageMaxBackups)
			if err != nil {
				t.Fatal(err)
			}

			tc.mock(s, tc.baseDir, mockModelKey, testData)
			tc.expect(t, s, tc.baseDir, mockModelKey)
			if err := s.ClearDownload(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestStorage_CreateNetworkTopology(t *testing.T) {
	require := require.New(t)
	testData, err := os.ReadFile("./testdata/networktopology.csv")
	require.Nil(err, "load test file")

	tests := []struct {
		name    string
		baseDir string
		mock    func(s Storage, baseDir, modelKey string, download []byte)
		expect  func(t *testing.T, s Storage, baseDir, modelKey string)
	}{
		{
			name:    "create networkTopology",
			baseDir: os.TempDir(),
			mock: func(s Storage, baseDir, modelKey string, download []byte) {
				if err := s.(*storage).CreateTempNetworkTopology(download, modelKey); err != nil {
					t.Fatal(err)
				}
			},
			expect: func(t *testing.T, s Storage, baseDir, modelKey string) {
				assert := assert.New(t)
				assert.Equal(len(s.(*storage).networkTopologyModelKey), 0)
				err := s.CreateNetworkTopology(modelKey)
				assert.NoError(err)
				assert.Equal(len(s.(*storage).networkTopologyModelKey), 1)
				err = s.(*storage).clearNetworkTopologyTempFile()
				assert.Error(err)

			},
		},
		{
			name:    "open file failed",
			baseDir: os.TempDir(),
			mock: func(s Storage, baseDir, modelKey string, download []byte) {
				if err := s.(*storage).CreateTempNetworkTopology(download, modelKey); err != nil {
					t.Fatal(err)
				}
				s.(*storage).baseDir = "bar"
			},
			expect: func(t *testing.T, s Storage, baseDir, modelKey string) {
				assert := assert.New(t)
				err := s.CreateNetworkTopology(modelKey)
				assert.Error(err)
				s.(*storage).baseDir = baseDir
				err = s.CreateNetworkTopology(modelKey)
				assert.NoError(err)
				assert.Equal(len(s.(*storage).networkTopologyModelKey), 1)
				err = s.(*storage).clearNetworkTopologyTempFile()
				assert.Error(err)

			},
		},
		{
			name:    "temp file not found",
			baseDir: os.TempDir(),
			mock:    func(s Storage, baseDir, modelKey string, download []byte) {},

			expect: func(t *testing.T, s Storage, baseDir, modelKey string) {
				assert := assert.New(t)
				err := s.CreateNetworkTopology(modelKey)
				assert.Error(err)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s, err := New(tc.baseDir, DefaultStorageMaxSize, DefaultStorageMaxBackups)
			if err != nil {
				t.Fatal(err)
			}

			tc.mock(s, tc.baseDir, mockModelKey, testData)
			tc.expect(t, s, tc.baseDir, mockModelKey)
			if err := s.ClearNetworkTopology(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestStorage_ListDownload(t *testing.T) {
	require := require.New(t)
	testData, err := os.ReadFile("./testdata/download.csv")
	require.Nil(err, "load test file")

	tests := []struct {
		name    string
		baseDir string
		mock    func(t *testing.T, s Storage, baseDir, modelKey string, download []byte)
		expect  func(t *testing.T, s Storage, baseDir, modelKey string, download []byte)
	}{
		{
			name:    "empty csv file given for selected model",
			baseDir: os.TempDir(),
			mock: func(t *testing.T, s Storage, baseDir, modelKey string, download []byte) {
				file, err := os.OpenFile(filepath.Join(baseDir, "download-hostname_127.0.0.1_2.csv"), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
				if err != nil {
					t.Fatal(err)
				}
				file.Close()
				s.(*storage).downloadModelKey[modelKey] = struct{}{}
			},
			expect: func(t *testing.T, s Storage, baseDir, modelKey string, download []byte) {
				assert := assert.New(t)
				_, err := s.ListDownload(modelKey)
				assert.Error(err)
			},
		},
		{
			name:    "get file infos failed",
			baseDir: os.TempDir(),
			mock: func(t *testing.T, s Storage, baseDir, modelKey string, download []byte) {
				s.(*storage).baseDir = "bas"
			},
			expect: func(t *testing.T, s Storage, baseDir, modelKey string, download []byte) {
				assert := assert.New(t)
				_, err := s.ListDownload(modelKey)
				assert.Error(err)
				s.(*storage).baseDir = baseDir
			},
		},
		{
			name:    "open file failed",
			baseDir: os.TempDir(),
			mock: func(t *testing.T, s Storage, baseDir, modelKey string, download []byte) {
				file, err := os.OpenFile(filepath.Join(baseDir, "download-hostname_127.0.0.1_2-test.csv"), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
				if err != nil {
					t.Fatal(err)
				}
				file.Close()
				s.(*storage).downloadModelKey[modelKey] = struct{}{}

			},
			expect: func(t *testing.T, s Storage, baseDir, modelKey string, download []byte) {
				assert := assert.New(t)
				_, err := s.ListDownload(modelKey)
				assert.Error(err)
			},
		},
		{
			name:    "list downloads of a file",
			baseDir: os.TempDir(),
			mock: func(t *testing.T, s Storage, baseDir, modelKey string, download []byte) {
				if err := s.(*storage).CreateTempDownload(download, modelKey); err != nil {
					t.Fatal(err)
				}

				if err := s.CreateDownload(modelKey); err != nil {
					t.Fatal(err)
				}
			},
			expect: func(t *testing.T, s Storage, baseDir, modelKey string, download []byte) {
				assert := assert.New(t)
				_, err := s.ListDownload(modelKey)
				assert.NoError(err)

				if err := s.(*storage).CreateTempDownload(download, modelKey); err != nil {
					t.Fatal(err)
				}

				if err := s.CreateDownload(modelKey); err != nil {
					t.Fatal(err)
				}

				downloads, err := s.ListDownload(modelKey)
				assert.NoError(err)
				assert.Equal(len(downloads), 2)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s, err := New(tc.baseDir, DefaultStorageMaxSize, DefaultStorageMaxBackups)
			if err != nil {
				t.Fatal(err)
			}
			tc.mock(t, s, tc.baseDir, mockModelKey, testData)
			tc.expect(t, s, tc.baseDir, mockModelKey, testData)
			if err := s.ClearDownload(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestStorage_ListNetworkTopology(t *testing.T) {
	require := require.New(t)
	testData, err := os.ReadFile("./testdata/networktopology.csv")
	require.Nil(err, "load test file")

	tests := []struct {
		name    string
		baseDir string
		mock    func(t *testing.T, s Storage, baseDir, modelKey string, networkTopology []byte)
		expect  func(t *testing.T, s Storage, baseDir, modelKey string, networkTopology []byte)
	}{
		{
			name:    "empty csv file given for selected model",
			baseDir: os.TempDir(),
			mock: func(t *testing.T, s Storage, baseDir, modelKey string, networkTopology []byte) {
				file, err := os.OpenFile(filepath.Join(baseDir, "networktopology-hostname_127.0.0.1_2.csv"), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
				if err != nil {
					t.Fatal(err)
				}
				file.Close()
				s.(*storage).networkTopologyModelKey[modelKey] = struct{}{}
			},
			expect: func(t *testing.T, s Storage, baseDir, modelKey string, networkTopology []byte) {
				assert := assert.New(t)
				_, err := s.ListNetworkTopology(modelKey)
				assert.Error(err)
			},
		},
		{
			name:    "get file infos failed",
			baseDir: os.TempDir(),
			mock: func(t *testing.T, s Storage, baseDir, modelKey string, networkTopology []byte) {
				s.(*storage).baseDir = "bae"
			},
			expect: func(t *testing.T, s Storage, baseDir, modelKey string, networkTopology []byte) {
				assert := assert.New(t)
				_, err := s.ListNetworkTopology(modelKey)
				assert.Error(err)
				s.(*storage).baseDir = baseDir
			},
		},
		{
			name:    "open file failed",
			baseDir: os.TempDir(),
			mock: func(t *testing.T, s Storage, baseDir, modelKey string, networkTopology []byte) {
				file, err := os.OpenFile(filepath.Join(baseDir, "networktopology-hostname_127.0.0.1_2-test.csv"), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
				if err != nil {
					t.Fatal(err)
				}
				file.Close()
				s.(*storage).networkTopologyModelKey[modelKey] = struct{}{}
			},
			expect: func(t *testing.T, s Storage, baseDir, modelKey string, networkTopology []byte) {
				assert := assert.New(t)
				_, err := s.ListNetworkTopology(modelKey)
				assert.Error(err)
			},
		},
		{
			name:    "list network topologies of a file",
			baseDir: os.TempDir(),
			mock: func(t *testing.T, s Storage, baseDir, modelKey string, networkTopology []byte) {
				if err := s.CreateTempNetworkTopology(networkTopology, modelKey); err != nil {
					t.Fatal(err)
				}

				if err := s.CreateNetworkTopology(modelKey); err != nil {
					t.Fatal(err)
				}
			},
			expect: func(t *testing.T, s Storage, baseDir, modelKey string, networkTopology []byte) {
				assert := assert.New(t)
				_, err := s.ListNetworkTopology(modelKey)
				assert.NoError(err)

				if err := s.CreateTempNetworkTopology(networkTopology, modelKey); err != nil {
					t.Fatal(err)
				}

				if err := s.CreateNetworkTopology(modelKey); err != nil {
					t.Fatal(err)
				}

				networkTopologies, err := s.ListNetworkTopology(modelKey)
				assert.NoError(err)
				assert.Equal(len(networkTopologies), 2)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s, err := New(tc.baseDir, DefaultStorageMaxSize, DefaultStorageMaxBackups)
			if err != nil {
				t.Fatal(err)
			}

			tc.mock(t, s, tc.baseDir, mockModelKey, testData)
			tc.expect(t, s, tc.baseDir, mockModelKey, testData)
			if err := s.ClearNetworkTopology(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestStorage_OpenDownload(t *testing.T) {
	require := require.New(t)
	testData, err := os.ReadFile("./testdata/download.csv")
	require.Nil(err, "load test file")

	tests := []struct {
		name    string
		baseDir string
		mock    func(t *testing.T, s Storage, baseDir, modelKey string, download []byte)
		expect  func(t *testing.T, s Storage, baseDir, modelKey string, download []byte)
	}{
		{
			name:    "open storage with empty download csv file and model key given",
			baseDir: os.TempDir(),
			mock: func(t *testing.T, s Storage, baseDir, modelKey string, download []byte) {
				file, err := os.OpenFile(filepath.Join(baseDir, "download-hostname_127.0.0.1_2.csv"), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
				if err != nil {
					t.Fatal(err)
				}
				file.Close()
				s.(*storage).downloadModelKey[modelKey] = struct{}{}
			},
			expect: func(t *testing.T, s Storage, baseDir, modelKey string, download []byte) {
				assert := assert.New(t)
				_, err := s.OpenDownload(modelKey)
				assert.NoError(err)
			},
		},
		{
			name:    "open file infos failed",
			baseDir: os.TempDir(),
			mock: func(t *testing.T, s Storage, baseDir, modelKey string, download []byte) {
				s.(*storage).baseDir = "bas"
			},
			expect: func(t *testing.T, s Storage, baseDir, modelKey string, download []byte) {
				assert := assert.New(t)
				_, err := s.OpenDownload(modelKey)
				assert.Error(err)
				s.(*storage).baseDir = baseDir
			},
		},
		{
			name:    "open storage with downloads of a file",
			baseDir: os.TempDir(),
			mock: func(t *testing.T, s Storage, baseDir, modelKey string, download []byte) {
				if err := s.(*storage).CreateTempDownload(download, modelKey); err != nil {
					t.Fatal(err)
				}

				if err := s.CreateDownload(modelKey); err != nil {
					t.Fatal(err)
				}
			},
			expect: func(t *testing.T, s Storage, baseDir, modelKey string, download []byte) {
				assert := assert.New(t)
				_, err := s.OpenDownload(modelKey)
				assert.NoError(err)

				if err := s.(*storage).CreateTempDownload(download, modelKey); err != nil {
					t.Fatal(err)
				}

				if err := s.CreateDownload(modelKey); err != nil {
					t.Fatal(err)
				}

				readCloser, err := s.OpenDownload(modelKey)
				assert.NoError(err)

				var downloads []Download
				err = gocsv.UnmarshalWithoutHeaders(readCloser, &downloads)
				assert.NoError(err)
				assert.Equal(len(downloads), 2)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s, err := New(tc.baseDir, DefaultStorageMaxSize, DefaultStorageMaxBackups)
			if err != nil {
				t.Fatal(err)
			}
			tc.mock(t, s, tc.baseDir, mockModelKey, testData)
			tc.expect(t, s, tc.baseDir, mockModelKey, testData)
			if err := s.ClearDownload(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestStorage_OpenNetworkTopology(t *testing.T) {
	require := require.New(t)
	testData, err := os.ReadFile("./testdata/networktopology.csv")
	require.Nil(err, "load test file")

	tests := []struct {
		name    string
		baseDir string
		mock    func(t *testing.T, s Storage, baseDir, modelKey string, networkTopology []byte)
		expect  func(t *testing.T, s Storage, baseDir, modelKey string, networkTopology []byte)
	}{
		{
			name:    "open storage with empty csv file and model key given",
			baseDir: os.TempDir(),
			mock: func(t *testing.T, s Storage, baseDir, modelKey string, networkTopology []byte) {
				file, err := os.OpenFile(filepath.Join(baseDir, "networktopology-hostname_127.0.0.1_2.csv"), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
				if err != nil {
					t.Fatal(err)
				}
				file.Close()
				s.(*storage).networkTopologyModelKey[modelKey] = struct{}{}
			},
			expect: func(t *testing.T, s Storage, baseDir, modelKey string, networkTopology []byte) {
				assert := assert.New(t)
				_, err := s.OpenNetworkTopology(modelKey)
				assert.NoError(err)
			},
		},
		{
			name:    "open file infos failed",
			baseDir: os.TempDir(),
			mock: func(t *testing.T, s Storage, baseDir, modelKey string, networkTopology []byte) {
				s.(*storage).baseDir = "bas"
			},
			expect: func(t *testing.T, s Storage, baseDir, modelKey string, networkTopology []byte) {
				assert := assert.New(t)
				_, err := s.OpenNetworkTopology(modelKey)
				assert.Error(err)
				s.(*storage).baseDir = baseDir
			},
		},
		{
			name:    "open storage with network topologies of a file",
			baseDir: os.TempDir(),
			mock: func(t *testing.T, s Storage, baseDir, modelKey string, networkTopology []byte) {
				if err := s.(*storage).CreateTempNetworkTopology(networkTopology, modelKey); err != nil {
					t.Fatal(err)
				}

				if err := s.CreateNetworkTopology(modelKey); err != nil {
					t.Fatal(err)
				}
			},
			expect: func(t *testing.T, s Storage, baseDir, modelKey string, networkTopology []byte) {
				assert := assert.New(t)
				_, err := s.OpenNetworkTopology(modelKey)
				assert.NoError(err)

				if err := s.(*storage).CreateTempNetworkTopology(networkTopology, modelKey); err != nil {
					t.Fatal(err)
				}

				if err := s.CreateNetworkTopology(modelKey); err != nil {
					t.Fatal(err)
				}

				readCloser, err := s.OpenNetworkTopology(modelKey)
				assert.NoError(err)

				var networkTopologies []NetworkTopology
				err = gocsv.UnmarshalWithoutHeaders(readCloser, &networkTopologies)
				assert.NoError(err)
				assert.Equal(len(networkTopologies), 2)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s, err := New(tc.baseDir, config.DefaultStorageMaxSize, config.DefaultStorageMaxBackups)
			if err != nil {
				t.Fatal(err)
			}

			tc.mock(t, s, tc.baseDir, mockModelKey, testData)
			tc.expect(t, s, tc.baseDir, mockModelKey, testData)
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
		mock    func(s Storage, baseDir, modelKey string)
		expect  func(t *testing.T, s Storage, baseDir, modelKey string)
	}{
		{
			name:    "clear file",
			baseDir: os.TempDir(),
			mock: func(s Storage, baseDir, modelKey string) {
				file, err := os.OpenFile(filepath.Join(baseDir, "download-hostname_127.0.0.1_2.csv"), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
				if err != nil {
					t.Fatal(err)
				}
				file.Close()
				s.(*storage).downloadModelKey[modelKey] = struct{}{}

			},
			expect: func(t *testing.T, s Storage, baseDir, modelKey string) {
				assert := assert.New(t)
				assert.NoError(s.ClearDownload())
				fileInfos, err := ioutil.ReadDir(filepath.Join(baseDir))
				assert.NoError(err)

				var backups []fs.FileInfo
				regexp := regexp.MustCompile(fmt.Sprintf("%s-%s", DownloadFilePrefix, modelKey))

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
			mock: func(s Storage, baseDir, modelKey string) {
				file, err := os.OpenFile(filepath.Join(baseDir, "download-hostname_127.0.0.1_2.csv"), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
				if err != nil {
					t.Fatal(err)
				}
				file.Close()
				s.(*storage).downloadModelKey[modelKey] = struct{}{}
				s.(*storage).baseDir = "baz"
			},
			expect: func(t *testing.T, s Storage, baseDir, modelKey string) {
				assert := assert.New(t)
				assert.Error(s.ClearDownload())

				s.(*storage).baseDir = baseDir
				assert.NoError(s.ClearDownload())
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s, err := New(tc.baseDir, DefaultStorageMaxSize, DefaultStorageMaxBackups)
			if err != nil {
				t.Fatal(err)
			}

			tc.mock(s, tc.baseDir, mockModelKey)
			tc.expect(t, s, tc.baseDir, mockModelKey)
		})
	}
}

func TestStorage_ClearNetworkTopology(t *testing.T) {
	tests := []struct {
		name    string
		baseDir string
		mock    func(s Storage, baseDir, modelKey string)
		expect  func(t *testing.T, s Storage, baseDir, modelKey string)
	}{
		{
			name:    "clear file",
			baseDir: os.TempDir(),
			mock: func(s Storage, baseDir, modelKey string) {
				file, err := os.OpenFile(filepath.Join(baseDir, "networktopology-hostname_127.0.0.1_2.csv"), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
				if err != nil {
					t.Fatal(err)
				}
				file.Close()
				s.(*storage).networkTopologyModelKey[modelKey] = struct{}{}

			},
			expect: func(t *testing.T, s Storage, baseDir, modelKey string) {
				assert := assert.New(t)
				assert.NoError(s.ClearNetworkTopology())
				fileInfos, err := ioutil.ReadDir(filepath.Join(baseDir))
				assert.NoError(err)

				var backups []fs.FileInfo
				regexp := regexp.MustCompile(fmt.Sprintf("%s-%s", NetworkTopologyFilePrefix, modelKey))
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
			mock: func(s Storage, baseDir, modelKey string) {
				file, err := os.OpenFile(filepath.Join(baseDir, "networktopology-hostname_127.0.0.1_2.csv"), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
				if err != nil {
					t.Fatal(err)
				}
				file.Close()
				s.(*storage).networkTopologyModelKey[modelKey] = struct{}{}
				s.(*storage).baseDir = "baz"
			},
			expect: func(t *testing.T, s Storage, baseDir, modelKey string) {
				assert := assert.New(t)
				assert.Error(s.ClearNetworkTopology())

				s.(*storage).baseDir = baseDir
				assert.NoError(s.ClearNetworkTopology())
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s, err := New(tc.baseDir, DefaultStorageMaxSize, DefaultStorageMaxBackups)
			if err != nil {
				t.Fatal(err)
			}

			tc.mock(s, tc.baseDir, mockModelKey)
			tc.expect(t, s, tc.baseDir, mockModelKey)
		})
	}
}

func TestStorage_openDownloadFile(t *testing.T) {
	require := require.New(t)
	testData, err := os.ReadFile("./testdata/download.csv")
	require.Nil(err, "load test file")

	tests := []struct {
		name       string
		baseDir    string
		maxSize    int
		maxBackups int
		mock       func(t *testing.T, s Storage, baseDir, modelKey string, download []byte)
		expect     func(t *testing.T, s Storage, baseDir, modelKey string)
	}{
		{
			name:       "open file failed",
			baseDir:    os.TempDir(),
			maxSize:    DefaultStorageMaxSize,
			maxBackups: DefaultStorageMaxBackups,
			mock: func(t *testing.T, s Storage, baseDir, modelKey string, download []byte) {
				file, err := os.OpenFile(filepath.Join(baseDir, "download-hostname_127.0.0.1_2.csv"), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
				if err != nil {
					t.Fatal(err)
				}
				file.Close()
				s.(*storage).baseDir = "bat"

			},
			expect: func(t *testing.T, s Storage, baseDir, modelKey string) {
				assert := assert.New(t)
				_, err := s.(*storage).openDownloadFile(modelKey)
				assert.Error(err)
				s.(*storage).baseDir = baseDir
			},
		},
		{
			name:       "open new download file",
			baseDir:    os.TempDir(),
			maxSize:    0,
			maxBackups: DefaultStorageMaxBackups,
			mock: func(t *testing.T, s Storage, baseDir, modelKey string, download []byte) {
				if err := s.CreateTempDownload(download, modelKey); err != nil {
					t.Fatal(err)
				}

				if err := s.CreateDownload(modelKey); err != nil {
					t.Fatal(err)
				}
				if err := s.CreateTempDownload(download, modelKey); err != nil {
					t.Fatal(err)
				}

				if err := s.CreateDownload(modelKey); err != nil {
					t.Fatal(err)
				}
			},
			expect: func(t *testing.T, s Storage, baseDir, modelKey string) {
				assert := assert.New(t)
				file, err := s.(*storage).openDownloadFile(modelKey)
				assert.NoError(err)
				assert.Equal(file.Name(), filepath.Join(baseDir, fmt.Sprintf("%s-%s.%s", DownloadFilePrefix, modelKey, CSVFileExt)))
				file.Close()
			},
		},
		{
			name:       "remove download file",
			baseDir:    os.TempDir(),
			maxSize:    0,
			maxBackups: 1,
			mock: func(t *testing.T, s Storage, baseDir, modelKey string, download []byte) {
				if err := s.CreateTempDownload(download, modelKey); err != nil {
					t.Fatal(err)
				}

				if err := s.CreateDownload(modelKey); err != nil {
					t.Fatal(err)
				}
				if err := s.CreateTempDownload(download, modelKey); err != nil {
					t.Fatal(err)
				}

				if err := s.CreateDownload(modelKey); err != nil {
					t.Fatal(err)
				}
			},
			expect: func(t *testing.T, s Storage, baseDir, modelKey string) {
				assert := assert.New(t)
				file, err := s.(*storage).openDownloadFile(modelKey)
				assert.NoError(err)
				assert.Equal(file.Name(), filepath.Join(baseDir, fmt.Sprintf("%s-%s.%s", DownloadFilePrefix, modelKey, CSVFileExt)))
				file.Close()
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s, err := New(tc.baseDir, tc.maxSize, tc.maxBackups)
			if err != nil {
				t.Fatal(err)
			}

			tc.mock(t, s, tc.baseDir, mockModelKey, testData)
			tc.expect(t, s, tc.baseDir, mockModelKey)
			if err := s.ClearDownload(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestStorage_openNetworkTopologyFile(t *testing.T) {
	require := require.New(t)
	testData, err := os.ReadFile("./testdata/networktopology.csv")
	require.Nil(err, "load test file")

	tests := []struct {
		name       string
		baseDir    string
		maxSize    int
		maxBackups int
		mock       func(t *testing.T, s Storage, baseDir, modelKey string, networkTopology []byte)
		expect     func(t *testing.T, s Storage, baseDir, modelKey string)
	}{
		{
			name:       "open file failed",
			baseDir:    os.TempDir(),
			maxSize:    DefaultStorageMaxSize,
			maxBackups: DefaultStorageMaxBackups,
			mock: func(t *testing.T, s Storage, baseDir, modelKey string, networkTopology []byte) {
				file, err := os.OpenFile(filepath.Join(baseDir, "networktopology-hostname_127.0.0.1_2.csv"), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
				if err != nil {
					t.Fatal(err)
				}
				file.Close()
				s.(*storage).baseDir = "bat"

			},
			expect: func(t *testing.T, s Storage, baseDir, modelKey string) {
				assert := assert.New(t)
				_, err := s.(*storage).openNetworkTopologyFile(modelKey)
				assert.Error(err)
				s.(*storage).baseDir = baseDir
			},
		},
		{
			name:       "open new network topology file",
			baseDir:    os.TempDir(),
			maxSize:    0,
			maxBackups: DefaultStorageMaxBackups,
			mock: func(t *testing.T, s Storage, baseDir, modelKey string, networkTopology []byte) {
				if err := s.CreateTempNetworkTopology(networkTopology, modelKey); err != nil {
					t.Fatal(err)
				}

				if err := s.CreateNetworkTopology(modelKey); err != nil {
					t.Fatal(err)
				}
				if err := s.CreateTempNetworkTopology(networkTopology, modelKey); err != nil {
					t.Fatal(err)
				}

				if err := s.CreateNetworkTopology(modelKey); err != nil {
					t.Fatal(err)
				}
			},
			expect: func(t *testing.T, s Storage, baseDir, modelKey string) {
				assert := assert.New(t)
				file, err := s.(*storage).openNetworkTopologyFile(modelKey)
				assert.NoError(err)
				assert.Equal(file.Name(), filepath.Join(baseDir, fmt.Sprintf("%s-%s.%s", NetworkTopologyFilePrefix, modelKey, CSVFileExt)))
				file.Close()
			},
		},
		{
			name:       "remove network topology file",
			baseDir:    os.TempDir(),
			maxSize:    0,
			maxBackups: 1,
			mock: func(t *testing.T, s Storage, baseDir, modelKey string, networkTopology []byte) {
				if err := s.CreateTempNetworkTopology(networkTopology, modelKey); err != nil {
					t.Fatal(err)
				}

				if err := s.CreateNetworkTopology(modelKey); err != nil {
					t.Fatal(err)
				}
				if err := s.CreateTempNetworkTopology(networkTopology, modelKey); err != nil {
					t.Fatal(err)
				}

				if err := s.CreateNetworkTopology(modelKey); err != nil {
					t.Fatal(err)
				}
			},
			expect: func(t *testing.T, s Storage, baseDir, modelKey string) {
				assert := assert.New(t)
				file, err := s.(*storage).openNetworkTopologyFile(modelKey)
				assert.NoError(err)
				assert.Equal(file.Name(), filepath.Join(baseDir, fmt.Sprintf("%s-%s.%s", NetworkTopologyFilePrefix, modelKey, CSVFileExt)))
				file.Close()
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s, err := New(tc.baseDir, tc.maxSize, tc.maxBackups)
			if err != nil {
				t.Fatal(err)
			}

			tc.mock(t, s, tc.baseDir, mockModelKey, testData)
			tc.expect(t, s, tc.baseDir, mockModelKey)
			if err := s.ClearNetworkTopology(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestStorage_downloadBackups(t *testing.T) {
	tests := []struct {
		name    string
		baseDir string
		mock    func(t *testing.T, s Storage, baseDir, modelKey string)
		expect  func(t *testing.T, s Storage, baseDir, modelKey string)
	}{
		{
			name:    "open file failed",
			baseDir: os.TempDir(),
			mock: func(t *testing.T, s Storage, baseDir, modelKey string) {
				file, err := os.OpenFile(filepath.Join(baseDir, "download-hostname_127.0.0.1_2.csv"), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
				if err != nil {
					t.Fatal(err)
				}
				file.Close()

				s.(*storage).baseDir = "bar"
				s.(*storage).downloadModelKey[modelKey] = struct{}{}
			},
			expect: func(t *testing.T, s Storage, baseDir, modelKey string) {
				assert := assert.New(t)
				_, err := s.(*storage).downloadBackups(modelKey)
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
			mock: func(t *testing.T, s Storage, baseDir, modelKey string) {
				file, err := os.OpenFile(filepath.Join(baseDir, "download-hostname_127.0.0.1_2.csv"), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
				if err != nil {
					t.Fatal(err)
				}
				file.Close()

				s.(*storage).downloadModelKey[modelKey] = struct{}{}
			},
			expect: func(t *testing.T, s Storage, baseDir, modelKey string) {
				assert := assert.New(t)
				if err := s.ClearDownload(); err != nil {
					t.Fatal(err)
				}

				_, err := s.(*storage).downloadBackups(modelKey)
				assert.Error(err)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s, err := New(tc.baseDir, DefaultStorageMaxSize, DefaultStorageMaxBackups)
			if err != nil {
				t.Fatal(err)
			}

			tc.mock(t, s, tc.baseDir, mockModelKey)
			tc.expect(t, s, tc.baseDir, mockModelKey)
		})
	}
}

func TestStorage_networkTopologyBackups(t *testing.T) {
	tests := []struct {
		name    string
		baseDir string
		mock    func(t *testing.T, s Storage, baseDir, modelKey string)
		expect  func(t *testing.T, s Storage, baseDir, modelKey string)
	}{
		{
			name:    "open file failed",
			baseDir: os.TempDir(),
			mock: func(t *testing.T, s Storage, baseDir, modelKey string) {
				file, err := os.OpenFile(filepath.Join(baseDir, "networktopology-hostname_127.0.0.1_2.csv"), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
				if err != nil {
					t.Fatal(err)
				}
				file.Close()

				s.(*storage).baseDir = "bar"
				s.(*storage).downloadModelKey[modelKey] = struct{}{}
			},
			expect: func(t *testing.T, s Storage, baseDir, modelKey string) {
				assert := assert.New(t)
				_, err := s.(*storage).networkTopologyBackups(modelKey)
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
			mock: func(t *testing.T, s Storage, baseDir, modelKey string) {
				file, err := os.OpenFile(filepath.Join(baseDir, "networktopology-hostname_127.0.0.1_2.csv"), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
				if err != nil {
					t.Fatal(err)
				}
				file.Close()

				s.(*storage).networkTopologyModelKey[modelKey] = struct{}{}
			},
			expect: func(t *testing.T, s Storage, baseDir, modelKey string) {
				assert := assert.New(t)
				if err := s.ClearNetworkTopology(); err != nil {
					t.Fatal(err)
				}

				_, err := s.(*storage).networkTopologyBackups(modelKey)
				assert.Error(err)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s, err := New(tc.baseDir, DefaultStorageMaxSize, DefaultStorageMaxBackups)
			if err != nil {
				t.Fatal(err)
			}

			tc.mock(t, s, tc.baseDir, mockModelKey)
			tc.expect(t, s, tc.baseDir, mockModelKey)
		})
	}
}

func TestStorage_downloadTempFilename(t *testing.T) {
	baseDir := os.TempDir()
	s, err := New(baseDir, DefaultStorageMaxSize, DefaultStorageMaxBackups)
	if err != nil {
		t.Fatal(err)
	}

	filename := s.(*storage).downloadTempFilename(mockModelKey)
	regexp := regexp.MustCompile(fmt.Sprintf("%s-%s-%s.*.%s$", DownloadFilePrefix, TempFileInfix, mockModelKey, CSVFileExt))
	assert := assert.New(t)
	assert.True(regexp.MatchString(filename))
}

func TestStorage_downloadFilename(t *testing.T) {
	baseDir := os.TempDir()
	s, err := New(baseDir, DefaultStorageMaxSize, DefaultStorageMaxBackups)
	if err != nil {
		t.Fatal(err)
	}

	filename := s.(*storage).downloadFilename(mockModelKey)
	regexp := regexp.MustCompile(fmt.Sprintf("%s-%s.*.%s$", DownloadFilePrefix, mockModelKey, CSVFileExt))
	assert := assert.New(t)
	assert.True(regexp.MatchString(filename))
}

func TestStorage_downloadBackupFilename(t *testing.T) {
	baseDir := os.TempDir()
	s, err := New(baseDir, DefaultStorageMaxSize, DefaultStorageMaxBackups)
	if err != nil {
		t.Fatal(err)
	}

	filename := s.(*storage).downloadBackupFilename(mockModelKey)
	regexp := regexp.MustCompile(fmt.Sprintf("%s-%s.*.%s$", DownloadFilePrefix, mockModelKey, CSVFileExt))
	assert := assert.New(t)
	assert.True(regexp.MatchString(filename))
}

func TestStorage_networkTopologyTempFilename(t *testing.T) {
	baseDir := os.TempDir()
	s, err := New(baseDir, DefaultStorageMaxSize, DefaultStorageMaxBackups)
	if err != nil {
		t.Fatal(err)
	}

	filename := s.(*storage).networkTopologyTempFilename(mockModelKey)
	regexp := regexp.MustCompile(fmt.Sprintf("%s-%s-%s.*.%s$", NetworkTopologyFilePrefix, TempFileInfix, mockModelKey, CSVFileExt))
	assert := assert.New(t)
	assert.True(regexp.MatchString(filename))
}

func TestStorage_networkTopologyFilename(t *testing.T) {
	baseDir := os.TempDir()
	s, err := New(baseDir, DefaultStorageMaxSize, DefaultStorageMaxBackups)
	if err != nil {
		t.Fatal(err)
	}

	filename := s.(*storage).networkTopologyFilename(mockModelKey)
	regexp := regexp.MustCompile(fmt.Sprintf("%s-%s.*.%s$", NetworkTopologyFilePrefix, mockModelKey, CSVFileExt))
	assert := assert.New(t)
	assert.True(regexp.MatchString(filename))
}

func TestStorage_networkTopologyBackupFilename(t *testing.T) {
	baseDir := os.TempDir()
	s, err := New(baseDir, DefaultStorageMaxSize, DefaultStorageMaxBackups)
	if err != nil {
		t.Fatal(err)
	}

	filename := s.(*storage).networkTopologyBackupFilename(mockModelKey)
	regexp := regexp.MustCompile(fmt.Sprintf("%s-%s.*.%s$", NetworkTopologyFilePrefix, mockModelKey, CSVFileExt))
	assert := assert.New(t)
	assert.True(regexp.MatchString(filename))
}
