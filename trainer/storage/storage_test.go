/*
 *     Copyright 2023 The Dragonfly Authors
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

	schedulerstorage "d7y.io/dragonfly/v2/scheduler/storage"
	"github.com/gocarina/gocsv"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	mockModelKey = "hostname_127.0.0.1_2"
)

func TestStorage_New(t *testing.T) {
	tests := []struct {
		name    string
		baseDir string
		expect  func(t *testing.T, s Storage)
	}{
		{
			name:    "new storage",
			baseDir: os.TempDir(),
			expect: func(t *testing.T, s Storage) {
				assert := assert.New(t)
				assert.Equal(reflect.TypeOf(s).Elem().Name(), "storage")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := New(tc.baseDir)
			tc.expect(t, s)
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
				file, err := os.OpenFile(filepath.Join(baseDir, "download-hostname_127.0.0.1_2.csv"), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
				if err != nil {
					t.Fatal(err)
				}
				file.Write(download)
				file.Close()
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
			name:    "list downloads of a file",
			baseDir: os.TempDir(),
			mock: func(t *testing.T, s Storage, baseDir, modelKey string, download []byte) {
				file, err := os.OpenFile(filepath.Join(baseDir, "download-hostname_127.0.0.1_2.csv"), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
				if err != nil {
					t.Fatal(err)
				}
				file.Write(download)
				file.Close()
			},
			expect: func(t *testing.T, s Storage, baseDir, modelKey string, download []byte) {
				assert := assert.New(t)
				list, err := s.ListDownload(modelKey)
				assert.NoError(err)
				assert.Equal(len(list), 1)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := New(tc.baseDir)
			tc.mock(t, s, tc.baseDir, mockModelKey, testData)
			tc.expect(t, s, tc.baseDir, mockModelKey, testData)
			if err := s.ClearDownload(mockModelKey); err != nil {
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
				file, err := os.OpenFile(filepath.Join(baseDir, "networktopology-hostname_127.0.0.1_2.csv"), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
				if err != nil {
					t.Fatal(err)
				}
				file.Write(networkTopology)
				file.Close()
				s.(*storage).baseDir = "foo"
			},
			expect: func(t *testing.T, s Storage, baseDir, modelKey string, networkTopology []byte) {
				assert := assert.New(t)
				_, err := s.ListNetworkTopology(modelKey)
				assert.Error(err)
				s.(*storage).baseDir = baseDir
			},
		},
		{
			name:    "list network topologies of a file",
			baseDir: os.TempDir(),
			mock: func(t *testing.T, s Storage, baseDir, modelKey string, networkTopology []byte) {
				file, err := os.OpenFile(filepath.Join(baseDir, "networktopology-hostname_127.0.0.1_2.csv"), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
				if err != nil {
					t.Fatal(err)
				}
				file.Write(networkTopology)
				file.Close()
			},
			expect: func(t *testing.T, s Storage, baseDir, modelKey string, networkTopology []byte) {
				assert := assert.New(t)
				list, err := s.ListNetworkTopology(modelKey)
				assert.NoError(err)
				assert.Equal(len(list), 1)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := New(tc.baseDir)
			tc.mock(t, s, tc.baseDir, mockModelKey, testData)
			tc.expect(t, s, tc.baseDir, mockModelKey, testData)
			if err := s.ClearNetworkTopology(mockModelKey); err != nil {
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
			name:    "open file infos failed",
			baseDir: os.TempDir(),
			mock: func(t *testing.T, s Storage, baseDir, modelKey string, download []byte) {
				file, err := os.OpenFile(filepath.Join(baseDir, "download-hostname_127.0.0.1_2.csv"), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
				if err != nil {
					t.Fatal(err)
				}
				file.Close()
				s.(*storage).baseDir = "baw"
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
				file, err := os.OpenFile(filepath.Join(baseDir, "download-hostname_127.0.0.1_2.csv"), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
				if err != nil {
					t.Fatal(err)
				}
				file.Write(download)
				file.Close()
			},
			expect: func(t *testing.T, s Storage, baseDir, modelKey string, download []byte) {
				assert := assert.New(t)
				readCloser, err := s.OpenDownload(modelKey)
				assert.NoError(err)

				var downloads []schedulerstorage.Download
				assert.NoError(gocsv.UnmarshalWithoutHeaders(readCloser, &downloads))
				assert.Equal(len(downloads), 1)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := New(tc.baseDir)
			tc.mock(t, s, tc.baseDir, mockModelKey, testData)
			tc.expect(t, s, tc.baseDir, mockModelKey, testData)
			if err := s.ClearDownload(mockModelKey); err != nil {
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
			name:    "open file infos failed",
			baseDir: os.TempDir(),
			mock: func(t *testing.T, s Storage, baseDir, modelKey string, networkTopology []byte) {
				file, err := os.OpenFile(filepath.Join(baseDir, "networktopology-hostname_127.0.0.1_2.csv"), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
				if err != nil {
					t.Fatal(err)
				}
				file.Close()
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
				file, err := os.OpenFile(filepath.Join(baseDir, "networktopology-hostname_127.0.0.1_2.csv"), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
				if err != nil {
					t.Fatal(err)
				}
				file.Write(networkTopology)
				file.Close()
			},
			expect: func(t *testing.T, s Storage, baseDir, modelKey string, networkTopology []byte) {
				assert := assert.New(t)
				readCloser, err := s.OpenNetworkTopology(modelKey)
				assert.NoError(err)

				var networkTopologies []schedulerstorage.NetworkTopology
				assert.NoError(gocsv.UnmarshalWithoutHeaders(readCloser, &networkTopologies))
				assert.Equal(len(networkTopologies), 1)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := New(tc.baseDir)
			tc.mock(t, s, tc.baseDir, mockModelKey, testData)
			tc.expect(t, s, tc.baseDir, mockModelKey, testData)
			if err := s.ClearNetworkTopology(mockModelKey); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestStorage_ClearDownload(t *testing.T) {
	tests := []struct {
		name    string
		baseDir string
		mock    func(t *testing.T, s Storage, baseDir, modelKey string)
		expect  func(t *testing.T, s Storage, baseDir, modelKey string)
	}{
		{
			name:    "clear file",
			baseDir: os.TempDir(),
			mock: func(t *testing.T, s Storage, baseDir, modelKey string) {
				file, err := os.OpenFile(filepath.Join(baseDir, "download-hostname_127.0.0.1_2.csv"), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
				if err != nil {
					t.Fatal(err)
				}
				file.Close()
			},
			expect: func(t *testing.T, s Storage, baseDir, modelKey string) {
				assert := assert.New(t)
				assert.NoError(s.ClearDownload(modelKey))
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
			mock: func(t *testing.T, s Storage, baseDir, modelKey string) {
				file, err := os.OpenFile(filepath.Join(baseDir, "download-hostname_127.0.0.1_2.csv"), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
				if err != nil {
					t.Fatal(err)
				}
				file.Close()
				s.(*storage).baseDir = "baz"
			},
			expect: func(t *testing.T, s Storage, baseDir, modelKey string) {
				assert := assert.New(t)
				assert.Error(s.ClearDownload(modelKey))

				s.(*storage).baseDir = baseDir
				assert.NoError(s.ClearDownload(modelKey))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := New(tc.baseDir)
			tc.mock(t, s, tc.baseDir, mockModelKey)
			tc.expect(t, s, tc.baseDir, mockModelKey)
		})
	}
}

func TestStorage_ClearNetworkTopology(t *testing.T) {
	tests := []struct {
		name    string
		baseDir string
		mock    func(t *testing.T, s Storage, baseDir, modelKey string)
		expect  func(t *testing.T, s Storage, baseDir, modelKey string)
	}{
		{
			name:    "clear file",
			baseDir: os.TempDir(),
			mock: func(t *testing.T, s Storage, baseDir, modelKey string) {
				file, err := os.OpenFile(filepath.Join(baseDir, "networktopology-hostname_127.0.0.1_2.csv"), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
				if err != nil {
					t.Fatal(err)
				}
				file.Close()
			},
			expect: func(t *testing.T, s Storage, baseDir, modelKey string) {
				assert := assert.New(t)
				assert.NoError(s.ClearNetworkTopology(modelKey))
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
			mock: func(t *testing.T, s Storage, baseDir, modelKey string) {
				file, err := os.OpenFile(filepath.Join(baseDir, "networktopology-hostname_127.0.0.1_2.csv"), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
				if err != nil {
					t.Fatal(err)
				}
				file.Close()
				s.(*storage).baseDir = "baz"
			},
			expect: func(t *testing.T, s Storage, baseDir, modelKey string) {
				assert := assert.New(t)
				assert.Error(s.ClearNetworkTopology(modelKey))

				s.(*storage).baseDir = baseDir
				assert.NoError(s.ClearNetworkTopology(modelKey))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := New(tc.baseDir)
			tc.mock(t, s, tc.baseDir, mockModelKey)
			tc.expect(t, s, tc.baseDir, mockModelKey)
		})
	}
}

func TestStorage_Clear(t *testing.T) {
	tests := []struct {
		name    string
		baseDir string
		mock    func(t *testing.T, s Storage, baseDir string)
		expect  func(t *testing.T, s Storage, baseDir string)
	}{
		{
			name:    "clear file",
			baseDir: os.TempDir(),
			mock: func(t *testing.T, s Storage, baseDir string) {
				downloadFile, err := os.OpenFile(filepath.Join(baseDir, "download-hostname_127.0.0.1_2.csv"), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
				if err != nil {
					t.Fatal(err)
				}
				downloadFile.Close()

				networkTopologyFile, err := os.OpenFile(filepath.Join(baseDir, "networktopology-hostname_127.0.0.1_2.csv"), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
				if err != nil {
					t.Fatal(err)
				}
				networkTopologyFile.Close()
			},
			expect: func(t *testing.T, s Storage, baseDir string) {
				assert := assert.New(t)
				assert.NoError(s.Clear())
				fileInfos, err := ioutil.ReadDir(filepath.Join(baseDir))
				assert.NoError(err)
				assert.Equal(len(fileInfos), 0)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := New(tc.baseDir)
			tc.mock(t, s, tc.baseDir)
			tc.expect(t, s, tc.baseDir)
		})
	}
}

func TestStorage_downloadFilename(t *testing.T) {
	baseDir := os.TempDir()
	s := New(baseDir)

	filename := s.(*storage).downloadFilename(mockModelKey)
	regexp := regexp.MustCompile(fmt.Sprintf("%s-%s.*.%s$", DownloadFilePrefix, mockModelKey, CSVFileExt))
	assert := assert.New(t)
	assert.True(regexp.MatchString(filename))
}

func TestStorage_networkTopologyFilename(t *testing.T) {
	baseDir := os.TempDir()
	s := New(baseDir)

	filename := s.(*storage).networkTopologyFilename(mockModelKey)
	regexp := regexp.MustCompile(fmt.Sprintf("%s-%s.*.%s$", NetworkTopologyFilePrefix, mockModelKey, CSVFileExt))
	assert := assert.New(t)
	assert.True(regexp.MatchString(filename))
}
