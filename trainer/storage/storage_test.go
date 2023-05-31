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
				assert.Equal(len(s.(*storage).downloadModelKeys.Values()), 0)
				assert.Equal(len(s.(*storage).networkTopologyModelKeys.Values()), 0)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s, error := New(tc.baseDir)
			tc.expect(t, s, error)
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
		mock    func(s Storage, baseDir, modelKey string)
		expect  func(t *testing.T, s Storage, baseDir, modelKey string, download []byte)
	}{
		{
			name:    "create download",
			baseDir: os.TempDir(),
			mock:    func(s Storage, baseDir, modelKey string) {},
			expect: func(t *testing.T, s Storage, baseDir, modelKey string, download []byte) {
				assert := assert.New(t)
				assert.Equal(len(s.(*storage).downloadModelKeys.Values()), 0)
				assert.NoError(s.CreateDownload(download, modelKey))
				assert.Equal(len(s.(*storage).downloadModelKeys.Values()), 1)
			},
		},
		{
			name:    "open file failed",
			baseDir: os.TempDir(),
			mock: func(s Storage, baseDir, modelKey string) {
				s.(*storage).baseDir = "bae"
			},
			expect: func(t *testing.T, s Storage, baseDir, modelKey string, download []byte) {
				assert := assert.New(t)
				assert.Error(s.CreateDownload(download, modelKey))
				s.(*storage).baseDir = baseDir
				assert.NoError(s.CreateDownload(download, modelKey))
				assert.Equal(len(s.(*storage).downloadModelKeys.Values()), 1)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s, err := New(tc.baseDir)
			if err != nil {
				t.Fatal(err)
			}

			tc.mock(s, tc.baseDir, mockModelKey)
			tc.expect(t, s, tc.baseDir, mockModelKey, testData)
			if err := s.ClearDownload(mockModelKey); err != nil {
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
		mock    func(s Storage, baseDir, modelKey string)
		expect  func(t *testing.T, s Storage, baseDir, modelKey string, networkTopology []byte)
	}{
		{
			name:    "create networkTopology",
			baseDir: os.TempDir(),
			mock:    func(s Storage, baseDir, modelKey string) {},
			expect: func(t *testing.T, s Storage, baseDir, modelKey string, networkTopology []byte) {
				assert := assert.New(t)
				assert.Equal(len(s.(*storage).networkTopologyModelKeys.Values()), 0)
				assert.NoError(s.CreateNetworkTopology(networkTopology, modelKey))
				assert.Equal(len(s.(*storage).networkTopologyModelKeys.Values()), 1)
			},
		},
		{
			name:    "open file failed",
			baseDir: os.TempDir(),
			mock: func(s Storage, baseDir, modelKey string) {
				s.(*storage).baseDir = "bae"
			},
			expect: func(t *testing.T, s Storage, baseDir, modelKey string, networkTopology []byte) {
				assert := assert.New(t)
				assert.Error(s.CreateNetworkTopology(networkTopology, modelKey))
				s.(*storage).baseDir = baseDir
				assert.NoError(s.CreateNetworkTopology(networkTopology, modelKey))
				assert.Equal(len(s.(*storage).networkTopologyModelKeys.Values()), 1)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s, err := New(tc.baseDir)
			if err != nil {
				t.Fatal(err)
			}

			tc.mock(s, tc.baseDir, mockModelKey)
			tc.expect(t, s, tc.baseDir, mockModelKey, testData)
			if err := s.ClearNetworkTopology(mockModelKey); err != nil {
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
				if err := s.CreateDownload(download, modelKey); err != nil {
					t.Fatal(err)
				}
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
				if err := s.CreateDownload(download, modelKey); err != nil {
					t.Fatal(err)
				}
			},
			expect: func(t *testing.T, s Storage, baseDir, modelKey string, download []byte) {
				assert := assert.New(t)
				_, err := s.ListDownload(modelKey)
				assert.NoError(err)

				if err := s.CreateDownload(download, modelKey); err != nil {
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
			s, err := New(tc.baseDir)
			if err != nil {
				t.Fatal(err)
			}
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
				if err := s.CreateNetworkTopology(networkTopology, modelKey); err != nil {
					t.Fatal(err)
				}
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
				if err := s.CreateNetworkTopology(networkTopology, modelKey); err != nil {
					t.Fatal(err)
				}
			},
			expect: func(t *testing.T, s Storage, baseDir, modelKey string, networkTopology []byte) {
				assert := assert.New(t)
				_, err := s.ListNetworkTopology(modelKey)
				assert.NoError(err)

				if err := s.CreateNetworkTopology(networkTopology, modelKey); err != nil {
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
			s, err := New(tc.baseDir)
			if err != nil {
				t.Fatal(err)
			}

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
			name:    "open storage with empty download csv file and model key given",
			baseDir: os.TempDir(),
			mock: func(t *testing.T, s Storage, baseDir, modelKey string, download []byte) {
				file, err := os.OpenFile(filepath.Join(baseDir, "download-hostname_127.0.0.1_2.csv"), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
				if err != nil {
					t.Fatal(err)
				}
				file.Close()
			},
			expect: func(t *testing.T, s Storage, baseDir, modelKey string, downloads []byte) {
				assert := assert.New(t)
				_, err := s.OpenDownload(modelKey)
				assert.NoError(err)
			},
		},
		{
			name:    "open file infos failed",
			baseDir: os.TempDir(),
			mock: func(t *testing.T, s Storage, baseDir, modelKey string, download []byte) {
				if err := s.CreateDownload(download, modelKey); err != nil {
					t.Fatal(err)
				}
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
				if err := s.CreateDownload(download, modelKey); err != nil {
					t.Fatal(err)
				}
			},
			expect: func(t *testing.T, s Storage, baseDir, modelKey string, download []byte) {
				assert := assert.New(t)
				_, err := s.OpenDownload(modelKey)
				assert.NoError(err)

				if err := s.CreateDownload(download, modelKey); err != nil {
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
			s, err := New(tc.baseDir)
			if err != nil {
				t.Fatal(err)
			}
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
			name:    "open storage with empty csv file and model key given",
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
				_, err := s.OpenNetworkTopology(modelKey)
				assert.NoError(err)
			},
		},
		{
			name:    "open file infos failed",
			baseDir: os.TempDir(),
			mock: func(t *testing.T, s Storage, baseDir, modelKey string, networkTopology []byte) {
				if err := s.CreateNetworkTopology(networkTopology, modelKey); err != nil {
					t.Fatal(err)
				}
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
				if err := s.CreateNetworkTopology(networkTopology, modelKey); err != nil {
					t.Fatal(err)
				}
			},
			expect: func(t *testing.T, s Storage, baseDir, modelKey string, networkTopology []byte) {
				assert := assert.New(t)
				_, err := s.OpenNetworkTopology(modelKey)
				assert.NoError(err)

				if err := s.CreateNetworkTopology(networkTopology, modelKey); err != nil {
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
			s, err := New(tc.baseDir)
			if err != nil {
				t.Fatal(err)
			}

			tc.mock(t, s, tc.baseDir, mockModelKey, testData)
			tc.expect(t, s, tc.baseDir, mockModelKey, testData)
			if err := s.ClearNetworkTopology(mockModelKey); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestStorage_ClearDownload(t *testing.T) {
	require := require.New(t)
	testData, err := os.ReadFile("./testdata/download.csv")
	require.Nil(err, "load test file")

	tests := []struct {
		name    string
		baseDir string
		mock    func(t *testing.T, s Storage, baseDir, modelKey string, download []byte)
		expect  func(t *testing.T, s Storage, baseDir, modelKey string)
	}{
		{
			name:    "clear file",
			baseDir: os.TempDir(),
			mock: func(t *testing.T, s Storage, baseDir, modelKey string, download []byte) {
				if err := s.CreateDownload(download, modelKey); err != nil {
					t.Fatal(err)
				}
			},
			expect: func(t *testing.T, s Storage, baseDir, modelKey string) {
				assert := assert.New(t)
				assert.Equal(len(s.(*storage).downloadModelKeys.Values()), 1)
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
				assert.Equal(len(s.(*storage).downloadModelKeys.Values()), 0)
			},
		},
		{
			name:    "open file failed",
			baseDir: os.TempDir(),
			mock: func(t *testing.T, s Storage, baseDir, modelKey string, downloads []byte) {
				if err := s.CreateDownload(downloads, modelKey); err != nil {
					t.Fatal(err)
				}
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
			s, err := New(tc.baseDir)
			if err != nil {
				t.Fatal(err)
			}

			tc.mock(t, s, tc.baseDir, mockModelKey, testData)
			tc.expect(t, s, tc.baseDir, mockModelKey)
		})
	}
}

func TestStorage_ClearNetworkTopology(t *testing.T) {
	require := require.New(t)
	testData, err := os.ReadFile("./testdata/networktopology.csv")
	require.Nil(err, "load test file")

	tests := []struct {
		name    string
		baseDir string
		mock    func(t *testing.T, s Storage, baseDir, modelKey string, networkTopology []byte)
		expect  func(t *testing.T, s Storage, baseDir, modelKey string)
	}{
		{
			name:    "clear file",
			baseDir: os.TempDir(),
			mock: func(t *testing.T, s Storage, baseDir, modelKey string, networkTopology []byte) {
				if err := s.CreateNetworkTopology(networkTopology, modelKey); err != nil {
					t.Fatal(err)
				}
			},
			expect: func(t *testing.T, s Storage, baseDir, modelKey string) {
				assert := assert.New(t)
				assert.Equal(len(s.(*storage).networkTopologyModelKeys.Values()), 1)
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
				assert.Equal(len(s.(*storage).networkTopologyModelKeys.Values()), 0)
			},
		},
		{
			name:    "open file failed",
			baseDir: os.TempDir(),
			mock: func(t *testing.T, s Storage, baseDir, modelKey string, networkTopology []byte) {
				if err := s.CreateNetworkTopology(networkTopology, modelKey); err != nil {
					t.Fatal(err)
				}
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
			s, err := New(tc.baseDir)
			if err != nil {
				t.Fatal(err)
			}

			tc.mock(t, s, tc.baseDir, mockModelKey, testData)
			tc.expect(t, s, tc.baseDir, mockModelKey)
		})
	}
}

func TestStorage_Clear(t *testing.T) {
	require := require.New(t)
	testDownloadData, err := os.ReadFile("./testdata/download.csv")
	require.Nil(err, "load download test file")
	testNetworkTopologyData, err := os.ReadFile("./testdata/networktopology.csv")
	require.Nil(err, "load networktopology test file")
	tests := []struct {
		name    string
		baseDir string
		mock    func(t *testing.T, s Storage, baseDir, modelKey string, download, networkTopology []byte)
		expect  func(t *testing.T, s Storage, baseDir, modelKey string)
	}{
		{
			name:    "clear file",
			baseDir: os.TempDir(),
			mock: func(t *testing.T, s Storage, baseDir, modelKey string, download, networkTopology []byte) {
				if err := s.CreateDownload(download, modelKey); err != nil {
					t.Fatal(err)
				}

				if err := s.CreateNetworkTopology(networkTopology, modelKey); err != nil {
					t.Fatal(err)
				}
			},
			expect: func(t *testing.T, s Storage, baseDir, modelKey string) {
				assert := assert.New(t)
				assert.Equal(len(s.(*storage).downloadModelKeys.Values()), 1)
				assert.Equal(len(s.(*storage).networkTopologyModelKeys.Values()), 1)
				assert.NoError(s.Clear())
				fileInfos, err := ioutil.ReadDir(filepath.Join(baseDir))
				assert.NoError(err)

				var downloadFiles []fs.FileInfo
				downloadRegexp := regexp.MustCompile(fmt.Sprintf("%s-%s", DownloadFilePrefix, modelKey))
				for _, fileInfo := range fileInfos {
					if !fileInfo.IsDir() && downloadRegexp.MatchString(fileInfo.Name()) {
						downloadFiles = append(downloadFiles, fileInfo)
					}
				}
				assert.Equal(len(downloadFiles), 0)
				assert.Equal(len(s.(*storage).downloadModelKeys.Values()), 0)

				var networkTopologyFiles []fs.FileInfo
				networkTopologyRegexp := regexp.MustCompile(fmt.Sprintf("%s-%s", NetworkTopologyFilePrefix, modelKey))
				for _, fileInfo := range fileInfos {
					if !fileInfo.IsDir() && networkTopologyRegexp.MatchString(fileInfo.Name()) {
						networkTopologyFiles = append(networkTopologyFiles, fileInfo)
					}
				}
				assert.Equal(len(networkTopologyFiles), 0)
				assert.Equal(len(s.(*storage).networkTopologyModelKeys.Values()), 0)
			},
		},
		{
			name:    "open file failed",
			baseDir: os.TempDir(),
			mock: func(t *testing.T, s Storage, baseDir, modelKey string, download, networkTopology []byte) {
				if err := s.CreateDownload(download, modelKey); err != nil {
					t.Fatal(err)
				}

				if err := s.CreateNetworkTopology(networkTopology, modelKey); err != nil {
					t.Fatal(err)
				}
				s.(*storage).baseDir = "bar"
			},
			expect: func(t *testing.T, s Storage, baseDir, modelKey string) {
				assert := assert.New(t)
				assert.Error(s.Clear())

				s.(*storage).baseDir = baseDir
				assert.NoError(s.Clear())
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s, err := New(tc.baseDir)
			if err != nil {
				t.Fatal(err)
			}

			tc.mock(t, s, tc.baseDir, mockModelKey, testDownloadData, testNetworkTopologyData)
			tc.expect(t, s, tc.baseDir, mockModelKey)
		})
	}
}

func TestStorage_downloadFilename(t *testing.T) {
	baseDir := os.TempDir()
	s, err := New(baseDir)
	if err != nil {
		t.Fatal(err)
	}

	filename := s.(*storage).downloadFilename(mockModelKey)
	regexp := regexp.MustCompile(fmt.Sprintf("%s-%s.*.%s$", DownloadFilePrefix, mockModelKey, CSVFileExt))
	assert := assert.New(t)
	assert.True(regexp.MatchString(filename))
}

func TestStorage_networkTopologyFilename(t *testing.T) {
	baseDir := os.TempDir()
	s, err := New(baseDir)
	if err != nil {
		t.Fatal(err)
	}

	filename := s.(*storage).networkTopologyFilename(mockModelKey)
	regexp := regexp.MustCompile(fmt.Sprintf("%s-%s.*.%s$", NetworkTopologyFilePrefix, mockModelKey, CSVFileExt))
	assert := assert.New(t)
	assert.True(regexp.MatchString(filename))
}
