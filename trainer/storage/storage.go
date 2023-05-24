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

//go:generate mockgen -destination mocks/storage_mock.go -source storage.go -package mocks

package storage

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"sync"
	"time"

	"github.com/gocarina/gocsv"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	pkgio "d7y.io/dragonfly/v2/pkg/io"
)

const (
	// DownloadFilePrefix is prefix of download file name.
	DownloadFilePrefix = "download"

	// NetworkTopologyFilePrefix is prefix of network topology file name.
	NetworkTopologyFilePrefix = "networktopology"

	// CSVFileExt is extension of file name.
	CSVFileExt = "csv"

	// TempFileInfix is infix of temp file name.
	TempFileInfix = "temp"
)

const (
	// megabyte is the converted factor of MaxSize and bytes.
	megabyte = 1024 * 1024

	// backupTimeFormat is the timestamp format of backup filename.
	backupTimeFormat = "2006-01-02T15-04-05.000"
)

const (
	// DefaultStorageMaxBackups is the default maximum count of backup for each model.
	DefaultStorageMaxBackups = 10

	// DefaultStorageMaxSize is the default maximum size in megabytes of storage file.
	DefaultStorageMaxSize = 500
)

// Storage is the interface used for storage.
type Storage interface {
	// CreateDownloadTempFile creates download temp file.
	CreateDownloadTempFile([]byte, string) error

	// CreateNetworkTopologyTempFile creates network topology temp file.
	CreateNetworkTopologyTempFile([]byte, string) error

	// CreateDownload inserts downloads into csv files from temp file based on the given model key.
	CreateDownload(string) error

	// CreateNetworkTopology inserts network topologies into csv files from temp file based on the given model key.
	CreateNetworkTopology(string) error

	// ListDownload returns downloads in csv files based on the given model key.
	ListDownload(string) ([]Download, error)

	// ListNetworkTopology returns network topologies in csv files based on the given model key.
	ListNetworkTopology(string) ([]NetworkTopology, error)

	// OpenDownload opens download files for read based on the given model key, it returns io.ReadCloser of download files.
	OpenDownload(string) (io.ReadCloser, error)

	// OpenNetworkTopology opens network topology files for read based on the given model key, it returns io.ReadCloser of network topology files.
	OpenNetworkTopology(string) (io.ReadCloser, error)

	// ClearDownload removes all download files.
	ClearDownload() error

	// ClearNetworkTopology removes all network topology files.
	ClearNetworkTopology() error
}

type storage struct {
	baseDir    string
	maxSize    int64
	maxBackups int

	downloadMu       *sync.RWMutex
	downloadTempFile map[string]*os.File
	downloadModelKey []string

	networkTopologyMu       *sync.RWMutex
	networkTopologyTempFile map[string]*os.File
	networkTopologyModelKey []string
}

// New returns a new Storage instance.
func New(baseDir string, maxSize, maxBackups int) (Storage, error) {
	s := &storage{
		baseDir:    baseDir,
		maxSize:    int64(maxSize * megabyte),
		maxBackups: maxBackups,

		downloadMu:       &sync.RWMutex{},
		downloadTempFile: make(map[string]*os.File),
		downloadModelKey: []string{},

		networkTopologyMu:       &sync.RWMutex{},
		networkTopologyTempFile: make(map[string]*os.File),
		networkTopologyModelKey: []string{},
	}

	return s, nil
}

// CreateDownloadTempFile creates download temp file.
func (s *storage) CreateDownloadTempFile(downloads []byte, modelKey string) error {

	// Init download temp file based on modelKey.
	filename := filepath.Join(s.baseDir, fmt.Sprintf("%s-%s-%s.%s", DownloadFilePrefix, TempFileInfix, modelKey, CSVFileExt))
	file, ok := s.downloadTempFile[modelKey]
	if !ok {
		tempFile, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
		if err != nil {
			return err
		}
		defer tempFile.Close()

		// Update download temp file map
		s.downloadTempFile[modelKey] = tempFile

		// Update model key list.
		s.downloadModelKey = append(s.downloadModelKey, modelKey)
	}

	// Write downloads to temp file.
	if _, err := io.Copy(file, bytes.NewReader(downloads)); err != nil {
		return err
	}

	return nil
}

// CreateNetworkTopologyTempFile creates network topology temp file.
func (s *storage) CreateNetworkTopologyTempFile(networkTopologies []byte, modelKey string) error {

	// Init network topology temp file based on model Key.
	filename := filepath.Join(s.baseDir, fmt.Sprintf("%s-%s-%s.%s", NetworkTopologyFilePrefix, TempFileInfix, modelKey, CSVFileExt))
	if _, ok := s.networkTopologyTempFile[modelKey]; !ok {
		file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
		if err != nil {
			return err
		}

		// Update network topology temp file map
		s.networkTopologyTempFile[modelKey] = file
		defer file.Close()

		// Update model key list.
		s.networkTopologyModelKey = append(s.networkTopologyModelKey, modelKey)
	}

	// Write network topologies to temp file.
	if _, err := io.Copy(s.networkTopologyTempFile[modelKey], bytes.NewReader(networkTopologies)); err != nil {
		return err
	}

	return nil
}

// CreateDownload inserts downloads into csv files from temp file based on the given model key.
func (s *storage) CreateDownload(modelKey string) error {
	file, err := s.openDownloadFile(modelKey)
	if err != nil {
		return err
	}
	defer file.Close()

	tempFilename := filepath.Join(s.baseDir, fmt.Sprintf("%s-%s-%s.%s", DownloadFilePrefix, TempFileInfix, modelKey, CSVFileExt))
	if _, err := os.Stat(tempFilename); err != nil {
		return err
	}

	// Write downloads to download csv file.
	if _, err := io.Copy(file, s.downloadTempFile[modelKey]); err != nil {
		return err
	}

	// Delete download temp file.
	if err := os.Remove(tempFilename); err != nil {
		return err
	}

	return nil
}

// CreateNetworkTopology inserts network topologies into csv files from temp file based on the given model key.
func (s *storage) CreateNetworkTopology(modelKey string) error {
	file, err := s.openNetworkTopologyFile(modelKey)
	if err != nil {
		return err
	}
	defer file.Close()

	cacheFilename := filepath.Join(s.baseDir, fmt.Sprintf("%s-%s-%s.%s", NetworkTopologyFilePrefix, TempFileInfix, modelKey, CSVFileExt))
	if _, err := os.Stat(cacheFilename); err != nil {
		return err
	}
	// Write network topologies to csv file.
	if _, err := io.Copy(file, s.networkTopologyTempFile[modelKey]); err != nil {
		return err
	}

	// Delete network topologies temp file.
	if err := os.Remove(cacheFilename); err != nil {
		return err
	}

	return nil
}

// ListDownload returns downloads in csv files based on the given model key.
func (s *storage) ListDownload(modelKey string) ([]Download, error) {
	s.downloadMu.RLock()
	defer s.downloadMu.RUnlock()

	fileInfos, err := s.downloadBackups(modelKey)
	if err != nil {
		return nil, err
	}

	var readers []io.Reader
	var readClosers []io.ReadCloser
	defer func() {
		for _, readCloser := range readClosers {
			if err := readCloser.Close(); err != nil {
				logger.Error(err)
			}
		}
	}()

	for _, fileInfo := range fileInfos {
		file, err := os.Open(filepath.Join(s.baseDir, fileInfo.Name()))
		if err != nil {
			return nil, err
		}

		readers = append(readers, file)
		readClosers = append(readClosers, file)
	}

	var downloads []Download
	if err := gocsv.UnmarshalWithoutHeaders(io.MultiReader(readers...), &downloads); err != nil {
		return nil, err
	}

	return downloads, nil
}

// ListNetworkTopology returns network topologies in csv files based on the given model key.
func (s *storage) ListNetworkTopology(modelKey string) ([]NetworkTopology, error) {
	s.networkTopologyMu.RLock()
	defer s.networkTopologyMu.RUnlock()

	fileInfos, err := s.networkTopologyBackups(modelKey)
	if err != nil {
		return nil, err
	}

	var readers []io.Reader
	var readClosers []io.ReadCloser
	defer func() {
		for _, readCloser := range readClosers {
			if err := readCloser.Close(); err != nil {
				logger.Error(err)
			}
		}
	}()

	for _, fileInfo := range fileInfos {
		file, err := os.Open(filepath.Join(s.baseDir, fileInfo.Name()))
		if err != nil {
			return nil, err
		}

		readers = append(readers, file)
		readClosers = append(readClosers, file)
	}

	var networkTopologies []NetworkTopology
	if err := gocsv.UnmarshalWithoutHeaders(io.MultiReader(readers...), &networkTopologies); err != nil {
		return nil, err
	}

	return networkTopologies, nil
}

// OpenDownload opens download files for read based on the given model key, it returns io.ReadCloser of download files.
func (s *storage) OpenDownload(modelKey string) (io.ReadCloser, error) {
	s.downloadMu.RLock()
	defer s.downloadMu.RUnlock()

	fileInfos, err := s.downloadBackups(modelKey)
	if err != nil {
		return nil, err
	}

	var readClosers []io.ReadCloser
	for _, fileInfo := range fileInfos {
		file, err := os.Open(filepath.Join(s.baseDir, fileInfo.Name()))
		if err != nil {
			return nil, err
		}

		readClosers = append(readClosers, file)
	}

	return pkgio.MultiReadCloser(readClosers...), nil
}

// OpenNetworkTopology opens network topology files for read based on the given model key, it returns io.ReadCloser of network topology files.
func (s *storage) OpenNetworkTopology(modelKey string) (io.ReadCloser, error) {
	s.networkTopologyMu.RLock()
	defer s.networkTopologyMu.RUnlock()

	fileInfos, err := s.networkTopologyBackups(modelKey)
	if err != nil {
		return nil, err
	}

	var readClosers []io.ReadCloser
	for _, fileInfo := range fileInfos {
		file, err := os.Open(filepath.Join(s.baseDir, fileInfo.Name()))
		if err != nil {
			return nil, err
		}

		readClosers = append(readClosers, file)
	}

	return pkgio.MultiReadCloser(readClosers...), nil
}

// ClearDownload removes all downloads.
func (s *storage) ClearDownload() error {
	s.downloadMu.Lock()
	defer s.downloadMu.Unlock()

	for _, modelKey := range s.downloadModelKey {
		fileInfos, err := s.downloadBackups(modelKey)
		if err != nil {
			return err
		}

		for _, fileInfo := range fileInfos {
			filename := filepath.Join(s.baseDir, fileInfo.Name())
			if err := os.Remove(filename); err != nil {
				return err
			}
		}
	}

	s.downloadModelKey = []string{}
	s.downloadTempFile = make(map[string]*os.File)
	return nil
}

// ClearNetworkTopology removes all network topologies.
func (s *storage) ClearNetworkTopology() error {
	s.networkTopologyMu.Lock()
	defer s.networkTopologyMu.Unlock()

	for _, modelKey := range s.networkTopologyModelKey {
		fileInfos, err := s.networkTopologyBackups(modelKey)
		if err != nil {
			return err
		}

		for _, fileInfo := range fileInfos {
			filename := filepath.Join(s.baseDir, fileInfo.Name())
			if err := os.Remove(filename); err != nil {
				return err
			}
		}
	}

	s.networkTopologyModelKey = []string{}
	s.networkTopologyTempFile = make(map[string]*os.File)
	return nil
}

// openDownloadFile opens the download file and removes download files that exceed the total size based on given model key.
func (s *storage) openDownloadFile(modelKey string) (*os.File, error) {
	downloadFilename := filepath.Join(s.baseDir, fmt.Sprintf("%s-%s.%s", DownloadFilePrefix, modelKey, CSVFileExt))

	fileInfo, err := os.Stat(downloadFilename)
	if err != nil {
		if _, ok := s.downloadTempFile[modelKey]; ok {
			file, e := os.OpenFile(downloadFilename, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
			if e != nil {
				return nil, e
			}
			return file, nil
		}
		return nil, err
	}

	if s.maxSize <= fileInfo.Size() {
		if err := os.Rename(downloadFilename, s.downloadBackupFilename(modelKey)); err != nil {
			return nil, err
		}
	}

	fileInfos, err := s.downloadBackups(modelKey)
	if err != nil {
		return nil, err
	}

	if s.maxBackups < len(fileInfos)+1 {
		filename := filepath.Join(s.baseDir, fileInfos[0].Name())
		if err := os.Remove(filename); err != nil {
			return nil, err
		}
	}

	file, err := os.OpenFile(downloadFilename, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
	if err != nil {
		return nil, err
	}

	return file, nil
}

// openNetworkTopologyFile opens the network topology file and removes network topology files that exceed the total size based on given model key.
func (s *storage) openNetworkTopologyFile(modelKey string) (*os.File, error) {
	networkTopologyFilename := filepath.Join(s.baseDir, fmt.Sprintf("%s-%s.%s", NetworkTopologyFilePrefix, modelKey, CSVFileExt))

	fileInfo, err := os.Stat(networkTopologyFilename)
	if err != nil {
		if _, ok := s.networkTopologyTempFile[modelKey]; ok {
			file, e := os.OpenFile(networkTopologyFilename, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
			if e != nil {
				return nil, e
			}
			return file, nil
		}
		return nil, err
	}

	if s.maxSize <= fileInfo.Size() {
		if err := os.Rename(networkTopologyFilename, s.networkTopologyBackupFilename(modelKey)); err != nil {
			return nil, err
		}
	}

	fileInfos, err := s.networkTopologyBackups(modelKey)
	if err != nil {
		return nil, err
	}

	if s.maxBackups < len(fileInfos)+1 {
		filename := filepath.Join(s.baseDir, fileInfos[0].Name())
		if err := os.Remove(filename); err != nil {
			return nil, err
		}
	}

	file, err := os.OpenFile(networkTopologyFilename, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
	if err != nil {
		return nil, err
	}

	return file, nil
}

// downloadBackupFilename generates download file name of backup files based on given model key.
func (s *storage) downloadBackupFilename(modelKey string) string {
	timestamp := time.Now().Format(backupTimeFormat)
	return filepath.Join(s.baseDir, fmt.Sprintf("%s-%s-%s.%s", DownloadFilePrefix, modelKey, timestamp, CSVFileExt))
}

// networkTopologyBackupFilename generates network topology file name of backup files based on given model key.
func (s *storage) networkTopologyBackupFilename(modelKey string) string {
	timestamp := time.Now().Format(backupTimeFormat)
	return filepath.Join(s.baseDir, fmt.Sprintf("%s-%s-%s.%s", NetworkTopologyFilePrefix, modelKey, timestamp, CSVFileExt))
}

// downloadBackups returns download backup file information based on given model key.
func (s *storage) downloadBackups(modelKey string) ([]fs.FileInfo, error) {
	fileInfos, err := ioutil.ReadDir(s.baseDir)
	if err != nil {
		return nil, err
	}

	var backups []fs.FileInfo

	regexp := regexp.MustCompile(fmt.Sprintf("%s-%s", DownloadFilePrefix, modelKey))
	for _, fileInfo := range fileInfos {
		if !fileInfo.IsDir() && regexp.MatchString(fileInfo.Name()) {
			backups = append(backups, fileInfo)
		}
	}

	if len(backups) <= 0 {
		return nil, errors.New("download files backup does not exist")
	}

	sort.Slice(backups, func(i, j int) bool {
		return backups[i].ModTime().Before(backups[j].ModTime())
	})

	return backups, nil
}

// networkTopologyBackups returns network topology backup file information based on given model key.
func (s *storage) networkTopologyBackups(modelKey string) ([]fs.FileInfo, error) {
	fileInfos, err := ioutil.ReadDir(s.baseDir)
	if err != nil {
		return nil, err
	}

	var backups []fs.FileInfo
	regexp := regexp.MustCompile(fmt.Sprintf("%s-%s", NetworkTopologyFilePrefix, modelKey))

	for _, fileInfo := range fileInfos {
		if !fileInfo.IsDir() && regexp.MatchString(fileInfo.Name()) {
			backups = append(backups, fileInfo)
		}
	}

	if len(backups) <= 0 {
		return nil, errors.New("network topology files backup does not exist")
	}

	sort.Slice(backups, func(i, j int) bool {
		return backups[i].ModTime().Before(backups[j].ModTime())
	})

	return backups, nil
}
