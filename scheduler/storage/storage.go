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
	"errors"
	"fmt"
	"io"
	"io/fs"
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
)

const (
	// megabyte is the converted factor of MaxSize and bytes.
	megabyte = 1024 * 1024

	// backupTimeFormat is the timestamp format of backup filename.
	backupTimeFormat = "2006-01-02T15-04-05.000"
)

// Storage is the interface used for storage.
type Storage interface {
	// CreateDownload inserts the download into csv file.
	CreateDownload(Download) error

	// CreateNetworkTopology inserts the network topology into csv file.
	CreateNetworkTopology(NetworkTopology) error

	// ListDownload returns all downloads in csv file.
	ListDownload() ([]Download, error)

	// ListNetworkTopology returns all network topologies in csv file.
	ListNetworkTopology() ([]NetworkTopology, error)

	// DownloadCount returns the count of downloads.
	DownloadCount() int64

	// NetworkTopologyCount returns the count of network topologies.
	NetworkTopologyCount() int64

	// OpenDownload opens download files for read, it returns io.ReadCloser of download files.
	OpenDownload() (io.ReadCloser, error)

	// OpenNetworkTopology opens network topology files for read, it returns io.ReadCloser of network topology files.
	OpenNetworkTopology() (io.ReadCloser, error)

	// ClearDownload removes all download files.
	ClearDownload() error

	// ClearNetworkTopology removes all network topology files.
	ClearNetworkTopology() error
}

// storage provides storage function.
type storage struct {
	baseDir    string
	maxSize    int64
	maxBackups int
	bufferSize int

	downloadMu       *sync.RWMutex
	downloadFilename string
	downloadBuffer   []Download
	downloadCount    int64

	networkTopologyMu       *sync.RWMutex
	networkTopologyFilename string
	networkTopologyBuffer   []NetworkTopology
	networkTopologyCount    int64
}

// New returns a new Storage instance.
func New(baseDir string, maxSize, maxBackups, bufferSize int) (Storage, error) {
	s := &storage{
		baseDir:    baseDir,
		maxSize:    int64(maxSize * megabyte),
		maxBackups: maxBackups,
		bufferSize: bufferSize,

		downloadMu:       &sync.RWMutex{},
		downloadFilename: filepath.Join(baseDir, fmt.Sprintf("%s.%s", DownloadFilePrefix, CSVFileExt)),
		downloadBuffer:   make([]Download, 0, bufferSize),

		networkTopologyMu:       &sync.RWMutex{},
		networkTopologyFilename: filepath.Join(baseDir, fmt.Sprintf("%s.%s", NetworkTopologyFilePrefix, CSVFileExt)),
		networkTopologyBuffer:   make([]NetworkTopology, 0, bufferSize),
	}

	downloadFile, err := os.OpenFile(s.downloadFilename, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return nil, err
	}
	downloadFile.Close()

	networkTopologyFile, err := os.OpenFile(s.networkTopologyFilename, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return nil, err
	}
	networkTopologyFile.Close()

	return s, nil
}

// CreateDownload inserts the download into csv file.
func (s *storage) CreateDownload(download Download) error {
	s.downloadMu.Lock()
	defer s.downloadMu.Unlock()

	// Write without buffer.
	if s.bufferSize == 0 {
		if err := s.createDownload(download); err != nil {
			return err
		}

		// Update download count.
		s.downloadCount++
		return nil
	}

	// Write downloads to file.
	if len(s.downloadBuffer) >= s.bufferSize {
		if err := s.createDownload(s.downloadBuffer...); err != nil {
			return err
		}

		// Update download count.
		s.downloadCount += int64(s.bufferSize)

		// Keep allocated memory.
		s.downloadBuffer = s.downloadBuffer[:0]
	}

	// Write downloads to buffer.
	s.downloadBuffer = append(s.downloadBuffer, download)
	return nil
}

// CreateNetworkTopology inserts the network topology into csv file.
func (s *storage) CreateNetworkTopology(networkTopology NetworkTopology) error {
	s.networkTopologyMu.Lock()
	defer s.networkTopologyMu.Unlock()

	// Write without buffer.
	if s.bufferSize == 0 {
		if err := s.createNetworkTopology(networkTopology); err != nil {
			return err
		}

		// Update network topology count.
		s.networkTopologyCount++
		return nil
	}

	// Write network topologies to file.
	if len(s.networkTopologyBuffer) >= s.bufferSize {
		if err := s.createNetworkTopology(s.networkTopologyBuffer...); err != nil {
			return err
		}

		// Update network topology count.
		s.networkTopologyCount += int64(s.bufferSize)

		// Keep allocated memory.
		s.networkTopologyBuffer = s.networkTopologyBuffer[:0]
	}

	// Write network topologies to buffer.
	s.networkTopologyBuffer = append(s.networkTopologyBuffer, networkTopology)
	return nil
}

// ListDownload returns all downloads in csv file.
func (s *storage) ListDownload() ([]Download, error) {
	s.downloadMu.RLock()
	defer s.downloadMu.RUnlock()

	fileInfos, err := s.downloadBackups()
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

// ListNetworkTopology returns all network topologies in csv file.
func (s *storage) ListNetworkTopology() ([]NetworkTopology, error) {
	s.networkTopologyMu.RLock()
	defer s.networkTopologyMu.RUnlock()

	fileInfos, err := s.networkTopologyBackups()
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

// DownloadCount returns the count of downloads.
func (s *storage) DownloadCount() int64 {
	return s.downloadCount
}

// NetworkTopologyCount returns the count of network topologies.
func (s *storage) NetworkTopologyCount() int64 {
	return s.networkTopologyCount
}

// OpenDownload opens download files for read, it returns io.ReadCloser of download files.
func (s *storage) OpenDownload() (io.ReadCloser, error) {
	s.downloadMu.RLock()
	defer s.downloadMu.RUnlock()

	fileInfos, err := s.downloadBackups()
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

// OpenNetworkTopology opens network topology files for read, it returns io.ReadCloser of network topology files.
func (s *storage) OpenNetworkTopology() (io.ReadCloser, error) {
	s.networkTopologyMu.RLock()
	defer s.networkTopologyMu.RUnlock()

	fileInfos, err := s.networkTopologyBackups()
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

	fileInfos, err := s.downloadBackups()
	if err != nil {
		return err
	}

	for _, fileInfo := range fileInfos {
		filename := filepath.Join(s.baseDir, fileInfo.Name())
		if err := os.Remove(filename); err != nil {
			return err
		}
	}

	return nil
}

// ClearNetworkTopology removes all network topologies.
func (s *storage) ClearNetworkTopology() error {
	s.networkTopologyMu.Lock()
	defer s.networkTopologyMu.Unlock()

	fileInfos, err := s.networkTopologyBackups()
	if err != nil {
		return err
	}

	for _, fileInfo := range fileInfos {
		filename := filepath.Join(s.baseDir, fileInfo.Name())
		if err := os.Remove(filename); err != nil {
			return err
		}
	}

	return nil
}

// createDownload inserts the downloads into csv file.
func (s *storage) createDownload(downloads ...Download) (err error) {
	file, err := s.openDownloadFile()
	if err != nil {
		return err
	}
	defer func() {
		if cerr := file.Close(); cerr != nil {
			err = errors.Join(err, cerr)
		}
	}()

	return gocsv.MarshalWithoutHeaders(downloads, file)
}

// createNetworkTopology inserts the network topologies into csv file.
func (s *storage) createNetworkTopology(networkTopologies ...NetworkTopology) (err error) {
	file, err := s.openNetworkTopologyFile()
	if err != nil {
		return err
	}
	defer func() {
		if cerr := file.Close(); cerr != nil {
			err = errors.Join(err, cerr)
		}
	}()

	return gocsv.MarshalWithoutHeaders(networkTopologies, file)
}

// openDownloadFile opens the download file and removes download files that exceed the total size.
func (s *storage) openDownloadFile() (*os.File, error) {
	fileInfo, err := os.Stat(s.downloadFilename)
	if err != nil {
		return nil, err
	}

	if s.maxSize <= fileInfo.Size() {
		if err := os.Rename(s.downloadFilename, s.downloadBackupFilename()); err != nil {
			return nil, err
		}
	}

	fileInfos, err := s.downloadBackups()
	if err != nil {
		return nil, err
	}

	if s.maxBackups < len(fileInfos)+1 {
		filename := filepath.Join(s.baseDir, fileInfos[0].Name())
		if err := os.Remove(filename); err != nil {
			return nil, err
		}
	}

	file, err := os.OpenFile(s.downloadFilename, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
	if err != nil {
		return nil, err
	}

	return file, nil
}

// openNetworkTopologyFile opens the network topology file and removes network topology files that exceed the total size.
func (s *storage) openNetworkTopologyFile() (*os.File, error) {
	fileInfo, err := os.Stat(s.networkTopologyFilename)
	if err != nil {
		return nil, err
	}

	if s.maxSize <= fileInfo.Size() {
		if err := os.Rename(s.networkTopologyFilename, s.networkTopologyBackupFilename()); err != nil {
			return nil, err
		}
	}

	fileInfos, err := s.networkTopologyBackups()
	if err != nil {
		return nil, err
	}

	if s.maxBackups < len(fileInfos)+1 {
		filename := filepath.Join(s.baseDir, fileInfos[0].Name())
		if err := os.Remove(filename); err != nil {
			return nil, err
		}
	}

	file, err := os.OpenFile(s.networkTopologyFilename, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
	if err != nil {
		return nil, err
	}

	return file, nil
}

// downloadBackupFilename generates download file name of backup files.
func (s *storage) downloadBackupFilename() string {
	timestamp := time.Now().Format(backupTimeFormat)
	return filepath.Join(s.baseDir, fmt.Sprintf("%s_%s.%s", DownloadFilePrefix, timestamp, CSVFileExt))
}

// networkTopologyBackupFilename generates network topology file name of backup files.
func (s *storage) networkTopologyBackupFilename() string {
	timestamp := time.Now().Format(backupTimeFormat)
	return filepath.Join(s.baseDir, fmt.Sprintf("%s_%s.%s", NetworkTopologyFilePrefix, timestamp, CSVFileExt))
}

// downloadBackups returns download backup file information.
func (s *storage) downloadBackups() ([]fs.FileInfo, error) {
	fileInfos, err := os.ReadDir(s.baseDir)
	if err != nil {
		return nil, err
	}

	var backups []fs.FileInfo
	regexp := regexp.MustCompile(DownloadFilePrefix)
	for _, fileInfo := range fileInfos {
		if !fileInfo.IsDir() && regexp.MatchString(fileInfo.Name()) {
			info, _ := fileInfo.Info()
			backups = append(backups, info)
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

// networkTopologyBackups returns network topology backup file information.
func (s *storage) networkTopologyBackups() ([]fs.FileInfo, error) {
	fileInfos, err := os.ReadDir(s.baseDir)
	if err != nil {
		return nil, err
	}

	var backups []fs.FileInfo
	regexp := regexp.MustCompile(NetworkTopologyFilePrefix)
	for _, fileInfo := range fileInfos {
		if !fileInfo.IsDir() && regexp.MatchString(fileInfo.Name()) {
			info, _ := fileInfo.Info()
			backups = append(backups, info)
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
