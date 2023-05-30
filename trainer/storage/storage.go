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
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/gocarina/gocsv"

	"d7y.io/dragonfly/v2/pkg/container/set"
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
)

// Storage is the interface used for storage.
type Storage interface {
	// CreateDownload inserts downloads into csv files based on the given model key.
	CreateDownload([]byte, string) error

	// CreateNetworkTopology inserts network topologies into csv files based on the given model key.
	CreateNetworkTopology([]byte, string) error

	// ListDownload returns downloads in csv files based on the given model key.
	ListDownload(string) ([]Download, error)

	// ListNetworkTopology returns network topologies in csv files based on the given model key.
	ListNetworkTopology(string) ([]NetworkTopology, error)

	// OpenDownload opens download files for read based on the given model key, it returns io.ReadCloser of download files.
	OpenDownload(string) (io.ReadCloser, error)

	// OpenNetworkTopology opens network topology files for read based on the given model key, it returns io.ReadCloser of network topology files.
	OpenNetworkTopology(string) (io.ReadCloser, error)

	// ClearDownload removes all download files based on the given model key.
	ClearDownload(string) error

	// ClearNetworkTopology removes all network topology files the given model key.
	ClearNetworkTopology(string) error

	// Clear removes all network topology files
	Clear() error
}

type storage struct {
	baseDir    string
	maxSize    int64
	maxBackups int

	downloadMu        *sync.RWMutex
	networkTopologyMu *sync.RWMutex

	downloadModelKeys        set.SafeSet[string]
	networkTopologyModelKeys set.SafeSet[string]
}

// New returns a new Storage instance.
func New(baseDir string, maxSize, maxBackups int) (Storage, error) {
	s := &storage{
		baseDir:    baseDir,
		maxSize:    int64(maxSize * megabyte),
		maxBackups: maxBackups,

		downloadMu:               &sync.RWMutex{},
		networkTopologyMu:        &sync.RWMutex{},
		downloadModelKeys:        set.NewSafeSet[string](),
		networkTopologyModelKeys: set.NewSafeSet[string](),
	}

	return s, nil
}

// CreateDownload inserts downloads into csv files based on the given model key.
func (s *storage) CreateDownload(downloads []byte, modelKey string) error {
	s.downloadMu.Lock()
	defer s.downloadMu.Unlock()

	filename := s.downloadFilename(modelKey)
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
	if err != nil {
		return err
	}

	// Write downloads to download csv file.
	if _, err := io.Copy(file, bytes.NewReader(downloads)); err != nil {
		if err := os.Remove(filename); err != nil {
			return err
		}

		return err
	}
	defer file.Close()

	// Add model key.
	s.downloadModelKeys.Add(modelKey)
	return nil
}

// CreateNetworkTopology inserts network topologies into csv files based on the given model key.
func (s *storage) CreateNetworkTopology(networkTopologies []byte, modelKey string) error {
	s.networkTopologyMu.Lock()
	defer s.networkTopologyMu.Unlock()

	filename := s.networkTopologyFilename(modelKey)
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
	if err != nil {
		return err
	}

	// Write network topologies to csv file.
	if _, err := io.Copy(file, bytes.NewReader(networkTopologies)); err != nil {
		if err := os.Remove(filename); err != nil {
			return err
		}
		return err
	}
	defer file.Close()

	// Add model key.
	s.networkTopologyModelKeys.Add(modelKey)
	return nil
}

// ListDownload returns downloads in csv files based on the given model key.
func (s *storage) ListDownload(modelKey string) ([]Download, error) {
	s.downloadMu.RLock()
	defer s.downloadMu.RUnlock()

	filename := s.downloadFilename(modelKey)
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var downloads []Download
	if err := gocsv.UnmarshalWithoutHeaders(file, &downloads); err != nil {
		return nil, err
	}

	return downloads, nil
}

// ListNetworkTopology returns network topologies in csv files based on the given model key.
func (s *storage) ListNetworkTopology(modelKey string) ([]NetworkTopology, error) {
	s.networkTopologyMu.RLock()
	defer s.networkTopologyMu.RUnlock()

	filename := s.networkTopologyFilename(modelKey)
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var networkTopologies []NetworkTopology
	if err := gocsv.UnmarshalWithoutHeaders(file, &networkTopologies); err != nil {
		return nil, err
	}

	return networkTopologies, nil
}

// OpenDownload opens download files for read based on the given model key, it returns io.ReadCloser of download files.
func (s *storage) OpenDownload(modelKey string) (io.ReadCloser, error) {
	s.downloadMu.RLock()
	defer s.downloadMu.RUnlock()

	filename := s.downloadFilename(modelKey)
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}

	return file, nil
}

// OpenNetworkTopology opens network topology files for read based on the given model key, it returns io.ReadCloser of network topology files.
func (s *storage) OpenNetworkTopology(modelKey string) (io.ReadCloser, error) {
	s.networkTopologyMu.RLock()
	defer s.networkTopologyMu.RUnlock()

	filename := s.networkTopologyFilename(modelKey)
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}

	return file, nil
}

// ClearDownload removes downloads based on the given model key.
func (s *storage) ClearDownload(modelKey string) error {
	s.downloadMu.Lock()
	defer s.downloadMu.Unlock()

	filename := s.downloadFilename(modelKey)
	if err := os.Remove(filename); err != nil {
		return err
	}

	s.downloadModelKeys.Delete(modelKey)
	return nil
}

// ClearNetworkTopology removes network topologies based on the given model key.
func (s *storage) ClearNetworkTopology(modelKey string) error {
	s.networkTopologyMu.Lock()
	defer s.networkTopologyMu.Unlock()

	filename := s.networkTopologyFilename(modelKey)
	if err := os.Remove(filename); err != nil {
		return err
	}

	s.networkTopologyModelKeys.Delete(modelKey)
	return nil
}

// ClearNetworkTopology removes all files.
func (s *storage) Clear() error {
	s.networkTopologyMu.Lock()
	defer s.networkTopologyMu.Unlock()

	modelKeys := s.downloadModelKeys.Values()
	for _, modelKey := range modelKeys {
		filename := s.downloadFilename(modelKey)
		if err := os.Remove(filename); err != nil {
			return err
		}
	}

	modelKeys = s.networkTopologyModelKeys.Values()
	for _, modelKey := range modelKeys {
		filename := s.networkTopologyFilename(modelKey)
		if err := os.Remove(filename); err != nil {
			return err
		}
	}

	s.downloadModelKeys = set.NewSafeSet[string]()
	s.networkTopologyModelKeys = set.NewSafeSet[string]()
	return nil
}

// downloadFilename generates download file name based on given model key.
func (s *storage) downloadFilename(modelKey string) string {
	return filepath.Join(s.baseDir, fmt.Sprintf("%s-%s.%s", DownloadFilePrefix, modelKey, CSVFileExt))
}

// networkTopologyFilename generates network topology file name based on given model key.
func (s *storage) networkTopologyFilename(modelKey string) string {
	return filepath.Join(s.baseDir, fmt.Sprintf("%s-%s.%s", NetworkTopologyFilePrefix, modelKey, CSVFileExt))
}
