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

//go:generate mockgen -destination mocks/storage_mock.go -source storage.go -package mocks

package storage

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/gocarina/gocsv"

	schedulerstorage "d7y.io/dragonfly/v2/scheduler/storage"
)

const (
	// DownloadFilePrefix is prefix of download file name.
	DownloadFilePrefix = "download"

	// NetworkTopologyFilePrefix is prefix of network topology file name.
	NetworkTopologyFilePrefix = "networktopology"

	// CSVFileExt is extension of file name.
	CSVFileExt = "csv"
)

// Storage is the interface used for storage.
type Storage interface {
	// ListDownload returns downloads in csv files based on the given model key.
	ListDownload(string) ([]schedulerstorage.Download, error)

	// ListNetworkTopology returns network topologies in csv files based on the given model key.
	ListNetworkTopology(string) ([]schedulerstorage.NetworkTopology, error)

	// OpenDownload opens download files for read based on the given model key, it returns io.ReadCloser of download files.
	OpenDownload(string) (*os.File, error)

	// OpenNetworkTopology opens network topology files for read based on the given model key, it returns io.ReadCloser of network topology files.
	OpenNetworkTopology(string) (*os.File, error)

	// ClearDownload removes all downloads based on the given model key.
	ClearDownload(string) error

	// ClearNetworkTopology removes network topologies based on the given model key.
	ClearNetworkTopology(string) error

	// Clear removes all files.
	Clear() error
}

// storage provides storage function.
type storage struct {
	baseDir string
}

// New returns a new Storage instance.
func New(baseDir string) Storage {
	return &storage{baseDir: baseDir}
}

// ListDownload returns downloads in csv files based on the given model key.
func (s *storage) ListDownload(key string) (downloads []schedulerstorage.Download, err error) {
	file, err := s.OpenDownload(key)
	if err != nil {
		return nil, err
	}
	defer func() {
		if cerr := file.Close(); cerr != nil {
			err = errors.Join(err, cerr)
		}
	}()

	if err = gocsv.UnmarshalWithoutHeaders(file, &downloads); err != nil {
		return nil, err
	}

	return downloads, nil
}

// ListNetworkTopology returns network topologies in csv files based on the given model key.
func (s *storage) ListNetworkTopology(key string) (networkTopologies []schedulerstorage.NetworkTopology, err error) {
	file, err := s.OpenNetworkTopology(key)
	if err != nil {
		return nil, err
	}
	defer func() {
		if cerr := file.Close(); cerr != nil {
			err = errors.Join(err, cerr)
		}
	}()

	if err = gocsv.UnmarshalWithoutHeaders(file, &networkTopologies); err != nil {
		return nil, err
	}

	return networkTopologies, nil
}

// OpenDownload opens download files for read based on the given model key, it returns io.ReadCloser of download files.
func (s *storage) OpenDownload(key string) (*os.File, error) {
	return os.OpenFile(s.downloadFilename(key), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
}

// OpenNetworkTopology opens network topology files for read based on the given model key, it returns io.ReadCloser of network topology files.
func (s *storage) OpenNetworkTopology(key string) (*os.File, error) {
	return os.OpenFile(s.networkTopologyFilename(key), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
}

// ClearDownload removes downloads based on the given model key.
func (s *storage) ClearDownload(key string) error {
	return os.Remove(s.downloadFilename(key))
}

// ClearNetworkTopology removes network topologies based on the given model key.
func (s *storage) ClearNetworkTopology(key string) error {
	return os.Remove(s.networkTopologyFilename(key))
}

// Clear removes all files.
func (s *storage) Clear() error {
	return os.RemoveAll(s.baseDir)
}

// downloadFilename generates download file name based on the given model key.
func (s *storage) downloadFilename(key string) string {
	return filepath.Join(s.baseDir, fmt.Sprintf("%s_%s.%s", DownloadFilePrefix, key, CSVFileExt))
}

// networkTopologyFilename generates network topology file name based on the given model key.
func (s *storage) networkTopologyFilename(key string) string {
	return filepath.Join(s.baseDir, fmt.Sprintf("%s_%s.%s", NetworkTopologyFilePrefix, key, CSVFileExt))
}
