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
	// RecordFilePrefix is prefix of record file name.
	RecordFilePrefix = "record"

	// RecordFileExt is extension of record file name.
	RecordFileExt = "csv"

	// ProbesFilePrefix is prefix of probes file name.
	ProbesFilePrefix = "probes"

	// ProbesFileExt is extension of probes file name.
	ProbesFileExt = "csv"
)

const (
	// megabyte is the converted factor of MaxSize and bytes.
	megabyte = 1024 * 1024

	// backupTimeFormat is the timestamp format of backup filename.
	backupTimeFormat = "2006-01-02T15-04-05.000"
)

// Storage is the interface used for storage.
type Storage interface {
	// Create inserts the data into csv file.
	Create(any) error

	// ListRecord returns all records in csv file.
	ListRecord() ([]Record, error)

	// ListProbes returns all probes in csv file.
	ListProbes() ([]Probes, error)

	// RecordCount returns the count of records.
	RecordCount() int64

	// ProbesCount returns the count of probes.
	ProbesCount() int64

	// OpenRecord opens storage for read, it returns io.ReadCloser of record storage files.
	OpenRecord() (io.ReadCloser, error)

	// OpenProbes opens storage for read, it returns io.ReadCloser of probes storage files.
	OpenProbes() (io.ReadCloser, error)

	// ClearRecord removes all record files.
	ClearRecord() error

	// ClearProbes removes all probes files.
	ClearProbes() error
}

// storage provides storage function.
type storage struct {
	baseDir    string
	maxSize    int64
	maxBackups int
	bufferSize int
	mu         *sync.RWMutex

	recordFilename string
	recordBuffer   []Record
	recordCount    int64

	probesFilename string
	probesBuffer   []Probes
	probesCount    int64
}

// New returns a new Storage instance.
func New(baseDir string, maxSize, maxBackups, bufferSize int) (Storage, error) {
	s := &storage{
		baseDir:    baseDir,
		maxSize:    int64(maxSize * megabyte),
		maxBackups: maxBackups,
		bufferSize: bufferSize,
		mu:         &sync.RWMutex{},

		recordFilename: filepath.Join(baseDir, fmt.Sprintf("%s.%s", RecordFilePrefix, RecordFileExt)),
		recordBuffer:   make([]Record, 0, bufferSize),
		probesFilename: filepath.Join(baseDir, fmt.Sprintf("%s.%s", ProbesFilePrefix, ProbesFileExt)),
		probesBuffer:   make([]Probes, 0, bufferSize),
	}

	RecordFile, err := os.OpenFile(s.recordFilename, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return nil, err
	}
	RecordFile.Close()

	ProbesFile, err := os.OpenFile(s.probesFilename, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return nil, err
	}
	ProbesFile.Close()

	return s, nil
}

// Create inserts the data into csv file.
func (s *storage) Create(data any) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	switch data.(type) {
	case Record:
		// Write without buffer.
		if s.bufferSize == 0 {
			if err := s.create(s.recordBuffer, RecordFilePrefix, RecordFileExt); err != nil {
				return err
			}

			// Update record count.
			s.recordCount++
			return nil
		}

		// Write records to file.
		if len(s.recordBuffer) >= s.bufferSize {
			if err := s.create(s.recordBuffer, RecordFilePrefix, RecordFileExt); err != nil {
				return err
			}

			// Update record count.
			s.recordCount += int64(s.bufferSize)

			// Keep allocated memory.
			s.recordBuffer = s.recordBuffer[:0]
		}

		// Write records to buffer.
		s.recordBuffer = append(s.recordBuffer, data.(Record))
		return nil
	case Probes:
		// Write without buffer.
		if s.bufferSize == 0 {
			if err := s.create(s.probesBuffer, ProbesFilePrefix, ProbesFileExt); err != nil {
				return err
			}

			// Update probes count.
			s.probesCount++
			return nil
		}

		// Write probes to file.
		if len(s.probesBuffer) >= s.bufferSize {
			if err := s.create(s.probesBuffer, ProbesFilePrefix, ProbesFileExt); err != nil {
				return err
			}

			// Update probes count.
			s.probesCount += int64(s.bufferSize)

			// Keep allocated memory.
			s.probesBuffer = s.probesBuffer[:0]
		}

		// Write probes to buffer.
		s.probesBuffer = append(s.probesBuffer, data.(Probes))
		return nil
	default:
		return errors.New("invalid storage type")
	}
}

// ListRecord returns all records in csv file.
func (s *storage) ListRecord() ([]Record, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	fileInfos, err := s.backups(RecordFilePrefix)
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

	var records []Record
	if err := gocsv.UnmarshalWithoutHeaders(io.MultiReader(readers...), &records); err != nil {
		return nil, err
	}

	return records, nil
}

// ListProbes returns all probes in csv file.
func (s *storage) ListProbes() ([]Probes, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	fileInfos, err := s.backups(ProbesFilePrefix)
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

	var probes []Probes
	if err := gocsv.UnmarshalWithoutHeaders(io.MultiReader(readers...), &probes); err != nil {
		return nil, err
	}

	return probes, nil
}

// RecordCount returns the count of records.
func (s *storage) RecordCount() int64 {
	return s.recordCount
}

// ProbesCount returns the count of probes.
func (s *storage) ProbesCount() int64 {
	return s.probesCount
}

// OpenRecord opens storage for read, it returns io.ReadCloser of record storage files.
func (s *storage) OpenRecord() (io.ReadCloser, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	fileInfos, err := s.backups(RecordFilePrefix)
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

// OpenProbes opens storage for read, it returns io.ReadCloser of record storage files.
func (s *storage) OpenProbes() (io.ReadCloser, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	fileInfos, err := s.backups(ProbesFilePrefix)
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

// ClearRecord removes all record files.
func (s *storage) ClearRecord() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	fileInfos, err := s.backups(RecordFilePrefix)
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

// ClearProbes removes all probes files.
func (s *storage) ClearProbes() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	fileInfos, err := s.backups(ProbesFilePrefix)
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

// create inserts the data into csv file.
func (s *storage) create(data any, filePrefix, fileExt string) error {
	file, err := s.openFile(filePrefix, fileExt)
	if err != nil {
		return err
	}
	defer file.Close()

	if err := gocsv.MarshalWithoutHeaders(data, file); err != nil {
		return err
	}

	return nil
}

// openFile opens the specified data file and removes specified data files that exceed the total size.
func (s *storage) openFile(filePrefix, fileExt string) (*os.File, error) {
	fileInfo, err := os.Stat(filepath.Join(s.baseDir, fmt.Sprintf("%s.%s", filePrefix, fileExt)))
	if err != nil {
		return nil, err
	}

	if s.maxSize <= fileInfo.Size() {
		if err := os.Rename(filepath.Join(s.baseDir, fmt.Sprintf("%s.%s", filePrefix, fileExt)),
			s.backupFilename(filePrefix, fileExt)); err != nil {
			return nil, err
		}
	}

	fileInfos, err := s.backups(filePrefix)
	if err != nil {
		return nil, err
	}

	if s.maxBackups < len(fileInfos)+1 {
		filename := filepath.Join(s.baseDir, fileInfos[0].Name())
		if err := os.Remove(filename); err != nil {
			return nil, err
		}
	}

	file, err := os.OpenFile(filepath.Join(s.baseDir, fmt.Sprintf("%s.%s", filePrefix, fileExt)),
		os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
	if err != nil {
		return nil, err
	}

	return file, nil
}

// backupFilename generates file name of specified backup files.
func (s *storage) backupFilename(filePrefix, fileExt string) string {
	timestamp := time.Now().Format(backupTimeFormat)
	return filepath.Join(s.baseDir, fmt.Sprintf("%s-%s.%s", filePrefix, timestamp, fileExt))
}

// backupFilename returns specified backup file information.
func (s *storage) backups(filePrefix string) ([]fs.FileInfo, error) {
	fileInfos, err := ioutil.ReadDir(s.baseDir)
	if err != nil {
		return nil, err
	}

	var backups []fs.FileInfo
	regexp := regexp.MustCompile(filePrefix)
	for _, fileInfo := range fileInfos {
		if !fileInfo.IsDir() && regexp.MatchString(fileInfo.Name()) {
			backups = append(backups, fileInfo)
		}
	}

	if len(backups) <= 0 {
		return nil, errors.New("backup does not exist")
	}

	sort.Slice(backups, func(i, j int) bool {
		return backups[i].ModTime().Before(backups[j].ModTime())
	})

	return backups, nil
}
