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
	Create(data any) error

	// List returns specified data in csv file.
	List(data any) (any, error)

	// RecordCount returns the count of records.
	RecordCount() int64

	// ProbesCount returns the count of probes.
	ProbesCount() int64

	// Open opens storage for read, it returns io.ReadCloser of specified storage files.
	Open(data any) (io.ReadCloser, error)

	// Clear removes specified data.
	Clear(data any) error
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

	// Write without buffer.

	switch data.(type) {
	case Record:
		if s.bufferSize == 0 {
			if err := s.create(s.recordBuffer); err != nil {
				return err
			}

			// Update record count.
			s.recordCount++
			return nil
		}

		// Write records to file.
		if len(s.recordBuffer) >= s.bufferSize {
			if err := s.create(s.recordBuffer); err != nil {
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
		if s.bufferSize == 0 {
			if err := s.create(s.probesBuffer); err != nil {
				return err
			}

			// Update probes count.
			s.probesCount++
			return nil
		}

		// Write probes to file.
		if len(s.probesBuffer) >= s.bufferSize {
			if err := s.create(s.probesBuffer); err != nil {
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

// List returns specified data in csv file.
func (s *storage) List(data any) (any, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	fileInfos, err := s.backups(data)
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

	switch data.(type) {
	case Record:
		var records []Record
		if err := gocsv.UnmarshalWithoutHeaders(io.MultiReader(readers...), &records); err != nil {
			return nil, err
		}

		return records, nil
	case Probes:
		var probes []Probes
		if err := gocsv.UnmarshalWithoutHeaders(io.MultiReader(readers...), &probes); err != nil {
			return nil, err
		}

		return probes, nil
	default:
		return nil, errors.New("invalid data type")
	}
}

// RecordCount returns the count of records.
func (s *storage) RecordCount() int64 {
	return s.recordCount
}

// ProbesCount returns the count of probes.
func (s *storage) ProbesCount() int64 {
	return s.probesCount
}

// Open opens storage for read, it returns io.ReadCloser of specified storage files.
func (s *storage) Open(data any) (io.ReadCloser, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	fileInfos, err := s.backups(data)
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

// Clear removes specified data.
func (s *storage) Clear(data any) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	fileInfos, err := s.backups(data)
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
func (s *storage) create(data any) error {
	file, err := s.openFile(data)
	if err != nil {
		return err
	}
	defer file.Close()

	if err := gocsv.MarshalWithoutHeaders(data, file); err != nil {
		return err
	}

	return nil
}

// openFile opens the data file and removes data files that exceed the total size.
func (s *storage) openFile(data any) (*os.File, error) {

	var filename string
	switch data.(type) {
	case Record:
		filename = s.recordFilename
	case Probes:
		filename = s.probesFilename
	}
	fileInfo, err := os.Stat(filename)
	if err != nil {
		return nil, err
	}

	if s.maxSize <= fileInfo.Size() {
		if err := os.Rename(filename, s.backupFilename(data)); err != nil {
			return nil, err
		}
	}

	fileInfos, err := s.backups(data)
	if err != nil {
		return nil, err
	}

	if s.maxBackups < len(fileInfos)+1 {
		filename := filepath.Join(s.baseDir, fileInfos[0].Name())
		if err := os.Remove(filename); err != nil {
			return nil, err
		}
	}

	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
	if err != nil {
		return nil, err
	}

	return file, nil
}

// backupFilename generates file name of backup files.
func (s *storage) backupFilename(data any) string {
	timestamp := time.Now().Format(backupTimeFormat)
	switch data.(type) {
	case Record:
		return filepath.Join(s.baseDir, fmt.Sprintf("%s-%s.%s", RecordFilePrefix, timestamp, RecordFileExt))
	case Probes:
		return filepath.Join(s.baseDir, fmt.Sprintf("%s-%s.%s", ProbesFilePrefix, timestamp, ProbesFileExt))
	default:
		return ""
	}
}

// backupFilename returns backup file information.
func (s *storage) backups(data any) ([]fs.FileInfo, error) {
	fileInfos, err := ioutil.ReadDir(s.baseDir)
	if err != nil {
		return nil, err
	}

	var (
		backups    []fs.FileInfo
		filePrefix string
	)
	switch data.(type) {
	case Record:
		filePrefix = RecordFilePrefix
	case Probes:
		filePrefix = ProbesFilePrefix
	}

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
