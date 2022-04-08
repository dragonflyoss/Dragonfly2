/*
 *     Copyright 2020 The Dragonfly Authors
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
)

const (
	// megabyte is the converted factor of MaxSize and bytes
	megabyte = 1024 * 1024

	// backupTimeFormat is the timestamp format of backup filename
	backupTimeFormat = "2006-01-02T15-04-05.000"

	// defaultMaxSize is the default maximum size of record file
	defaultMaxSize = 100

	// defaultMaxBackups is the default maximum count of backup
	defaultMaxBackups = 10
)

const (
	// RecordFilePrefix is prefix of record file name
	RecordFilePrefix = "record"

	// RecordFileExt is extension of record file name
	RecordFileExt = "csv"
)

const (
	// Peer has been downloaded successfully
	PeerStateSucceeded = iota

	// Peer has been downloaded failed
	PeerStateFailed

	// Peer has been back-to-source downloaded successfully
	PeerStateBackToSourceSucceeded

	// Peer has been back-to-source downloaded failed
	PeerStateBackToSourceFailed
)

type Record struct {
	// ID is peer id
	ID string `csv:"id"`

	// IP is host ip
	IP string `csv:"ip"`

	// Hostname is host name
	Hostname string `csv:"hostname"`

	// BizTag is peer biz tag
	BizTag string `csv:"bizTag"`

	// Cost is the task download time(millisecond)
	Cost uint32 `csv:"cost"`

	// PieceCount is total piece count
	PieceCount int32 `csv:"pieceCount"`

	// TotalPieceCount is total piece count
	TotalPieceCount int32 `csv:"totalPieceCount"`

	// ContentLength is task total content length
	ContentLength int64 `csv:"contentLength"`

	// SecurityDomain is security domain of host
	SecurityDomain string `csv:"securityDomain"`

	// IDC is internet data center of host
	IDC string `csv:"idc"`

	// NetTopology is network topology of host
	// Example: switch|router|...
	NetTopology string `csv:"netTopology"`

	// Location is location of host
	// Example: country|province|...
	Location string `csv:"location"`

	// FreeUploadLoad is free upload load of host
	FreeUploadLoad int32 `csv:"freeUpoladLoad"`

	// State is the download state of the peer
	State int `csv:"state"`

	// CreateAt is peer create time
	CreateAt time.Time `csv:"createAt"`

	// UpdateAt is peer update time
	UpdateAt time.Time `csv:"updateAt"`

	// ParentID is parent peer id
	ParentID string `csv:"parentID"`

	// ParentIP is parent host ip
	ParentIP string `csv:"parentIP"`

	// ParentHostname is parent host name
	ParentHostname string `csv:"parentHostname"`

	// ParentBizTag is parent peer biz tag
	ParentBizTag string `csv:"parentBizTag"`

	// ParentCost is the parent task download time(millisecond)
	ParentCost uint32 `csv:"parentCost"`

	// ParentPieceCount is parent total piece count
	ParentPieceCount int32 `csv:"parentPieceCount"`

	// ParentTotalPieceCount is parent total piece count
	ParentTotalPieceCount int32 `csv:"parentTotalPieceCount"`

	// ParentContentLength is parent task total content length
	ParentContentLength int64 `csv:"parentContentLength"`

	// ParentSecurityDomain is parent security domain of host
	ParentSecurityDomain string `csv:"parentSecurityDomain"`

	// ParentIDC is parent internet data center of host
	ParentIDC string `csv:"parentIDC"`

	// ParentNetTopology is parent network topology of host
	// Example: switch|router|...
	ParentNetTopology string `csv:"parentNetTopology"`

	// ParentLocation is parent location of host
	// Example: country|province|...
	ParentLocation string `csv:"parentLocation"`

	// ParentFreeUploadLoad is parent free upload load of host
	ParentFreeUploadLoad int32 `csv:"parentFreeUploadLoad"`

	// ParentIsCDN is used as tag cdn
	ParentIsCDN bool `csv:"parentIsCDN"`

	// ParentCreateAt is parent peer create time
	ParentCreateAt time.Time `csv:"parentCreateAt"`

	// ParentUpdateAt is parent peer update time
	ParentUpdateAt time.Time `csv:"parentUpdateAt"`
}

// Storage is the interface used for storage
type Storage interface {
	// Create inserts the record into csv file
	Create(...Record) error

	// List returns all of records in csv file
	List() ([]Record, error)

	// Clear removes all record files
	Clear() error
}

// storage provides storage function
type storage struct {
	baseDir    string
	filename   string
	maxSize    int64
	maxBackups int
	mu         *sync.RWMutex
}

// Option is a functional option for configuring the Storage
type Option func(s *storage)

// WithMaxSize set the maximum size in megabytes of storage file
func WithMaxSize(maxSize int) Option {
	return func(s *storage) {
		s.maxSize = int64(maxSize * megabyte)
	}
}

// WithMaxBackups set the maximum number of storage files to retain
func WithMaxBackups(maxBackups int) Option {
	return func(s *storage) {
		s.maxBackups = maxBackups
	}
}

// New returns a new Storage instence
func New(baseDir string, options ...Option) (Storage, error) {
	s := &storage{
		baseDir:    baseDir,
		filename:   filepath.Join(baseDir, fmt.Sprintf("%s.%s", RecordFilePrefix, RecordFileExt)),
		maxSize:    defaultMaxSize,
		maxBackups: defaultMaxBackups,
		mu:         &sync.RWMutex{},
	}

	for _, opt := range options {
		opt(s)
	}

	file, err := os.OpenFile(s.filename, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return nil, err
	}
	file.Close()

	return s, nil
}

// Create inserts the record into csv file
func (s *storage) Create(records ...Record) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	file, err := s.openFile()
	if err != nil {
		return err
	}
	defer file.Close()

	if err := gocsv.MarshalWithoutHeaders(records, file); err != nil {
		return err
	}

	return nil
}

// List returns all of records in csv file
func (s *storage) List() ([]Record, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	fileInfos, err := s.backups()
	if err != nil {
		return nil, err
	}

	var files []io.Reader
	for _, fileInfo := range fileInfos {
		file, err := os.Open(filepath.Join(s.baseDir, fileInfo.Name()))
		if err != nil {
			return nil, err
		}
		defer file.Close()
		files = append(files, file)
	}

	var records []Record
	if err := gocsv.Unmarshal(io.MultiReader(files...), records); err != nil {
		return nil, err
	}

	return records, nil
}

// Clear removes all records
func (s *storage) Clear() error {
	fileInfos, err := s.backups()
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

// openFile opens the record file and removes record files that exceed the total size
func (s *storage) openFile() (*os.File, error) {
	fileInfo, err := os.Stat(s.filename)
	if err != nil {
		return nil, err
	}

	if s.maxSize < fileInfo.Size() {
		if err := os.Rename(s.filename, s.backupFilename()); err != nil {
			return nil, err
		}
	}

	fileInfos, err := s.backups()
	if err != nil {
		return nil, err
	}

	if s.maxBackups < len(fileInfos) {
		filename := filepath.Join(s.baseDir, fileInfos[0].Name())
		if err := os.Remove(filename); err != nil {
			return nil, err
		}
	}

	file, err := os.OpenFile(s.filename, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
	if err != nil {
		return nil, err
	}

	return file, nil
}

// backupFilename generates file name of backup files
func (s *storage) backupFilename() string {
	timestamp := time.Now().Format(backupTimeFormat)
	return filepath.Join(s.baseDir, fmt.Sprintf("%s-%s.%s", RecordFilePrefix, timestamp, RecordFileExt))
}

// backupFilename returns backup file information
func (s *storage) backups() ([]fs.FileInfo, error) {
	fileInfos, err := ioutil.ReadDir(s.baseDir)
	if err != nil {
		return nil, err
	}

	var backups []fs.FileInfo
	regexp := regexp.MustCompile(RecordFilePrefix)
	for _, fileInfo := range fileInfos {
		if !fileInfo.IsDir() && regexp.MatchString(fileInfo.Name()) {
			backups = append(backups, fileInfo)
		}
	}

	if len(backups) <= 0 {
		return nil, errors.New("backup does not exist")
	}

	sort.Slice(backups, func(i, j int) bool {
		return backups[i].ModTime().After(backups[j].ModTime())
	})

	return backups, nil
}
