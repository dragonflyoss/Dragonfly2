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
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/gocarina/gocsv"
)

const (
	// RecordFilePrefix is prefix of record file name
	RecordFilePrefix = "record"
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
	Cost uint32

	// PieceCount is total piece count
	PieceCount int32

	// TotalPieceCount is total piece count
	TotalPieceCount int32

	// ContentLength is task total content length
	ContentLength int64

	// SecurityDomain is security domain of host
	SecurityDomain string

	// IDC is internet data center of host
	IDC string

	// NetTopology is network topology of host
	// Example: switch|router|...
	NetTopology string

	// Location is location of host
	// Example: country|province|...
	Location string

	// FreeUploadLoad is free upload load of host
	FreeUploadLoad int32

	// State is the download state of the peer
	State int

	// CreateAt is peer create time
	CreateAt time.Time

	// UpdateAt is peer update time
	UpdateAt time.Time

	// ParentID is parent peer id
	ParentID string

	// ParentIP is parent host ip
	ParentIP string

	// ParentHostname is parent host name
	ParentHostname string

	// ParentBizTag is parent peer biz tag
	ParentBizTag string

	// ParentCost is the parent task download time(millisecond)
	ParentCost uint32

	// ParentPieceCount is parent total piece count
	ParentPieceCount int32

	// ParentTotalPieceCount is parent total piece count
	ParentTotalPieceCount int32

	// ParentContentLength is parent task total content length
	ParentContentLength int64

	// ParentSecurityDomain is parent security domain of host
	ParentSecurityDomain string

	// ParentIDC is parent internet data center of host
	ParentIDC string

	// ParentNetTopology is parent network topology of host
	// Example: switch|router|...
	ParentNetTopology string

	// ParentLocation is parent location of host
	// Example: country|province|...
	ParentLocation string

	// ParentFreeUploadLoad is parent free upload load of host
	ParentFreeUploadLoad int32

	// ParentIsCDN is used as tag cdn
	ParentIsCDN bool

	// ParentCreateAt is parent peer create time
	ParentCreateAt time.Time

	// ParentUpdateAt is parent peer update time
	ParentUpdateAt time.Time
}

// Storage is the interface used for storage
type Storage interface {
	// Create inserts the record into csv file
	Create(context.Context, Record) error

	// Get returns the record in csv file
	Get(context.Context, string) (Record error)

	// List returns all of records in csv file
	List(context.Context) ([]Record, error)
}

// storage provides storage function
type storage struct {
	baseDir    string
	maxSize    uint
	maxBackups uint
}

// Option is a functional option for configuring the Storage
type Option func(s *storage)

// WithMaxSize set the maximum size in megabytes of storage file
func WithMaxSize(maxSize uint) Option {
	return func(s *storage) {
		s.maxSize = maxSize
	}
}

// WithMaxBackups set the maximum number of storage files to retain
func WithMaxBackups(maxBackups uint) Option {
	return func(s *storage) {
		s.maxBackups = maxBackups
	}
}

// New returns a new Storage instence
func New(baseDir string, options ...Option) Storage {
	s := &storage{
		baseDir: baseDir,
	}

	for _, opt := range options {
		opt(s)
	}

	return s
}

// Create inserts the record into csv file
func (s *storage) Create(ctx context.Context, record Record) error {
	file, err := os.OpenFile(filepath.Join(s.baseDir, fmt.Sprintf("%s.csv", RecordFilePrefix)), os.O_RDWR|os.O_CREATE|os.O_APPEND, os.ModePerm)
	if err != nil {
		return err
	}
	defer file.Close()

	if err := gocsv.MarshalWithoutHeaders(items, file); err != nil {
		return err
	}
	return nil
}
