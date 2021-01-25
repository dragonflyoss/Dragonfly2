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
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/dragonflyoss/Dragonfly2/client/daemon/gc"
	logger "github.com/dragonflyoss/Dragonfly2/pkg/dflog"
	"github.com/dragonflyoss/Dragonfly2/pkg/rpc/base"
)

type TaskStorageDriver interface {
	// WritePiece put a piece of a task to storage
	WritePiece(ctx context.Context, req *WritePieceRequest) error

	// ReadPiece get a piece data reader of a task from storage
	// return a Reader and a Closer from task data with seeked, caller should read bytes and close it.
	ReadPiece(ctx context.Context, req *ReadPieceRequest) (io.Reader, io.Closer, error)

	GetPieces(ctx context.Context, req *base.PieceTaskRequest) (*base.PiecePacket, error)

	// Store stores task data to the target path
	Store(ctx context.Context, req *StoreRequest) error
}

type Manager interface {
	TaskStorageDriver
	// RegisterTask registers a task in storage driver
	RegisterTask(ctx context.Context, req RegisterTaskRequest) error
}

type Option struct {
	// DataPath indicates directory which stores temporary files for p2p uploading
	DataPath string
	// TaskExpireTime indicates caching duration for which cached file keeps no accessed by any process,
	// after this period cache file will be gc
	TaskExpireTime time.Duration
}

// TaskStorageBuilder creates a TaskStorageDriver for a task, lock free by storage manager
type TaskStorageExecutor interface {
	gc.GC
	// LoadTask loads TaskStorageDriver from memory for task operations
	LoadTask(meta PeerTaskMetaData) (TaskStorageDriver, bool)
	// CreateTask creates a new TaskStorageDriver
	CreateTask(request RegisterTaskRequest) error
	// ReloadPersistentTask
	ReloadPersistentTask(gcCallback GCCallback) error
}

var (
	ErrTaskNotFound = errors.New("task not found")

	drivers = make(map[Driver]func(opt *Option) (TaskStorageExecutor, error))
)

const (
	GCName = "StorageManager"
)

type storageManager struct {
	sync.Locker
	executor     TaskStorageExecutor
	driverOption *Option
	lastAccess   time.Time
}

type Driver string
type GCCallback func(request CommonTaskRequest)

func Register(driver Driver, exec func(opt *Option) (TaskStorageExecutor, error)) {
	drivers[driver] = exec
}

func NewStorageManager(driver Driver, opt *Option, gcCallback GCCallback, moreOpts ...func(*storageManager) error) (Manager, error) {
	s := &storageManager{
		Locker:       &sync.Mutex{},
		driverOption: opt,
	}
	for _, o := range moreOpts {
		if err := o(s); err != nil {
			return nil, err
		}
	}

	if driver == "" {
		// set default value
		driver = SimpleLocalTaskStoreDriver
	}

	logger.Infof("use storage driver: %s", driver)
	exe := drivers[driver]
	if exe == nil {
		panic("empty storage executor")
	}
	if executor, err := exe(opt); err != nil {
		return nil, fmt.Errorf("init storage executor error: %s", err)
	} else {
		s.executor = executor
	}
	if err := s.executor.ReloadPersistentTask(gcCallback); err != nil {
		logger.Warnf("reload tasks error: %s", err)
	}

	gc.Register(GCName, s)
	return s, nil
}

func WithTaskStoreDriver(e TaskStorageExecutor) func(*storageManager) error {
	return func(manager *storageManager) error {
		manager.executor = e
		return nil
	}
}

func WithStorageOption(opt *Option) func(*storageManager) error {
	return func(manager *storageManager) error {
		manager.driverOption = opt
		return nil
	}
}

func (s *storageManager) touch() {
	s.lastAccess = time.Now()
}

func (s *storageManager) RegisterTask(ctx context.Context, req RegisterTaskRequest) error {
	s.touch()
	if _, ok := s.executor.LoadTask(
		PeerTaskMetaData{
			PeerID: req.PeerID,
			TaskID: req.TaskID,
		}); !ok {
		// double check if task store exists
		// if ok, just unlock and return
		s.Lock()
		defer s.Unlock()
		if _, ok := s.executor.LoadTask(
			PeerTaskMetaData{
				PeerID: req.PeerID,
				TaskID: req.TaskID,
			}); ok {
			return nil
		}
		// still not exist, create a new task store
		return s.executor.CreateTask(req)
	}
	return nil
}

func (s *storageManager) WritePiece(ctx context.Context, req *WritePieceRequest) error {
	s.touch()
	t, ok := s.executor.LoadTask(
		PeerTaskMetaData{
			PeerID: req.PeerID,
			TaskID: req.TaskID,
		})
	if !ok {
		return ErrTaskNotFound
	}
	return t.(TaskStorageDriver).WritePiece(ctx, req)
}

func (s *storageManager) ReadPiece(ctx context.Context, req *ReadPieceRequest) (io.Reader, io.Closer, error) {
	s.touch()
	t, ok := s.executor.LoadTask(
		PeerTaskMetaData{
			PeerID: req.PeerID,
			TaskID: req.TaskID,
		})
	if !ok {
		// TODO recover for local task persistentMetadata data
		return nil, nil, ErrTaskNotFound
	}
	return t.(TaskStorageDriver).ReadPiece(ctx, req)
}

func (s *storageManager) Store(ctx context.Context, req *StoreRequest) error {
	s.touch()
	t, ok := s.executor.LoadTask(
		PeerTaskMetaData{
			PeerID: req.PeerID,
			TaskID: req.TaskID,
		})
	if !ok {
		// TODO recover for local task persistentMetadata data
		return ErrTaskNotFound
	}
	return t.(TaskStorageDriver).Store(ctx, req)
}

func (s *storageManager) GetPieces(ctx context.Context, req *base.PieceTaskRequest) (*base.PiecePacket, error) {
	s.touch()
	t, ok := s.executor.LoadTask(
		PeerTaskMetaData{
			TaskID: req.TaskId,
			PeerID: req.DstPid,
		})
	if !ok {
		// TODO recover for local task persistentMetadata data
		return nil, ErrTaskNotFound
	}
	return t.(TaskStorageDriver).GetPieces(ctx, req)
}

func (s *storageManager) TryGC() (bool, error) {
	return s.executor.TryGC()
}
