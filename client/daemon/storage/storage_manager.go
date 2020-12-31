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

	GetPieces(ctx context.Context, req *base.PieceTaskRequest) ([]*base.PieceTask, error)

	// Store stores task data to the target path
	Store(ctx context.Context, req *StoreRequest) error
}

type Manager interface {
	TaskStorageDriver
	// RegisterTask registers a task in storage driver
	RegisterTask(ctx context.Context, req *RegisterTaskRequest) error
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
	LoadTask(taskID string, peerID string) (TaskStorageDriver, bool)
	// CreateTask creates a new TaskStorageDriver
	CreateTask(taskID string, peerID string, dest string) error
	// ReloadPersistentTask
	ReloadPersistentTask() error
}

var (
	ErrTaskNotFound = errors.New("task not found")

	drivers = make(map[Driver]func(opt *Option) (TaskStorageExecutor, error))
)

const (
	GCName = "StorageManager"
)

type storageManager struct {
	lock         sync.Locker
	executor     TaskStorageExecutor
	driverOption *Option
}

type Driver string

func Register(driver Driver, exec func(opt *Option) (TaskStorageExecutor, error)) {
	drivers[driver] = exec
}

func NewStorageManager(driver Driver, opt *Option, moreOpts ...func(*storageManager) error) (Manager, error) {
	s := &storageManager{
		lock:         &sync.Mutex{},
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
	if err := s.executor.ReloadPersistentTask(); err != nil {
		logger.Warnf("reload tasks error: %s", err)
	}

	// TODO
	gc.Register(GCName, s)
	// TODO reload tasks from local storage
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

func (s *storageManager) RegisterTask(ctx context.Context, req *RegisterTaskRequest) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	// double check if task store exists
	// if ok, just unlock and return
	if _, ok := s.executor.LoadTask(req.TaskID, req.PeerID); !ok {
		// still not exist, create a new task store
		return s.executor.CreateTask(req.TaskID, req.PeerID, req.Destination)
	}
	return nil
}

func (s *storageManager) WritePiece(ctx context.Context, req *WritePieceRequest) error {
	t, ok := s.executor.LoadTask(req.TaskID, req.PeerID)
	if !ok {
		return ErrTaskNotFound
	}
	return t.(TaskStorageDriver).WritePiece(ctx, req)
}

func (s *storageManager) ReadPiece(ctx context.Context, req *ReadPieceRequest) (io.Reader, io.Closer, error) {
	t, ok := s.executor.LoadTask(req.TaskID, req.PeerID)
	if !ok {
		// TODO recover for local task persistentMetadata data
		return nil, nil, ErrTaskNotFound
	}
	return t.(TaskStorageDriver).ReadPiece(ctx, req)
}

func (s *storageManager) Store(ctx context.Context, req *StoreRequest) error {
	t, ok := s.executor.LoadTask(req.TaskID, req.PeerID)
	if !ok {
		// TODO recover for local task persistentMetadata data
		return ErrTaskNotFound
	}
	return t.(TaskStorageDriver).Store(ctx, req)
}

func (s *storageManager) GetPieces(ctx context.Context, req *base.PieceTaskRequest) ([]*base.PieceTask, error) {
	t, ok := s.executor.LoadTask(req.TaskId, "")
	if !ok {
		// TODO recover for local task persistentMetadata data
		return nil, ErrTaskNotFound
	}
	return t.(TaskStorageDriver).GetPieces(ctx, req)
}

func (s *storageManager) TryGC() (bool, error) {
	return s.executor.TryGC()
}
