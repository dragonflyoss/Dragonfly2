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

package daemon

import (
	"context"
	"errors"
	"io"
	"os"
	"path"
	"sync"
	"syscall"
	"time"

	"github.com/dragonflyoss/Dragonfly2/client/util"
	"github.com/dragonflyoss/Dragonfly2/pkg/grpc/base"
	"github.com/sirupsen/logrus"
)

type PieceMetaData struct {
	TaskID string          `json:"taskID,omitempty"`
	Num    int32           `json:"num,omitempty"`
	Md5    string          `json:"md5,omitempty"`
	Offset uint64          `json:"offset,omitempty"`
	Range  util.Range      `json:"range,omitempty"`
	Style  base.PieceStyle `json:"style,omitempty"`
}

type PutPieceRequest struct {
	PieceMetaData
	Reader io.Reader
}

type StoreRequest struct {
	TaskID      string
	Destination string
}

type GetPieceRequest = PieceMetaData

type StorageDriver interface {
	// PutPiece put a piece of a task to storage
	PutPiece(ctx context.Context, req *PutPieceRequest) error

	// GetPiece get a piece data reader of a task from storage
	// return a Reader and a Closer from task data with seeked, caller should read bytes and close it.
	GetPiece(ctx context.Context, req *GetPieceRequest) (io.Reader, io.Closer, error)

	// StoreTaskData stores task data to the target path
	StoreTaskData(ctx context.Context, req *StoreRequest) error
}

type StorageManager = StorageDriver

type StorageOption struct {
	BaseDir    string
	GCInterval time.Duration
}

// TaskStorageBuilder creates a StorageDriver for a task, lock free by storage manager
type TaskStorageBuilder func(taskID string, opt *StorageOption) (StorageDriver, error)

var (
	ErrTaskNotFound = errors.New("task not found")
)

const (
	defaultFileMode      = os.FileMode(0644)
	defaultDirectoryMode = os.FileMode(0755)
)

const (
	taskData     = "data"
	taskMetaData = "metadata"
)

type localTaskStore struct {
	lock       sync.Locker
	taskID     string
	dir        string
	metadata   *os.File
	data       *os.File
	gcInterval time.Duration
	lastAccess time.Time
}

//type ReadWriteSeekCloser interface {
//	io.ReadWriteSeeker
//	io.Closer
//}

//type FdGetter interface {
//	Fd() uintptr
//}

type storageManager struct {
	baseDir            string
	lock               sync.Locker
	taskStores         sync.Map
	taskStorageBuilder TaskStorageBuilder
	opt                *StorageOption
}

func NewStorageManager(baseDir string, opts ...func(*storageManager) error) (StorageManager, error) {
	s := &storageManager{
		baseDir:    baseDir,
		lock:       &sync.Mutex{},
		taskStores: sync.Map{},
	}
	for _, opt := range opts {
		if err := opt(s); err != nil {
			return nil, err
		}
	}

	// set default value
	if s.taskStorageBuilder == nil {
		s.taskStorageBuilder = NewLocalTaskStorageBuilder
	}

	// TODO reload tasks from local storage
	return s, nil
}

func NewLocalTaskStorageBuilder(taskID string, opt *StorageOption) (StorageDriver, error) {
	ts := &localTaskStore{
		lock:       &sync.Mutex{},
		taskID:     taskID,
		dir:        path.Join(opt.BaseDir, taskID),
		gcInterval: opt.GCInterval,
	}
	if err := os.Mkdir(ts.dir, defaultDirectoryMode); err != nil && !os.IsExist(err) {
		return nil, err
	}
	metadata, err := os.OpenFile(path.Join(ts.dir, taskMetaData), os.O_CREATE|os.O_RDWR, defaultFileMode)
	if err != nil {
		return nil, err
	}
	ts.metadata = metadata

	data, err := os.OpenFile(path.Join(ts.dir, taskData), os.O_CREATE|os.O_RDWR, defaultFileMode)
	if err != nil {
		return nil, err
	}
	ts.data = data
	return ts, nil
}

func WithTaskStoreDriver(s TaskStorageBuilder) func(*storageManager) error {
	return func(manager *storageManager) error {
		manager.taskStorageBuilder = s
		return nil
	}
}

func WithStorageOption(opt *StorageOption) func(*storageManager) error {
	return func(manager *storageManager) error {
		manager.opt = opt
		return nil
	}
}

func (s *storageManager) PutPiece(ctx context.Context, req *PutPieceRequest) error {
	t, ok := s.taskStores.Load(req.TaskID)
	if !ok {
		s.lock.Lock()
		// double check if task store exists
		// if ok, just unlock and return
		if t, ok = s.taskStores.Load(req.TaskID); !ok {
			// still not exist, create a new task store
			var err error
			var opt *StorageOption
			if s.opt != nil {
				opt = s.opt
			} else {
				opt = &StorageOption{BaseDir: s.baseDir}
			}
			t, err = s.taskStorageBuilder(req.TaskID, opt)
			if err != nil {
				s.lock.Unlock()
				return err
			}
			s.taskStores.Store(req.TaskID, t)
		}
		s.lock.Unlock()
	}
	return t.(StorageDriver).PutPiece(ctx, req)
}

func (s *storageManager) GetPiece(ctx context.Context, req *GetPieceRequest) (io.Reader, io.Closer, error) {
	t, ok := s.taskStores.Load(req.TaskID)
	if !ok {
		// TODO recover for local task meta data
		return nil, nil, ErrTaskNotFound
	}
	return t.(StorageDriver).GetPiece(ctx, req)
}

func (s *storageManager) StoreTaskData(ctx context.Context, req *StoreRequest) error {
	t, ok := s.taskStores.Load(req.TaskID)
	if !ok {
		// TODO recover for local task meta data
		return ErrTaskNotFound
	}
	return t.(StorageDriver).StoreTaskData(ctx, req)
}

func (s *storageManager) TryGC() (bool, error) {
	// FIXME current lock stop the world, need lock by single task
	s.lock.Lock()
	s.lock.Unlock()
	var tasks []string
	s.taskStores.Range(func(key, value interface{}) bool {
		gc, err := value.(GC).TryGC()
		if err != nil {
			logrus.Errorf("gc task store %s error: %s", key, value)
		}
		if gc {
			tasks = append(tasks, key.(string))
			logrus.Infof("gc task store %s ok", key)
		}
		return true
	})
	for _, task := range tasks {
		s.taskStores.Delete(task)
	}
	return true, nil
}

func (t *localTaskStore) touch() {
	t.lastAccess = time.Now()
}

func (t *localTaskStore) PutPiece(ctx context.Context, req *PutPieceRequest) error {
	t.touch()
	t.lock.Lock()
	defer t.lock.Unlock()
	if _, err := t.data.Seek(int64(req.Range.Start), 0); err != nil {
		return err
	}
	_, err := io.Copy(t.data, io.LimitReader(req.Reader, req.Range.Length))
	return err
}

// GetPiece get a LimitReadCloser from task data with seeked, caller should read bytes and close it.
func (t *localTaskStore) GetPiece(ctx context.Context, req *GetPieceRequest) (io.Reader, io.Closer, error) {
	t.touch()
	// dup fd instead lock and open a new file
	dupFd, err := syscall.Dup(int(t.data.Fd()))
	if err != nil {
		return nil, nil, err
	}
	dupFile := os.NewFile(uintptr(dupFd), t.data.Name())
	// who call GetPiece, who close the io.ReadCloser
	if _, err = dupFile.Seek(req.Range.Start, 0); err != nil {
		return nil, nil, err
	}
	return io.LimitReader(dupFile, req.Range.Length), dupFile, nil
}

func (t *localTaskStore) StoreTaskData(ctx context.Context, req *StoreRequest) error {
	// 1. try to link
	err := os.Link(path.Join(t.dir, taskData), req.Destination)
	if err == nil {
		return nil
	}
	// 2. link failed, copy it
	dupFD, err := syscall.Dup(int(t.data.Fd()))
	if err != nil {
		return err
	}
	dupFile := os.NewFile(uintptr(dupFD), t.data.Name())
	dstFile, err := os.OpenFile(req.Destination, os.O_CREATE|os.O_RDWR, defaultFileMode)
	if err != nil {
		return err
	}
	defer dupFile.Close()
	defer dstFile.Close()
	// copy_file_range is valid in linux
	// https://go-review.googlesource.com/c/go/+/229101/
	_, err = io.Copy(dstFile, dupFile)
	return err
}

func (t *localTaskStore) TryGC() (bool, error) {
	if t.lastAccess.Add(t.gcInterval).Before(time.Now()) {
		var err error
		// close and remove metadata
		if err = t.metadata.Close(); err != nil {
			return true, err
		}
		if err = os.Remove(path.Join(t.dir, taskMetaData)); err != nil && !os.IsNotExist(err) {
			return true, err
		}

		// close and remove data
		if err = t.data.Close(); err != nil {
			return true, err
		}
		if err = os.Remove(path.Join(t.dir, taskData)); err != nil && !os.IsNotExist(err) {
			return true, err
		}

		// remove task work dir
		if err = os.Remove(t.dir); err != nil && !os.IsNotExist(err) {
			return true, err
		}
	}
	return true, nil
}
