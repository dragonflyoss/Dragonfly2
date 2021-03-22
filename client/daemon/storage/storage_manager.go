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
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/pkg/errors"

	"d7y.io/dragonfly/v2/client/clientutil"
	"d7y.io/dragonfly/v2/client/daemon/gc"
	logger "d7y.io/dragonfly/v2/pkg/dflog"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
)

type TaskStorageDriver interface {
	// WritePiece put a piece of a task to storage
	WritePiece(ctx context.Context, req *WritePieceRequest) (int64, error)

	// ReadPiece get a piece data reader of a task from storage
	// return a Reader and a Closer from task data with seeked, caller should read bytes and close it.
	ReadPiece(ctx context.Context, req *ReadPieceRequest) (io.Reader, io.Closer, error)

	GetPieces(ctx context.Context, req *base.PieceTaskRequest) (*base.PiecePacket, error)

	UpdateTask(ctx context.Context, req *UpdateTaskRequest) error

	// Store stores task data to the target path
	Store(ctx context.Context, req *StoreRequest) error
}

// Reclaimer stands storage reclaimer
type Reclaimer interface {
	// CanReclaim indicates whether the storage can be reclaimed
	CanReclaim() bool

	// MarkReclaim marks the storage which will be reclaimed
	MarkReclaim()

	// Reclaim reclaims the storage
	Reclaim() error
}

type Manager interface {
	TaskStorageDriver
	// KeepAlive tests if storage is used in given time duration
	clientutil.KeepAlive
	// RegisterTask registers a task in storage driver
	RegisterTask(ctx context.Context, req RegisterTaskRequest) error
	// CleanUp cleans all storage data
	CleanUp()
}

type Option struct {
	// DataPath indicates directory which stores temporary files for p2p uploading
	DataPath string `json:"data_path" yaml:"data_path"`
	// TaskExpireTime indicates caching duration for which cached file keeps no accessed by any process,
	// after this period cache file will be gc
	TaskExpireTime clientutil.Duration `json:"task_expire_time" yaml:"task_expire_time"`
}

var (
	ErrTaskNotFound  = errors.New("task not found")
	ErrPieceNotFound = errors.New("piece not found")
)

const (
	GCName = "StorageManager"
)

type storageManager struct {
	sync.Locker
	clientutil.KeepAlive
	storeStrategy      StoreStrategy
	storeOption        *Option
	tasks              *sync.Map
	markedReclaimTasks []PeerTaskMetaData
	dataPathStat       *syscall.Stat_t
	gcCallback         func(CommonTaskRequest)
}

type StoreStrategy string
type GCCallback func(request CommonTaskRequest)

func NewStorageManager(storeStrategy StoreStrategy, opt *Option, gcCallback GCCallback, moreOpts ...func(*storageManager) error) (Manager, error) {
	if !path.IsAbs(opt.DataPath) {
		abs, err := filepath.Abs(opt.DataPath)
		if err != nil {
			return nil, err
		}
		opt.DataPath = abs
	}
	stat, err := os.Stat(opt.DataPath)
	if os.IsNotExist(err) {
		if err := os.MkdirAll(opt.DataPath, defaultDirectoryMode); err != nil {
			return nil, err
		}
		stat, err = os.Stat(opt.DataPath)
	}
	if err != nil {
		return nil, err
	}
	switch storeStrategy {
	case SimpleLocalTaskStoreStrategy, AdvanceLocalTaskStoreStrategy:
	case StoreStrategy(""):
		storeStrategy = SimpleLocalTaskStoreStrategy
	default:
		return nil, fmt.Errorf("not support store strategy: %s", storeStrategy)
	}

	s := &storageManager{
		KeepAlive:     clientutil.NewKeepAlive("storage manager"),
		storeStrategy: storeStrategy,
		Locker:        &sync.Mutex{},
		storeOption:   opt,
		tasks:         &sync.Map{},
		dataPathStat:  stat.Sys().(*syscall.Stat_t),
		gcCallback:    gcCallback,
	}

	for _, o := range moreOpts {
		if err := o(s); err != nil {
			return nil, err
		}
	}

	if err := s.ReloadPersistentTask(gcCallback); err != nil {
		logger.Warnf("reload tasks error: %s", err)
	}

	gc.Register(GCName, s)
	return s, nil
}

func WithStorageOption(opt *Option) func(*storageManager) error {
	return func(manager *storageManager) error {
		manager.storeOption = opt
		return nil
	}
}

func (s *storageManager) RegisterTask(ctx context.Context, req RegisterTaskRequest) error {
	if _, ok := s.LoadTask(
		PeerTaskMetaData{
			PeerID: req.PeerID,
			TaskID: req.TaskID,
		}); !ok {
		// double check if task store exists
		// if ok, just unlock and return
		s.Lock()
		defer s.Unlock()
		if _, ok := s.LoadTask(
			PeerTaskMetaData{
				PeerID: req.PeerID,
				TaskID: req.TaskID,
			}); ok {
			return nil
		}
		// still not exist, create a new task store
		return s.CreateTask(req)
	}
	return nil
}

func (s *storageManager) WritePiece(ctx context.Context, req *WritePieceRequest) (int64, error) {
	t, ok := s.LoadTask(
		PeerTaskMetaData{
			PeerID: req.PeerID,
			TaskID: req.TaskID,
		})
	if !ok {
		return 0, ErrTaskNotFound
	}
	return t.(TaskStorageDriver).WritePiece(ctx, req)
}

func (s *storageManager) ReadPiece(ctx context.Context, req *ReadPieceRequest) (io.Reader, io.Closer, error) {
	t, ok := s.LoadTask(
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
	t, ok := s.LoadTask(
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
	t, ok := s.LoadTask(
		PeerTaskMetaData{
			TaskID: req.TaskId,
			PeerID: req.DstPid,
		})
	if !ok {
		return nil, ErrTaskNotFound
	}
	return t.(TaskStorageDriver).GetPieces(ctx, req)
}

func (s *storageManager) LoadTask(meta PeerTaskMetaData) (TaskStorageDriver, bool) {
	s.Keep()
	d, ok := s.tasks.Load(meta)
	if !ok {
		return nil, false
	}
	return d.(TaskStorageDriver), ok
}

func (s *storageManager) UpdateTask(ctx context.Context, req *UpdateTaskRequest) error {
	t, ok := s.LoadTask(
		PeerTaskMetaData{
			TaskID: req.TaskID,
			PeerID: req.PeerID,
		})
	if !ok {
		return ErrTaskNotFound
	}
	return t.(TaskStorageDriver).UpdateTask(ctx, req)
}

func (s *storageManager) CreateTask(req RegisterTaskRequest) error {
	logger.Debugf("init local task storage, peer id: %s, task id: %s", req.PeerID, req.TaskID)

	dataDir := path.Join(s.storeOption.DataPath, string(s.storeStrategy), req.TaskID, req.PeerID)
	t := &localTaskStore{
		persistentMetadata: persistentMetadata{
			StoreStrategy: string(s.storeStrategy),
			TaskID:        req.TaskID,
			TaskMeta:      map[string]string{},
			ContentLength: req.ContentLength,
			TotalPieces:   req.TotalPieces,
			PeerID:        req.PeerID,
			Pieces:        map[int32]PieceMetaData{},
		},
		gcCallback:       s.gcCallback,
		RWMutex:          &sync.RWMutex{},
		dataDir:          dataDir,
		metadataFilePath: path.Join(dataDir, taskMetaData),
		expireTime:       s.storeOption.TaskExpireTime.Duration,

		SugaredLoggerOnWith: logger.With("task", req.TaskID, "peer", req.PeerID, "component", "localTaskStore"),
	}
	switch t.StoreStrategy {
	case string(SimpleLocalTaskStoreStrategy):
		t.dataFilePath = path.Join(dataDir, taskData)
	case string(AdvanceLocalTaskStoreStrategy):
		t.dataFilePath = req.Destination
		// just create a new file
		f, err := os.OpenFile(t.dataFilePath, os.O_CREATE|os.O_TRUNC, defaultFileMode)
		if err != nil {
			return err
		}
		f.Close()
		if err = os.MkdirAll(dataDir, defaultDirectoryMode); err != nil {
			return err
		}
		dir := filepath.Dir(req.Destination)
		dirStat, err := os.Stat(dir)
		if err != nil {
			return err
		}
		stat := dirStat.Sys().(*syscall.Stat_t)
		if stat.Dev == s.dataPathStat.Dev {
			// hard link
			if err := os.Link(t.dataFilePath, path.Join(dataDir, taskData)); err != nil {
				return err
			}
		} else {
			// symbol link
			if err := os.Symlink(t.dataFilePath, path.Join(dataDir, taskData)); err != nil {
				return err
			}
		}
	}
	if err := t.init(); err != nil {
		return err
	}
	s.tasks.Store(PeerTaskMetaData{
		PeerID: req.PeerID,
		TaskID: req.TaskID,
	}, t)
	return nil
}

func (s *storageManager) ReloadPersistentTask(gcCallback GCCallback) error {
	dirs, err := ioutil.ReadDir(path.Join(s.storeOption.DataPath, string(s.storeStrategy)))
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	var (
		loadErrs    []error
		loadErrDirs []string
	)
	for _, dir := range dirs {
		taskID := dir.Name()
		peerDirs, err := ioutil.ReadDir(path.Join(s.storeOption.DataPath, string(s.storeStrategy), taskID))
		if err != nil {
			continue
		}
		for _, peerDir := range peerDirs {
			peerID := peerDir.Name()
			dataDir := path.Join(s.storeOption.DataPath, string(s.storeStrategy), taskID, peerID)
			t := &localTaskStore{
				persistentMetadata: persistentMetadata{
					StoreStrategy: string(s.storeStrategy),
					TaskID:        taskID,
					PeerID:        peerID,
					TaskMeta:      map[string]string{},
					Pieces:        map[int32]PieceMetaData{},
				},
				RWMutex:             &sync.RWMutex{},
				dataDir:             dataDir,
				metadataFilePath:    path.Join(dataDir, taskMetaData),
				dataFilePath:        path.Join(dataDir, taskData),
				expireTime:          s.storeOption.TaskExpireTime.Duration,
				lastAccess:          time.Now(),
				gcCallback:          gcCallback,
				SugaredLoggerOnWith: logger.With("task", taskID, "peer", peerID, "component", s.storeStrategy),
			}
			switch t.StoreStrategy {
			case string(SimpleLocalTaskStoreStrategy):
				t.dataFilePath = path.Join(dataDir, taskData)
			case string(AdvanceLocalTaskStoreStrategy):
				// check sym link
				stat, err0 := os.Lstat(path.Join(dataDir, taskData))
				if err0 != nil {
					loadErrs = append(loadErrs, err0)
					loadErrDirs = append(loadErrDirs, dataDir)
					logger.With("action", "reload", "stage", "init", "taskID", taskID, "peerID", peerID).
						Warnf("load task from disk error: %s", err0)
					continue
				}
				// is sym link
				if stat.Mode()&os.ModeSymlink == os.ModeSymlink {
					dest, err0 := os.Readlink(path.Join(dataDir, taskData))
					if err0 != nil {
						loadErrs = append(loadErrs, err0)
						loadErrDirs = append(loadErrDirs, dataDir)
						logger.With("action", "reload", "stage", "init", "taskID", taskID, "peerID", peerID).
							Warnf("load task from disk error: %s", err0)
						continue
					}
					t.dataFilePath = dest
				} else {
					t.dataFilePath = path.Join(dataDir, taskData)
				}
			}
			if err0 := t.init(); err0 != nil {
				loadErrs = append(loadErrs, err0)
				loadErrDirs = append(loadErrDirs, dataDir)
				logger.With("action", "reload", "stage", "init", "taskID", taskID, "peerID", peerID).
					Warnf("load task from disk error: %s", err0)
				continue
			}

			bytes, err0 := ioutil.ReadAll(t.metadataFile)
			if err0 != nil {
				loadErrs = append(loadErrs, err0)
				loadErrDirs = append(loadErrDirs, dataDir)
				logger.With("action", "reload", "stage", "read metadata", "taskID", taskID, "peerID", peerID).
					Warnf("load task from disk error: %s", err0)
				continue
			}

			if err0 = json.Unmarshal(bytes, &t.persistentMetadata); err0 != nil {
				loadErrs = append(loadErrs, err0)
				loadErrDirs = append(loadErrDirs, dataDir)
				logger.With("action", "reload", "stage", "parse metadata", "taskID", taskID, "peerID", peerID).
					Warnf("load task from disk error: %s", err0)
				continue
			}
			logger.Debugf("load task %s/%s metadata from %s",
				t.persistentMetadata.TaskID, t.persistentMetadata.PeerID, t.metadataFilePath)
			s.tasks.Store(PeerTaskMetaData{
				PeerID: peerID,
				TaskID: taskID,
			}, t)
		}
	}
	// remove load error peer tasks
	for _, dir := range loadErrDirs {
		if err = os.Remove(path.Join(dir, taskMetaData)); err != nil {
			logger.Warnf("remove load error file %s error: %s", path.Join(dir, taskMetaData), err)
		}
		logger.Warnf("remove load error file %s ok", path.Join(dir, taskMetaData))

		if err = os.Remove(path.Join(dir, taskData)); err != nil {
			logger.Warnf("remove load error file %s error: %s", path.Join(dir, taskData), err)
		}
		logger.Warnf("remove load error file %s ok", path.Join(dir, taskData))

		if err = os.Remove(dir); err != nil {
			logger.Warnf("remove load error directory %s error: %s", dir, err)
		}
		logger.Warnf("remove load error directory %s ok", dir)
	}
	if len(loadErrs) > 0 {
		var sb strings.Builder
		for _, err := range loadErrs {
			sb.WriteString(err.Error())
		}
		return fmt.Errorf("load tasks from disk error: %q", sb.String())
	}
	return nil
}

func (s *storageManager) TryGC() (bool, error) {
	var markedTasks []PeerTaskMetaData
	s.tasks.Range(func(key, task interface{}) bool {
		// remove from task list first
		if task.(*localTaskStore).CanReclaim() {
			task.(*localTaskStore).MarkReclaim()
			markedTasks = append(markedTasks, key.(PeerTaskMetaData))
		} else {
			logger.Debugf("task %s/%s not reach gc time",
				key.(PeerTaskMetaData).TaskID, key.(PeerTaskMetaData).PeerID)
		}
		return true
	})
	for _, key := range s.markedReclaimTasks {
		task, ok := s.tasks.Load(key)
		if !ok {
			logger.Warnf("task %s/%s marked, but not found", key.TaskID, key.PeerID)
			continue
		}
		s.tasks.Delete(key)
		if err := task.(*localTaskStore).Reclaim(); err != nil {
			// FIXME: retry later or push to queue
			logger.Errorf("gc task %s/%s error: %s", key.TaskID, key.PeerID, err)
			continue
		}
		logger.Infof("task %s/%s reclaimed", key.TaskID, key.PeerID)
	}
	logger.Infof("marked %d task(s), reclaimed %d task(s)", len(markedTasks), len(s.markedReclaimTasks))
	s.markedReclaimTasks = markedTasks
	return true, nil
}

func (s *storageManager) CleanUp() {
	_, _ = s.forceGC()
}

func (s *storageManager) forceGC() (bool, error) {
	s.tasks.Range(func(key, value interface{}) bool {
		s.tasks.Delete(key.(PeerTaskMetaData))
		err := value.(*localTaskStore).Reclaim()
		if err != nil {
			logger.Errorf("gc task store %s error: %s", key, value)
		}
		return true
	})
	return true, nil
}
