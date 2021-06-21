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
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/docker/go-units"
	"github.com/pkg/errors"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	"d7y.io/dragonfly/v2/client/clientutil"
	"d7y.io/dragonfly/v2/client/config"
	"d7y.io/dragonfly/v2/client/daemon/gc"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/internal/rpc/base"
)

type TaskStorageDriver interface {
	// WritePiece put a piece of a task to storage
	WritePiece(ctx context.Context, req *WritePieceRequest) (int64, error)

	// ReadPiece get a piece data reader of a task from storage
	// return a Reader and a Closer from task data with seeked, caller should read bytes and close it.
	ReadPiece(ctx context.Context, req *ReadPieceRequest) (io.Reader, io.Closer, error)

	ReadAllPieces(ctx context.Context, req *PeerTaskMetaData) (io.ReadCloser, error)

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
	// FindCompletedTask try to find a completed task for fast path
	FindCompletedTask(taskID string) *ReusePeerTask
	// CleanUp cleans all storage data
	CleanUp()
}

var (
	ErrTaskNotFound  = errors.New("task not found")
	ErrPieceNotFound = errors.New("piece not found")
)

const (
	GCName = "StorageManager"
)

var tracer trace.Tracer

func init() {
	tracer = otel.Tracer("dfget-daemon-gc")
}

type storageManager struct {
	sync.Mutex
	clientutil.KeepAlive
	storeStrategy      config.StoreStrategy
	storeOption        *config.StorageOption
	tasks              sync.Map
	markedReclaimTasks []PeerTaskMetaData
	dataPathStat       *syscall.Stat_t
	gcCallback         func(CommonTaskRequest)

	indexRWMutex       sync.RWMutex
	indexTask2PeerTask map[string][]*localTaskStore // key: task id, value: slice of localTaskStore
}

var _ gc.GC = (*storageManager)(nil)
var _ Manager = (*storageManager)(nil)

type GCCallback func(request CommonTaskRequest)

func NewStorageManager(storeStrategy config.StoreStrategy, opt *config.StorageOption, gcCallback GCCallback, moreOpts ...func(*storageManager) error) (Manager, error) {
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
	case config.SimpleLocalTaskStoreStrategy, config.AdvanceLocalTaskStoreStrategy:
	case config.StoreStrategy(""):
		storeStrategy = config.SimpleLocalTaskStoreStrategy
	default:
		return nil, fmt.Errorf("not support store strategy: %s", storeStrategy)
	}

	s := &storageManager{
		KeepAlive:     clientutil.NewKeepAlive("storage manager"),
		storeStrategy: storeStrategy,
		storeOption:   opt,
		dataPathStat:  stat.Sys().(*syscall.Stat_t),
		gcCallback:    gcCallback,

		indexTask2PeerTask: map[string][]*localTaskStore{},
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

func WithStorageOption(opt *config.StorageOption) func(*storageManager) error {
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

func (s *storageManager) ReadAllPieces(ctx context.Context, req *PeerTaskMetaData) (io.ReadCloser, error) {
	t, ok := s.LoadTask(
		PeerTaskMetaData{
			PeerID: req.PeerID,
			TaskID: req.TaskID,
		})
	if !ok {
		// TODO recover for local task persistentMetadata data
		return nil, ErrTaskNotFound
	}
	return t.(TaskStorageDriver).ReadAllPieces(ctx, req)
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
	s.Keep()
	logger.Debugf("init local task storage, peer id: %s, task id: %s", req.PeerID, req.TaskID)

	dataDir := path.Join(s.storeOption.DataPath, req.TaskID, req.PeerID)
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
		dataDir:          dataDir,
		metadataFilePath: path.Join(dataDir, taskMetaData),
		expireTime:       s.storeOption.TaskExpireTime.Duration,

		SugaredLoggerOnWith: logger.With("task", req.TaskID, "peer", req.PeerID, "component", "localTaskStore"),
	}
	if err := os.MkdirAll(t.dataDir, defaultDirectoryMode); err != nil && !os.IsExist(err) {
		return err
	}
	t.touch()
	metadata, err := os.OpenFile(t.metadataFilePath, os.O_CREATE|os.O_RDWR, defaultFileMode)
	if err != nil {
		return err
	}
	t.metadataFile = metadata

	// fallback to simple strategy for proxy
	if req.Destination == "" {
		t.StoreStrategy = string(config.SimpleLocalTaskStoreStrategy)
	}
	data := path.Join(dataDir, taskData)
	switch t.StoreStrategy {
	case string(config.SimpleLocalTaskStoreStrategy):
		t.DataFilePath = data
		f, err := os.OpenFile(t.DataFilePath, os.O_CREATE|os.O_RDWR, defaultFileMode)
		if err != nil {
			return err
		}
		f.Close()
	case string(config.AdvanceLocalTaskStoreStrategy):
		dir, file := path.Split(req.Destination)
		dirStat, err := os.Stat(dir)
		if err != nil {
			return err
		}

		t.DataFilePath = path.Join(dir, fmt.Sprintf(".%s.dfget.cache.%s", file, req.PeerID))
		f, err := os.OpenFile(t.DataFilePath, os.O_CREATE|os.O_RDWR, defaultFileMode)
		if err != nil {
			return err
		}
		f.Close()

		stat := dirStat.Sys().(*syscall.Stat_t)
		// same dev, can hard link
		if stat.Dev == s.dataPathStat.Dev {
			logger.Debugf("same device, try to hard link")
			if err := os.Link(t.DataFilePath, data); err != nil {
				logger.Warnf("hard link failed for same device: %s, fallback to symbol link", err)
				// fallback to symbol link
				if err := os.Symlink(t.DataFilePath, data); err != nil {
					logger.Errorf("symbol link failed: %s", err)
					return err
				}
			}
		} else {
			logger.Debugf("different devices, try to symbol link")
			// make symbol link for reload error gc
			if err := os.Symlink(t.DataFilePath, data); err != nil {
				logger.Errorf("symbol link failed: %s", err)
				return err
			}
		}
	}
	s.tasks.Store(
		PeerTaskMetaData{
			PeerID: req.PeerID,
			TaskID: req.TaskID,
		}, t)

	s.indexRWMutex.Lock()
	if ts, ok := s.indexTask2PeerTask[req.TaskID]; ok {
		ts = append(ts, t)
		s.indexTask2PeerTask[req.TaskID] = ts
	} else {
		s.indexTask2PeerTask[req.TaskID] = []*localTaskStore{t}
	}
	s.indexRWMutex.Unlock()
	return nil
}

func (s *storageManager) FindCompletedTask(taskID string) *ReusePeerTask {
	s.indexRWMutex.RLock()
	defer s.indexRWMutex.RUnlock()
	ts, ok := s.indexTask2PeerTask[taskID]
	if !ok {
		return nil
	}
	for _, t := range ts {
		// touch it before marking reclaim
		t.touch()
		// already marked, skip
		if t.reclaimMarked.Load() {
			continue
		}

		if !t.Done {
			continue
		}
		return &ReusePeerTask{
			PeerTaskMetaData: PeerTaskMetaData{
				PeerID: t.PeerID,
				TaskID: taskID,
			},
			ContentLength: t.ContentLength,
			TotalPieces:   t.TotalPieces,
		}
	}
	return nil
}

func (s *storageManager) cleanIndex(taskID, peerID string) {
	s.indexRWMutex.Lock()
	defer s.indexRWMutex.Unlock()
	ts, ok := s.indexTask2PeerTask[taskID]
	if !ok {
		return
	}
	var remain []*localTaskStore
	// FIXME switch instead copy
	for _, t := range ts {
		if t.PeerID == peerID {
			logger.Debugf("clean index for %s/%s", taskID, peerID)
			continue
		}
		remain = append(remain, t)
	}
	s.indexTask2PeerTask[taskID] = remain
}

func (s *storageManager) ReloadPersistentTask(gcCallback GCCallback) error {
	dirs, err := ioutil.ReadDir(s.storeOption.DataPath)
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
		peerDirs, err := ioutil.ReadDir(path.Join(s.storeOption.DataPath, taskID))
		if err != nil {
			continue
		}
		for _, peerDir := range peerDirs {
			peerID := peerDir.Name()
			dataDir := path.Join(s.storeOption.DataPath, taskID, peerID)
			t := &localTaskStore{
				dataDir:             dataDir,
				metadataFilePath:    path.Join(dataDir, taskMetaData),
				expireTime:          s.storeOption.TaskExpireTime.Duration,
				gcCallback:          gcCallback,
				SugaredLoggerOnWith: logger.With("task", taskID, "peer", peerID, "component", s.storeStrategy),
			}
			t.touch()

			if t.metadataFile, err = os.Open(t.metadataFilePath); err != nil {
				loadErrs = append(loadErrs, err)
				loadErrDirs = append(loadErrDirs, dataDir)
				logger.With("action", "reload", "stage", "read metadata", "taskID", taskID, "peerID", peerID).
					Warnf("open task metadata error: %s", err)
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
			logger.Debugf("load task %s/%s from disk, metadata %s",
				t.persistentMetadata.TaskID, t.persistentMetadata.PeerID, t.metadataFilePath)
			s.tasks.Store(PeerTaskMetaData{
				PeerID: peerID,
				TaskID: taskID,
			}, t)

			// update index
			if ts, ok := s.indexTask2PeerTask[taskID]; ok {
				ts = append(ts, t)
				s.indexTask2PeerTask[taskID] = ts
			} else {
				s.indexTask2PeerTask[taskID] = []*localTaskStore{t}
			}
		}
	}
	// remove load error peer tasks
	for _, dir := range loadErrDirs {
		// remove metadata
		if err = os.Remove(path.Join(dir, taskMetaData)); err != nil {
			logger.Warnf("remove load error file %s error: %s", path.Join(dir, taskMetaData), err)
		} else {
			logger.Warnf("remove load error file %s ok", path.Join(dir, taskMetaData))
		}

		// remove data
		data := path.Join(dir, taskData)
		stat, err := os.Lstat(data)
		if err == nil {
			// remove sym link file
			if stat.Mode()&os.ModeSymlink == os.ModeSymlink {
				dest, err0 := os.Readlink(data)
				if err0 == nil {
					if err = os.Remove(dest); err != nil {
						logger.Warnf("remove load error file %s error: %s", data, err)
					}
				}
			}
			if err = os.Remove(data); err != nil {
				logger.Warnf("remove load error file %s error: %s", data, err)
			} else {
				logger.Warnf("remove load error file %s ok", data)
			}
		}

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
	var totalNotMarkedSize int64
	s.tasks.Range(func(key, task interface{}) bool {
		if task.(*localTaskStore).CanReclaim() {
			task.(*localTaskStore).MarkReclaim()
			markedTasks = append(markedTasks, key.(PeerTaskMetaData))
		} else {
			// just calculate not reclaimed task
			totalNotMarkedSize += task.(*localTaskStore).ContentLength
			logger.Debugf("task %s/%s not reach gc time",
				key.(PeerTaskMetaData).TaskID, key.(PeerTaskMetaData).PeerID)
		}
		return true
	})

	if s.storeOption.DiskGCThreshold > 0 && totalNotMarkedSize > int64(s.storeOption.DiskGCThreshold) {
		logger.Infof("quota threshold reached, start gc oldest task")
		var tasks []*localTaskStore
		s.tasks.Range(func(key, task interface{}) bool {
			// skip reclaimed task
			if task.(*localTaskStore).reclaimMarked.Load() {
				return true
			}
			tasks = append(tasks, task.(*localTaskStore))
			return true
		})
		sort.SliceStable(tasks, func(i, j int) bool {
			return tasks[i].lastAccess.Load() < tasks[j].lastAccess.Load()
		})
		for _, task := range tasks {
			task.MarkReclaim()
			markedTasks = append(markedTasks, PeerTaskMetaData{task.PeerID, task.TaskID})
			logger.Infof("quota threshold reached, mark task %s/%s reclaimed, last access: %s, size: %s",
				task.TaskID, task.PeerID, time.Unix(0, task.lastAccess.Load()).Format(time.RFC3339Nano),
				units.BytesSize(float64(task.ContentLength)))
			totalNotMarkedSize -= task.ContentLength
			if totalNotMarkedSize < int64(s.storeOption.DiskGCThreshold) {
				break
			}
		}
	}

	for _, key := range s.markedReclaimTasks {
		t, ok := s.tasks.Load(key)
		if !ok {
			logger.Warnf("task %s/%s marked, but not found", key.TaskID, key.PeerID)
			continue
		}
		task := t.(*localTaskStore)
		_, span := tracer.Start(context.Background(), config.SpanPeerGC)
		span.SetAttributes(config.AttributePeerID.String(task.PeerID))
		span.SetAttributes(config.AttributeTaskID.String(task.TaskID))

		s.tasks.Delete(key)
		s.cleanIndex(task.TaskID, task.PeerID)
		if err := task.Reclaim(); err != nil {
			// FIXME: retry later or push to queue
			logger.Errorf("gc task %s/%s error: %s", key.TaskID, key.PeerID, err)
			span.RecordError(err)
			span.End()
			continue
		}
		logger.Infof("task %s/%s reclaimed", key.TaskID, key.PeerID)

		span.End()
	}
	logger.Infof("marked %d task(s), reclaimed %d task(s)", len(markedTasks), len(s.markedReclaimTasks))
	s.markedReclaimTasks = markedTasks
	return true, nil
}

func (s *storageManager) CleanUp() {
	_, _ = s.forceGC()
}

func (s *storageManager) forceGC() (bool, error) {
	s.tasks.Range(func(key, task interface{}) bool {
		meta := key.(PeerTaskMetaData)
		s.tasks.Delete(meta)
		s.cleanIndex(meta.TaskID, meta.PeerID)
		task.(*localTaskStore).MarkReclaim()
		err := task.(*localTaskStore).Reclaim()
		if err != nil {
			logger.Errorf("gc task store %s error: %s", key, err)
		}
		return true
	})
	return true, nil
}
