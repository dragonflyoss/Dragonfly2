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

//go:generate mockgen -destination mocks/stroage_manager_mock.go -source storage_manager.go -package mocks

package storage

import (
	"context"
	"d7y.io/dragonfly/v2/client/daemon/metrics"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/docker/go-units"
	"github.com/shirou/gopsutil/v3/disk"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	commonv1 "d7y.io/api/v2/pkg/apis/common/v1"
	dfdaemonv1 "d7y.io/api/v2/pkg/apis/dfdaemon/v1"

	"d7y.io/dragonfly/v2/client/config"
	"d7y.io/dragonfly/v2/client/daemon/gc"
	"d7y.io/dragonfly/v2/client/daemon/pex"
	"d7y.io/dragonfly/v2/client/util"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	nethttp "d7y.io/dragonfly/v2/pkg/net/http"
)

type TaskStorageDriver interface {
	// WritePiece put a piece of a task to storage
	WritePiece(ctx context.Context, req *WritePieceRequest) (int64, error)

	// ReadPiece get a piece data reader of a task from storage
	// return a Reader and a Closer from task data with sought, caller should read bytes and close it.
	// If req.Num is equal to -1, range has a fixed value.
	ReadPiece(ctx context.Context, req *ReadPieceRequest) (io.Reader, io.Closer, error)

	ReadAllPieces(ctx context.Context, req *ReadAllPiecesRequest) (io.ReadCloser, error)

	GetPieces(ctx context.Context, req *commonv1.PieceTaskRequest) (*commonv1.PiecePacket, error)

	GetTotalPieces(ctx context.Context, req *PeerTaskMetadata) (int32, error)

	GetExtendAttribute(ctx context.Context, req *PeerTaskMetadata) (*commonv1.ExtendAttribute, error)

	UpdateTask(ctx context.Context, req *UpdateTaskRequest) error

	// Store stores task data to the target path
	Store(ctx context.Context, req *StoreRequest) error

	ValidateDigest(req *PeerTaskMetadata) error

	IsInvalid(req *PeerTaskMetadata) (bool, error)
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
	util.KeepAlive
	// RegisterTask registers a task in storage driver
	RegisterTask(ctx context.Context, req *RegisterTaskRequest) (TaskStorageDriver, error)
	// RegisterSubTask registers a subtask in storage driver
	RegisterSubTask(ctx context.Context, req *RegisterSubTaskRequest) (TaskStorageDriver, error)
	// UnregisterTask unregisters a task in storage driver
	UnregisterTask(ctx context.Context, req CommonTaskRequest) error
	// FindCompletedTask try to find a completed task for fast path
	FindCompletedTask(taskID string) *ReusePeerTask
	// FindCompletedSubTask try to find a completed subtask for fast path
	FindCompletedSubTask(taskID string) *ReusePeerTask
	// FindPartialCompletedTask try to find a partial completed task for fast path
	FindPartialCompletedTask(taskID string, rg *nethttp.Range) *ReusePeerTask
	// CleanUp cleans all storage data
	CleanUp()
	// ListAllPeers return all peers info
	ListAllPeers(perGroupCount int) [][]*dfdaemonv1.PeerMetadata
}

var (
	ErrTaskNotFound     = errors.New("task not found")
	ErrPieceNotFound    = errors.New("piece not found")
	ErrPieceCountNotSet = errors.New("total piece count not set")
	ErrDigestNotSet     = errors.New("digest not set")
	ErrInvalidDigest    = errors.New("invalid digest")
	ErrBadRequest       = errors.New("bad request")
)

const (
	GCName = "StorageManager"
)

var (
	tracer          trace.Tracer
	writeBufferPool *sync.Pool
)

func init() {
	tracer = otel.Tracer("dfget-daemon-gc")
}

type storageManager struct {
	sync.Mutex
	util.KeepAlive
	storeStrategy      config.StoreStrategy
	storeOption        *config.StorageOption
	tasks              sync.Map
	markedReclaimTasks []PeerTaskMetadata
	dataPathStat       *syscall.Stat_t
	gcCallback         func(CommonTaskRequest)
	gcInterval         time.Duration
	dataDirMode        fs.FileMode

	indexRWMutex       sync.RWMutex
	indexTask2PeerTask map[string][]*localTaskStore // key: task id, value: slice of localTaskStore

	subIndexRWMutex       sync.RWMutex
	subIndexTask2PeerTask map[string][]*localSubTaskStore // key: task id, value: slice of localSubTaskStore

	peerSearchBroadcaster pex.PeerSearchBroadcaster
}

var _ gc.GC = (*storageManager)(nil)
var _ Manager = (*storageManager)(nil)

type GCCallback func(request CommonTaskRequest)

func NewStorageManager(storeStrategy config.StoreStrategy, opt *config.StorageOption, gcCallback GCCallback, dirMode fs.FileMode, moreOpts ...func(*storageManager) error) (Manager, error) {
	dataDirMode := defaultDirectoryMode
	// If dirMode isn't in config, use default
	if dirMode != os.FileMode(0) {
		dataDirMode = defaultDirectoryMode
	}
	if !path.IsAbs(opt.DataPath) {
		abs, err := filepath.Abs(opt.DataPath)
		if err != nil {
			return nil, err
		}
		opt.DataPath = abs
	}
	stat, err := os.Stat(opt.DataPath)
	if os.IsNotExist(err) {
		if err := os.MkdirAll(opt.DataPath, dataDirMode); err != nil {
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
		KeepAlive:             util.NewKeepAlive("storage manager"),
		storeStrategy:         storeStrategy,
		storeOption:           opt,
		dataPathStat:          stat.Sys().(*syscall.Stat_t),
		gcCallback:            gcCallback,
		gcInterval:            time.Minute,
		dataDirMode:           dataDirMode,
		indexTask2PeerTask:    map[string][]*localTaskStore{},
		subIndexTask2PeerTask: map[string][]*localSubTaskStore{},
	}

	for _, o := range moreOpts {
		if err := o(s); err != nil {
			return nil, err
		}
	}

	if s.storeOption.ReloadGoroutineCount <= 0 {
		s.storeOption.ReloadGoroutineCount = 64
	}
	s.ReloadPersistentTask(gcCallback)

	gc.Register(GCName, s)
	return s, nil
}

func WithStorageOption(opt *config.StorageOption) func(*storageManager) error {
	return func(manager *storageManager) error {
		manager.storeOption = opt
		return nil
	}
}

func WithGCInterval(gcInterval time.Duration) func(*storageManager) error {
	return func(manager *storageManager) error {
		manager.gcInterval = gcInterval
		return nil
	}
}

func WithWriteBufferSize(size int64) func(*storageManager) error {
	return func(manager *storageManager) error {
		if size > 0 {
			writeBufferPool = &sync.Pool{New: func() any {
				return make([]byte, size)
			}}
		}
		return nil
	}
}

func WithPeerSearchBroadcaster(peerSearchBroadcaster pex.PeerSearchBroadcaster) func(*storageManager) error {
	return func(manager *storageManager) error {
		manager.peerSearchBroadcaster = peerSearchBroadcaster
		return nil
	}
}

func (s *storageManager) RegisterTask(ctx context.Context, req *RegisterTaskRequest) (TaskStorageDriver, error) {
	ts, ok := s.LoadTask(
		PeerTaskMetadata{
			PeerID: req.PeerID,
			TaskID: req.TaskID,
		})
	if ok {
		return s.keepAliveTaskStorageDriver(ts), nil
	}
	// double check if task store exists
	// if ok, just unlock and return
	s.Lock()
	defer s.Unlock()
	if ts, ok = s.LoadTask(
		PeerTaskMetadata{
			PeerID: req.PeerID,
			TaskID: req.TaskID,
		}); ok {
		return s.keepAliveTaskStorageDriver(ts), nil
	}
	// still not exist, create a new task store
	ts, err := s.CreateTask(req)
	if err != nil {
		return nil, err
	}
	return s.keepAliveTaskStorageDriver(ts), err
}

func (s *storageManager) RegisterSubTask(ctx context.Context, req *RegisterSubTaskRequest) (TaskStorageDriver, error) {
	t, ok := s.LoadTask(
		PeerTaskMetadata{
			PeerID: req.Parent.PeerID,
			TaskID: req.Parent.TaskID,
		})
	if !ok {
		return nil, fmt.Errorf("task %s not found", req.Parent.TaskID)
	}

	subtask := t.(*localTaskStore).SubTask(req)
	s.subIndexRWMutex.Lock()
	if ts, ok := s.subIndexTask2PeerTask[req.SubTask.TaskID]; ok {
		ts = append(ts, subtask)
		s.subIndexTask2PeerTask[req.SubTask.TaskID] = ts
	} else {
		s.subIndexTask2PeerTask[req.SubTask.TaskID] = []*localSubTaskStore{subtask}
	}
	s.subIndexRWMutex.Unlock()

	s.Lock()
	s.tasks.Store(
		PeerTaskMetadata{
			PeerID: req.SubTask.PeerID,
			TaskID: req.SubTask.TaskID,
		}, subtask)
	s.Unlock()
	return s.keepAliveTaskStorageDriver(subtask), nil
}

func (s *storageManager) WritePiece(ctx context.Context, req *WritePieceRequest) (int64, error) {
	t, ok := s.LoadTask(
		PeerTaskMetadata{
			PeerID: req.PeerID,
			TaskID: req.TaskID,
		})
	if !ok {
		return 0, ErrTaskNotFound
	}
	return t.WritePiece(ctx, req)
}

func (s *storageManager) ReadPiece(ctx context.Context, req *ReadPieceRequest) (io.Reader, io.Closer, error) {
	t, ok := s.LoadTask(
		PeerTaskMetadata{
			PeerID: req.PeerID,
			TaskID: req.TaskID,
		})
	if !ok {
		// TODO recover for local task persistentMetadata data
		return nil, nil, ErrTaskNotFound
	}
	return t.ReadPiece(ctx, req)
}

func (s *storageManager) ReadAllPieces(ctx context.Context, req *ReadAllPiecesRequest) (io.ReadCloser, error) {
	t, ok := s.LoadTask(
		PeerTaskMetadata{
			PeerID: req.PeerID,
			TaskID: req.TaskID,
		})
	if !ok {
		// TODO recover for local task persistentMetadata data
		return nil, ErrTaskNotFound
	}
	return t.ReadAllPieces(ctx, req)
}

func (s *storageManager) Store(ctx context.Context, req *StoreRequest) error {
	t, ok := s.LoadTask(
		PeerTaskMetadata{
			PeerID: req.PeerID,
			TaskID: req.TaskID,
		})
	if !ok {
		// TODO recover for local task persistentMetadata data
		return ErrTaskNotFound
	}
	return t.Store(ctx, req)
}

func (s *storageManager) GetPieces(ctx context.Context, req *commonv1.PieceTaskRequest) (*commonv1.PiecePacket, error) {
	t, ok := s.LoadTask(
		PeerTaskMetadata{
			TaskID: req.TaskId,
			PeerID: req.DstPid,
		})
	if !ok {
		return nil, ErrTaskNotFound
	}
	return t.GetPieces(ctx, req)
}

func (s *storageManager) GetTotalPieces(ctx context.Context, req *PeerTaskMetadata) (int32, error) {
	t, ok := s.LoadTask(
		PeerTaskMetadata{
			TaskID: req.TaskID,
			PeerID: req.PeerID,
		})
	if !ok {
		return -1, ErrTaskNotFound
	}
	return t.(TaskStorageDriver).GetTotalPieces(ctx, req)
}

func (s *storageManager) GetExtendAttribute(ctx context.Context, req *PeerTaskMetadata) (*commonv1.ExtendAttribute, error) {
	t, ok := s.LoadTask(
		PeerTaskMetadata{
			TaskID: req.TaskID,
			PeerID: req.PeerID,
		})
	if !ok {
		return nil, ErrTaskNotFound
	}
	return t.(TaskStorageDriver).GetExtendAttribute(ctx, req)
}

func (s *storageManager) LoadTask(meta PeerTaskMetadata) (TaskStorageDriver, bool) {
	s.Keep()
	d, ok := s.tasks.Load(meta)
	if !ok {
		return nil, false
	}
	return d.(TaskStorageDriver), ok
}

func (s *storageManager) LoadAndDeleteTask(meta PeerTaskMetadata) (TaskStorageDriver, bool) {
	s.Keep()
	d, ok := s.tasks.LoadAndDelete(meta)
	if !ok {
		return nil, false
	}
	return d.(TaskStorageDriver), ok
}

func (s *storageManager) UpdateTask(ctx context.Context, req *UpdateTaskRequest) error {
	t, ok := s.LoadTask(
		PeerTaskMetadata{
			TaskID: req.TaskID,
			PeerID: req.PeerID,
		})
	if !ok {
		return ErrTaskNotFound
	}
	return t.UpdateTask(ctx, req)
}

func (s *storageManager) CreateTask(req *RegisterTaskRequest) (TaskStorageDriver, error) {
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
			PieceMd5Sign:  req.PieceMd5Sign,
			PeerID:        req.PeerID,
			Pieces:        map[int32]PieceMetadata{},
		},
		gcCallback:       s.gcCallback,
		dataDir:          dataDir,
		metadataFilePath: path.Join(dataDir, taskMetadata),
		expireTime:       s.storeOption.TaskExpireTime.Duration,
		subtasks:         map[PeerTaskMetadata]*localSubTaskStore{},

		SugaredLoggerOnWith: logger.With("task", req.TaskID, "peer", req.PeerID, "component", "localTaskStore"),
	}

	dataDirMode := defaultDirectoryMode
	// If dirMode isn't in config, use default
	if s.dataDirMode != os.FileMode(0) {
		dataDirMode = s.dataDirMode
	}

	if err := os.MkdirAll(t.dataDir, dataDirMode); err != nil && !os.IsExist(err) {
		return nil, err
	}
	t.touch()

	// fallback to simple strategy for proxy
	if req.DesiredLocation == "" {
		t.StoreStrategy = string(config.SimpleLocalTaskStoreStrategy)
	}
	data := path.Join(dataDir, taskData)
	switch t.StoreStrategy {
	case string(config.SimpleLocalTaskStoreStrategy):
		t.DataFilePath = data
		f, err := os.OpenFile(t.DataFilePath, os.O_CREATE|os.O_RDWR, defaultFileMode)
		if err != nil {
			return nil, err
		}
		f.Close()
	case string(config.AdvanceLocalTaskStoreStrategy):
		dir, file := path.Split(req.DesiredLocation)
		dirStat, err := os.Stat(dir)
		if err != nil {
			return nil, err
		}

		t.DataFilePath = path.Join(dir, fmt.Sprintf(".%s.dfget.cache.%s", file, req.PeerID))
		f, err := os.OpenFile(t.DataFilePath, os.O_CREATE|os.O_RDWR, defaultFileMode)
		if err != nil {
			return nil, err
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
					return nil, err
				}
			}
		} else {
			logger.Debugf("different devices, try to symbol link")
			// make symbol link for reload error gc
			if err := os.Symlink(t.DataFilePath, data); err != nil {
				logger.Errorf("symbol link failed: %s", err)
				return nil, err
			}
		}
	}
	s.tasks.Store(
		PeerTaskMetadata{
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
	return t, nil
}

func (s *storageManager) FindCompletedTask(taskID string) *ReusePeerTask {
	s.indexRWMutex.RLock()
	defer s.indexRWMutex.RUnlock()
	ts, ok := s.indexTask2PeerTask[taskID]
	if !ok {
		return nil
	}
	for i := len(ts) - 1; i > -1; i-- {
		t := ts[i]
		if t.invalid.Load() {
			continue
		}
		// touch it before marking reclaim
		t.touch()
		// already marked, skip
		if t.reclaimMarked.Load() {
			continue
		}

		if t.Done {
			return &ReusePeerTask{
				Storage: t,
				PeerTaskMetadata: PeerTaskMetadata{
					PeerID: t.PeerID,
					TaskID: taskID,
				},
				ContentLength: t.ContentLength,
				TotalPieces:   t.TotalPieces,
				Header:        t.Header,
			}
		}
	}
	return nil
}

func (s *storageManager) FindPartialCompletedTask(taskID string, rg *nethttp.Range) *ReusePeerTask {
	s.indexRWMutex.RLock()
	defer s.indexRWMutex.RUnlock()
	ts, ok := s.indexTask2PeerTask[taskID]
	if !ok {
		return nil
	}
	for i := len(ts) - 1; i > -1; i-- {
		t := ts[i]
		if t.invalid.Load() {
			continue
		}
		// touch it before marking reclaim
		t.touch()
		// already marked, skip
		if t.reclaimMarked.Load() {
			continue
		}

		if t.Done || t.partialCompleted(rg) {
			return &ReusePeerTask{
				Storage: t,
				PeerTaskMetadata: PeerTaskMetadata{
					PeerID: t.PeerID,
					TaskID: taskID,
				},
				ContentLength: t.ContentLength,
				TotalPieces:   t.TotalPieces,
				Header:        t.Header,
			}
		}
	}
	return nil
}

func (s *storageManager) FindCompletedSubTask(taskID string) *ReusePeerTask {
	s.subIndexRWMutex.RLock()
	defer s.subIndexRWMutex.RUnlock()
	ts, ok := s.subIndexTask2PeerTask[taskID]
	if !ok {
		return nil
	}
	for i := len(ts) - 1; i > -1; i-- {
		t := ts[i]
		if t.invalid.Load() {
			continue
		}
		// touch it before marking reclaim
		t.parent.touch()
		// already marked, skip
		if t.parent.reclaimMarked.Load() {
			continue
		}

		if !t.Done {
			continue
		}
		return &ReusePeerTask{
			PeerTaskMetadata: PeerTaskMetadata{
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
	if len(remain) > 0 {
		s.indexTask2PeerTask[taskID] = remain
	} else {
		delete(s.indexTask2PeerTask, taskID)
	}
}

func (s *storageManager) cleanSubIndex(taskID, peerID string) {
	s.subIndexRWMutex.Lock()
	defer s.subIndexRWMutex.Unlock()
	ts, ok := s.subIndexTask2PeerTask[taskID]
	if !ok {
		return
	}
	var remain []*localSubTaskStore
	// FIXME switch instead copy
	for _, t := range ts {
		if t.PeerID == peerID {
			logger.Debugf("clean index for %s/%s", taskID, peerID)
			continue
		}
		remain = append(remain, t)
	}
	if len(remain) > 0 {
		s.subIndexTask2PeerTask[taskID] = remain
	} else {
		delete(s.subIndexTask2PeerTask, taskID)
	}
}

func (s *storageManager) ValidateDigest(req *PeerTaskMetadata) error {
	t, ok := s.LoadTask(
		PeerTaskMetadata{
			TaskID: req.TaskID,
			PeerID: req.PeerID,
		})
	if !ok {
		return ErrTaskNotFound
	}
	return t.ValidateDigest(req)
}

func (s *storageManager) IsInvalid(req *PeerTaskMetadata) (bool, error) {
	t, ok := s.LoadTask(
		PeerTaskMetadata{
			TaskID: req.TaskID,
			PeerID: req.PeerID,
		})
	if !ok {
		return false, ErrTaskNotFound
	}
	return t.IsInvalid(req)
}

func (s *storageManager) ReloadPersistentTask(gcCallback GCCallback) {
	entries, err := os.ReadDir(s.storeOption.DataPath)
	if os.IsNotExist(err) {
		return
	}
	if err != nil {
		return
	}

	var dirs []os.DirEntry
	for _, entry := range entries {
		if entry.IsDir() {
			dirs = append(dirs, entry)
		}
	}
	count := len(dirs)
	if count == 0 {
		return
	}

	wg := &sync.WaitGroup{}
	wg.Add(len(dirs))
	dirCh := make(chan string, 1000)
	done := make(chan struct{})

	reloadGoroutineCount := s.storeOption.ReloadGoroutineCount
	if count < reloadGoroutineCount {
		reloadGoroutineCount = count
	}

	for i := 0; i < reloadGoroutineCount; i++ {
		go func() {
			for {
				select {
				case taskID := <-dirCh:
					s.reloadPersistentTaskByTaskDir(gcCallback, taskID)
					wg.Done()
				case <-done:
					return
				}
			}
		}()
	}

	logger.Infof("start to reload task data from disk, count: %d", len(dirs))

	start := time.Now()
	for _, dir := range dirs {
		dirCh <- dir.Name()
	}
	wg.Wait()

	logger.Infof("reload task data from disk done, cost: %dms", time.Now().Sub(start).Milliseconds())
	close(done)
}

func (s *storageManager) reloadPersistentTaskByTaskDir(gcCallback GCCallback, taskID string) {
	taskDir := path.Join(s.storeOption.DataPath, taskID)
	peerDirs, err := os.ReadDir(taskDir)
	if err != nil {
		logger.Errorf("read dir %s error: %s", taskDir, err)
		return
	}

	var loadErrDirs []string
	for _, peer := range peerDirs {
		peerID := peer.Name()
		loadErr := s.reloadPersistentTaskByPeerDir(gcCallback, taskID, peerID)
		if loadErr != nil {
			loadErrDirs = append(loadErrDirs, path.Join(s.storeOption.DataPath, taskID, peerID))
		}
	}

	if len(loadErrDirs) > 0 {
		s.removeErrorPeers(loadErrDirs)
	}
	// remove empty task dir
	if len(peerDirs) == 0 || len(loadErrDirs) == len(peerDirs) {
		// skip dot files or directories
		if !strings.HasPrefix(taskDir, ".") {
			if err := os.Remove(taskDir); err != nil {
				logger.Errorf("remove empty task dir %s failed: %s", taskDir, err)
			} else {
				logger.Infof("remove empty task dir %s", taskDir)
			}
		}
	}
}

func (s *storageManager) reloadPersistentTaskByPeerDir(gcCallback GCCallback, taskID, peerID string) error {
	dataDir := path.Join(s.storeOption.DataPath, taskID, peerID)
	t := &localTaskStore{
		dataDir:             dataDir,
		metadataFilePath:    path.Join(dataDir, taskMetadata),
		expireTime:          s.storeOption.TaskExpireTime.Duration,
		gcCallback:          gcCallback,
		SugaredLoggerOnWith: logger.With("task", taskID, "peer", peerID, "component", s.storeStrategy),
	}
	t.touch()

	bytes, err := os.ReadFile(t.metadataFilePath)
	if err != nil {
		logger.With("action", "reload", "stage", "read metadata", "taskID", taskID, "peerID", peerID).
			Warnf("load task metadata from disk error: %s", err)
		return err
	}

	if err = json.Unmarshal(bytes, &t.persistentMetadata); err != nil {
		logger.With("action", "reload", "stage", "parse metadata", "taskID", taskID, "peerID", peerID).
			Warnf("load task from disk error: %s, data base64 encode: %s", err, base64.StdEncoding.EncodeToString(bytes))
		return err
	}
	logger.Debugf("load task %s/%s from disk, metadata %s, last access: %v, expire time: %s",
		t.persistentMetadata.TaskID, t.persistentMetadata.PeerID, t.metadataFilePath, time.Unix(0, t.lastAccess.Load()), t.expireTime)
	s.tasks.Store(PeerTaskMetadata{
		PeerID: peerID,
		TaskID: taskID,
	}, t)

	// update index
	s.indexRWMutex.Lock()
	if ts, ok := s.indexTask2PeerTask[taskID]; ok {
		ts = append(ts, t)
		s.indexTask2PeerTask[taskID] = ts
	} else {
		s.indexTask2PeerTask[taskID] = []*localTaskStore{t}
	}
	s.indexRWMutex.Unlock()
	return nil
}

func (s *storageManager) removeErrorPeers(loadErrDirs []string) {
	// remove load error peer tasks
	for _, dir := range loadErrDirs {
		// remove metadata
		if err := os.Remove(path.Join(dir, taskMetadata)); err != nil {
			logger.Warnf("remove load error file %s error: %s", path.Join(dir, taskMetadata), err)
		} else {
			logger.Warnf("remove load error file %s ok", path.Join(dir, taskMetadata))
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
}

func (s *storageManager) TryGC() (bool, error) {
	// FIXME gc subtask
	var markedTasks []PeerTaskMetadata
	var totalNotMarkedSize int64
	s.tasks.Range(func(key, task any) bool {
		if task.(Reclaimer).CanReclaim() {
			task.(Reclaimer).MarkReclaim()
			markedTasks = append(markedTasks, key.(PeerTaskMetadata))
		} else {
			lts, ok := task.(*localTaskStore)
			if ok {
				// just calculate not reclaimed task
				totalNotMarkedSize += lts.ContentLength
				// TODO add a option to avoid print log too frequently
				// logger.Debugf("task %s/%s not reach gc time",
				//	key.(PeerTaskMetadata).TaskID, key.(PeerTaskMetadata).PeerID)
			}
		}
		return true
	})
	quotaBytesExceed := totalNotMarkedSize - int64(s.storeOption.DiskGCThreshold)
	quotaExceed := s.storeOption.DiskGCThreshold > 0 && quotaBytesExceed > 0

	metrics.DataUnReclaimedUsage.Set(float64(totalNotMarkedSize))
	metrics.DataDiskGCThreshold.Set(float64(s.storeOption.DiskGCThreshold))
	metrics.DataDiskGCThresholdPercent.Set(s.storeOption.DiskGCThresholdPercent)

	usage := s.diskUsage()
	if usage != nil {
		metrics.DataDiskUsage.Set(float64(usage.Used))
		metrics.DataDiskCapacity.Set(float64(usage.Total))
	}

	usageExceed, usageBytesExceed := s.diskUsageExceed(usage)

	if quotaExceed || usageExceed {
		var bytesExceed int64
		// only use quotaBytesExceed when s.storeOption.DiskGCThreshold > 0
		if s.storeOption.DiskGCThreshold > 0 && quotaBytesExceed > usageBytesExceed {
			bytesExceed = quotaBytesExceed
		} else {
			bytesExceed = usageBytesExceed
		}
		logger.Infof("quota threshold reached, start gc oldest task, size: %d bytes", bytesExceed)
		var tasks []*localTaskStore
		s.tasks.Range(func(key, val any) bool {
			// skip reclaimed task
			task, ok := val.(*localTaskStore)
			if !ok { // skip subtask
				return true
			}
			if task.reclaimMarked.Load() {
				return true
			}
			// task is not done, and is active in s.gcInterval
			// next gc loop will check it again
			if !task.Done && time.Since(time.Unix(0, task.lastAccess.Load())) < s.gcInterval {
				return true
			}
			tasks = append(tasks, task)
			return true
		})
		// sort by access time
		sort.SliceStable(tasks, func(i, j int) bool {
			return tasks[i].lastAccess.Load() < tasks[j].lastAccess.Load()
		})
		for _, task := range tasks {
			task.MarkReclaim()
			markedTasks = append(markedTasks, PeerTaskMetadata{task.PeerID, task.TaskID})
			logger.Infof("quota threshold reached, mark task %s/%s reclaimed, last access: %s, size: %s",
				task.TaskID, task.PeerID, time.Unix(0, task.lastAccess.Load()).Format(time.RFC3339Nano),
				units.BytesSize(float64(task.ContentLength)))
			bytesExceed -= task.ContentLength
			if bytesExceed <= 0 {
				break
			}
		}
		if bytesExceed > 0 {
			logger.Warnf("no enough tasks to gc, remind %d bytes", bytesExceed)
		}
	}

	// broadcast delete peer event
	if s.peerSearchBroadcaster != nil {
		for _, task := range markedTasks {
			s.peerSearchBroadcaster.BroadcastPeer(&dfdaemonv1.PeerMetadata{
				TaskId: task.TaskID,
				PeerId: task.PeerID,
				State:  dfdaemonv1.PeerState_Deleted,
			})
		}
	}

	for _, key := range s.markedReclaimTasks {
		t, ok := s.tasks.Load(key)
		if !ok {
			continue
		}
		_, span := tracer.Start(context.Background(), config.SpanPeerGC)
		s.tasks.Delete(key)

		if lts, ok := t.(*localTaskStore); ok {
			span.SetAttributes(config.AttributePeerID.String(lts.PeerID))
			span.SetAttributes(config.AttributeTaskID.String(lts.TaskID))
			s.cleanIndex(lts.TaskID, lts.PeerID)
		} else {
			task := t.(*localSubTaskStore)
			span.SetAttributes(config.AttributePeerID.String(task.PeerID))
			span.SetAttributes(config.AttributeTaskID.String(task.TaskID))
			s.cleanSubIndex(task.TaskID, task.PeerID)
		}

		if err := t.(Reclaimer).Reclaim(); err != nil {
			// FIXME: retry later or push to queue
			logger.Errorf("gc task %s/%s error: %s", key.TaskID, key.PeerID, err)
			span.RecordError(err)
			span.End()
			continue
		}
		logger.Infof("task %s/%s reclaimed", key.TaskID, key.PeerID)
		// remove reclaimed task in markedTasks
		for i, k := range markedTasks {
			if k.TaskID == key.TaskID && k.PeerID == key.PeerID {
				markedTasks = append(markedTasks[:i], markedTasks[i+1:]...)
				break
			}
		}
		span.End()
	}
	logger.Infof("marked %d task(s), reclaimed %d task(s)", len(markedTasks), len(s.markedReclaimTasks))
	s.markedReclaimTasks = markedTasks
	return true, nil
}

// delete the given task from local storage and unregister it from scheduler.
func (s *storageManager) deleteTask(meta PeerTaskMetadata) error {
	task, ok := s.LoadAndDeleteTask(meta)
	if !ok {
		logger.Warnf("deleteTask: task meta not found: %v", meta)
		return nil
	}

	// broadcast delete peer event
	if s.peerSearchBroadcaster != nil {
		s.peerSearchBroadcaster.BroadcastPeer(&dfdaemonv1.PeerMetadata{
			TaskId: meta.TaskID,
			PeerId: meta.PeerID,
			State:  dfdaemonv1.PeerState_Deleted,
		})
	}

	logger.Debugf("deleteTask: deleting task: %v", meta)
	if _, ok := task.(*localTaskStore); ok {
		s.cleanIndex(meta.TaskID, meta.PeerID)
	} else {
		s.cleanSubIndex(meta.TaskID, meta.PeerID)
	}
	// MarkReclaim() will call gcCallback, which will unregister task from scheduler
	task.(Reclaimer).MarkReclaim()
	return task.(Reclaimer).Reclaim()
}

func (s *storageManager) UnregisterTask(ctx context.Context, req CommonTaskRequest) error {
	return s.deleteTask(PeerTaskMetadata{
		TaskID: req.TaskID,
		PeerID: req.PeerID,
	})
}

func (s *storageManager) CleanUp() {
	_, _ = s.forceGC()
}

func (s *storageManager) forceGC() (bool, error) {
	s.tasks.Range(func(key, task any) bool {
		meta := key.(PeerTaskMetadata)
		err := s.deleteTask(meta)
		if err != nil {
			logger.Errorf("gc task store %s error: %s", key, err)
		}
		return true
	})
	return true, nil
}

func (s *storageManager) diskUsage() *disk.UsageStat {
	usage, err := disk.Usage(s.storeOption.DataPath)
	if err != nil {
		logger.Warnf("get %s disk usage error: %s", s.storeOption.DataPath, err)
		return nil
	}
	return usage
}

func (s *storageManager) diskUsageExceed(usage *disk.UsageStat) (exceed bool, bytes int64) {
	if s.storeOption.DiskGCThresholdPercent <= 0 {
		return false, 0
	}
	if usage == nil {
		return false, 0
	}
	logger.Debugf("disk usage: %+v", usage)
	if usage.UsedPercent < s.storeOption.DiskGCThresholdPercent {
		return false, 0
	}

	bs := (usage.UsedPercent - s.storeOption.DiskGCThresholdPercent) * float64(usage.Total) / 100.0
	logger.Infof("disk used percent %f, exceed threshold percent %f, %d bytes to reclaim",
		usage.UsedPercent, s.storeOption.DiskGCThresholdPercent, int64(bs))
	return true, int64(bs)
}

type onlyWriter struct {
	io.Writer
}

func tryWriteWithBuffer(writer io.Writer, reader io.Reader, readSize int64) (written int64, err error) {
	if writeBufferPool != nil {
		buf := writeBufferPool.Get().([]byte)
		// skip io.ReadFrom logic in io.CopyBuffer, force to use buffer
		written, err = io.CopyBuffer(onlyWriter{writer}, io.LimitReader(reader, readSize), buf)
		//nolint:all
		writeBufferPool.Put(buf)
	} else {
		written, err = io.Copy(writer, io.LimitReader(reader, readSize))
	}
	return written, err
}

func (s *storageManager) ListAllPeers(perGroupCount int) [][]*dfdaemonv1.PeerMetadata {
	s.indexRWMutex.RLock()
	defer s.indexRWMutex.RUnlock()

	if perGroupCount == 0 {
		perGroupCount = 1000
	}

	allPeers := make([][]*dfdaemonv1.PeerMetadata, 0, len(s.indexTask2PeerTask)/perGroupCount)
	peers := make([]*dfdaemonv1.PeerMetadata, 0, perGroupCount)

	for taskID, task := range s.indexTask2PeerTask {
		for _, peer := range task {
			peers = append(peers, &dfdaemonv1.PeerMetadata{
				TaskId: taskID,
				PeerId: peer.PeerID,
				State:  dfdaemonv1.PeerState_Success,
			})

			if len(peers) == perGroupCount {
				allPeers = append(allPeers, peers)
				peers = make([]*dfdaemonv1.PeerMetadata, 0, perGroupCount)
			}
		}
	}

	if len(peers) > 0 {
		allPeers = append(allPeers, peers)
	}
	return allPeers
}

func (s *storageManager) keepAliveTaskStorageDriver(ts TaskStorageDriver) TaskStorageDriver {
	return &keepAliveTaskStorageDriver{
		KeepAlive:         s,
		TaskStorageDriver: ts,
	}
}
