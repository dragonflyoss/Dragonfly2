package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/dragonflyoss/Dragonfly2/client/daemon/gc"
	logger "github.com/dragonflyoss/Dragonfly2/pkg/dflog"
	"github.com/dragonflyoss/Dragonfly2/pkg/rpc/base"
)

type simpleLocalTaskStore struct {
	persistentPeerTaskMetadata

	lock sync.Locker

	dataDir string

	metadataFile     *os.File
	metadataFilePath string
	dataFile         *os.File
	dataFilePath     string

	expireTime time.Duration
	lastAccess time.Time
}

func init() {
	Register(SimpleLocalTaskStoreDriver, NewSimpleLocalTaskStoreExecutor)
}

type simpleLocalTaskStoreExecutor struct {
	tasks *sync.Map
	opt   *Option
}

func NewSimpleLocalTaskStoreExecutor(opt *Option) (TaskStorageExecutor, error) {
	return simpleLocalTaskStoreExecutor{
		tasks: &sync.Map{},
		opt:   opt,
	}, nil
}

func (e simpleLocalTaskStoreExecutor) LoadTask(taskID string, _ string) (TaskStorageDriver, bool) {
	d, ok := e.tasks.Load(taskID)
	if !ok {
		return nil, false
	}
	return d.(TaskStorageDriver), ok
}

func (e simpleLocalTaskStoreExecutor) CreateTask(taskID string, peerID string, dest string) error {
	logger.Debugf("init local task storage, peer id: %s, task id: %s", peerID, taskID)

	dataDir := path.Join(e.opt.DataPath, string(SimpleLocalTaskStoreDriver), taskID)
	t := &simpleLocalTaskStore{
		persistentPeerTaskMetadata: persistentPeerTaskMetadata{
			TaskID:   taskID,
			TaskMeta: map[string]string{},
			PeerID:   peerID,
			Pieces:   map[int32]PieceMetaData{},
		},
		lock:             &sync.Mutex{},
		dataDir:          dataDir,
		metadataFilePath: path.Join(dataDir, taskMetaData),
		dataFilePath:     path.Join(dataDir, taskData),
		expireTime:       e.opt.TaskExpireTime,
	}
	if err := t.init(); err != nil {
		return err
	}
	e.tasks.Store(taskID, t)
	return nil
}

func (e simpleLocalTaskStoreExecutor) ReloadPersistentTask() error {
	dirs, err := ioutil.ReadDir(path.Join(e.opt.DataPath, string(SimpleLocalTaskStoreDriver)))
	if err != nil {
		return err
	}
	var loadErrs []error
	for _, dir := range dirs {
		taskID := dir.Name()
		dataDir := path.Join(e.opt.DataPath, string(SimpleLocalTaskStoreDriver), taskID)
		t := &simpleLocalTaskStore{
			persistentPeerTaskMetadata: persistentPeerTaskMetadata{
				TaskID:   taskID,
				TaskMeta: map[string]string{},
				Pieces:   map[int32]PieceMetaData{},
			},
			lock:             &sync.Mutex{},
			dataDir:          dataDir,
			metadataFilePath: path.Join(dataDir, taskMetaData),
			dataFilePath:     path.Join(dataDir, taskData),
			expireTime:       e.opt.TaskExpireTime,
			lastAccess:       time.Now(),
		}
		if err0 := t.init(); err0 != nil {
			loadErrs = append(loadErrs, err0)
			logger.With("action", "reload", "stage", "init", "taskID", taskID).
				Warnf("load task from disk error: %s", err0)
			continue
		}

		bytes, err0 := ioutil.ReadAll(t.metadataFile)
		if err0 != nil {
			loadErrs = append(loadErrs, err0)
			logger.With("action", "reload", "stage", "read metadata", "taskID", taskID).
				Warnf("load task from disk error: %s", err0)
			continue
		}

		if err0 = json.Unmarshal(bytes, &t.persistentPeerTaskMetadata); err0 != nil {
			loadErrs = append(loadErrs, err0)
			logger.With("action", "reload", "stage", "parse metadata", "taskID", taskID).
				Warnf("load task from disk error: %s", err0)
			continue
		}
		e.tasks.Store(taskID, t)
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

func (e simpleLocalTaskStoreExecutor) TryGC() (bool, error) {
	var tasks []string
	e.tasks.Range(func(key, value interface{}) bool {
		ok, err := value.(gc.GC).TryGC()
		if err != nil {
			logger.Errorf("gc task store %s error: %s", key, value)
		}
		if ok {
			tasks = append(tasks, key.(string))
			logger.Infof("gc task store %s ok", key)
		}
		return true
	})
	for _, task := range tasks {
		e.tasks.Delete(task)
	}
	return true, nil
}

func (t *simpleLocalTaskStore) init() error {
	if err := os.MkdirAll(t.dataDir, defaultDirectoryMode); err != nil && !os.IsExist(err) {
		return err
	}
	metadata, err := os.OpenFile(t.metadataFilePath, os.O_CREATE|os.O_RDWR, defaultFileMode)
	if err != nil {
		return err
	}
	t.metadataFile = metadata

	data, err := os.OpenFile(t.dataFilePath, os.O_CREATE|os.O_RDWR, defaultFileMode)
	if err != nil {
		return err
	}
	t.dataFile = data
	return nil
}

func (t *simpleLocalTaskStore) touch() {
	t.lastAccess = time.Now()
}

func (t *simpleLocalTaskStore) WritePiece(ctx context.Context, req *WritePieceRequest) error {
	t.touch()

	// TODO check piece if already exists

	// FIXME dup fd to remove lock
	t.lock.Lock()
	defer t.lock.Unlock()
	if _, err := t.dataFile.Seek(req.Range.Start, 0); err != nil {
		return err
	}
	_, err := io.Copy(t.dataFile, io.LimitReader(req.Reader, req.Range.Length))
	if err != nil {
		return err
	}
	t.Pieces[req.Num] = req.PieceMetaData
	return err
}

// GetPiece get a LimitReadCloser from task data with seeked, caller should read bytes and close it.
func (t *simpleLocalTaskStore) ReadPiece(ctx context.Context, req *ReadPieceRequest) (io.Reader, io.Closer, error) {
	t.touch()
	// dup fd instead lock and open a new file
	var dupFd, err = syscall.Dup(int(t.dataFile.Fd()))
	if err != nil {
		return nil, nil, err
	}
	dupFile := os.NewFile(uintptr(dupFd), t.dataFile.Name())
	// who call ReadPiece, who close the io.ReadCloser
	if _, err = dupFile.Seek(req.Range.Start, 0); err != nil {
		return nil, nil, err
	}
	return io.LimitReader(dupFile, req.Range.Length), dupFile, nil
}

func (t *simpleLocalTaskStore) Store(ctx context.Context, req *StoreRequest) error {
	err := t.saveMetadata()
	if err != nil {
		return err
	}
	// 1. try to link
	err = os.Link(path.Join(t.dataDir, taskData), req.Destination)
	if err == nil {
		return nil
	}
	// 2. link failed, copy it
	dupFD, err := syscall.Dup(int(t.dataFile.Fd()))
	if err != nil {
		return err
	}
	dupFile := os.NewFile(uintptr(dupFD), t.dataFile.Name())
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

func (t *simpleLocalTaskStore) GetPieces(ctx context.Context, req *base.PieceTaskRequest) ([]*base.PieceTask, error) {
	var tasks []*base.PieceTask
	for i := int32(0); i < req.Limit; i++ {
		if piece, ok := t.Pieces[req.StartNum+i]; ok {
			tasks = append(tasks, &base.PieceTask{
				PieceNum:   piece.Num,
				RangeStart: uint64(piece.Range.Start),
				RangeSize:  int32(piece.Range.Length),
				PieceMd5:   piece.Md5,
				SrcPid:     "",
				DstPid:     t.PeerID,
				// TODO update dst addr
				DstAddr:     "",
				PieceOffset: piece.Offset,
				PieceStyle:  piece.Style,
			})
		}
	}
	return tasks, nil
}

func (t *simpleLocalTaskStore) TryGC() (bool, error) {
	if t.lastAccess.Add(t.expireTime).Before(time.Now()) {
		log := logger.With("gc", SimpleLocalTaskStoreDriver, "task", t.TaskID)
		log.Infof("start gc task data")
		var err error

		// close and remove data
		if err = t.dataFile.Close(); err != nil {
			log.Warnf("close task data %q error: %s", t.dataFilePath, err)
			return false, err
		}
		if err = os.Remove(t.dataFilePath); err != nil && !os.IsNotExist(err) {
			log.Warnf("remove task data %q error: %s", t.dataFilePath, err)
			return false, err
		}
		log.Infof("purged task data: %s", t.dataFilePath)
		// close and remove metadata
		if err = t.metadataFile.Close(); err != nil {
			log.Warnf("close task meta data %q error: %s", t.metadataFilePath, err)
			return false, err
		}
		log.Infof("start gc task metadata")
		if err = os.Remove(t.metadataFilePath); err != nil && !os.IsNotExist(err) {
			log.Warnf("remove task meta data %q error: %s", t.metadataFilePath, err)
			return false, err
		}
		log.Infof("purged task mata data: %s", t.metadataFilePath)

		// remove task work metaDir
		if err = os.Remove(t.dataDir); err != nil && !os.IsNotExist(err) {
			log.Warnf("remove task data directory %q error: %s", t.dataDir, err)
			return false, err
		}
		log.Infof("purged task work directory: %s", t.dataDir)
		return true, nil
	}
	return false, nil
}

func (t *simpleLocalTaskStore) saveMetadata() error {
	t.lock.Lock()
	defer t.lock.Unlock()
	data, err := json.Marshal(t.persistentPeerTaskMetadata)
	if err != nil {
		return err
	}
	_, err = t.metadataFile.Seek(0, 0)
	if err != nil {
		return err
	}
	_, err = t.metadataFile.Write(data)
	return err
}
