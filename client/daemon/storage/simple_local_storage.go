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

	// TODO currently, we open a new *os.File for all operations, we need a cache for it
	// open file syscall costs about 700ns, while close syscall costs about 800ns
	dataFile     *os.File
	dataFilePath string

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
		logger.Debugf("load task %s metadata from %s", t.persistentPeerTaskMetadata.TaskID, t.metadataFilePath)
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

	// piece already exists
	if _, ok := t.Pieces[req.Num]; ok {
		return nil
	}

	file, err := os.OpenFile(t.dataFilePath, os.O_RDWR, defaultFileMode)
	if err != nil {
		return err
	}
	defer file.Close()
	if _, err = file.Seek(req.Range.Start, io.SeekStart); err != nil {
		return err
	}
	n, err := io.Copy(file, io.LimitReader(req.Reader, req.Range.Length))
	if err != nil {
		return err
	}
	logger.Debugf("task %s wrote %d bytes to file %s, piece %d, start %d, length: %d",
		t.TaskID, n, t.dataFilePath, req.Num, req.Range.Start, req.Range.Length)
	t.lock.Lock()
	t.Pieces[req.Num] = req.PieceMetaData
	t.lock.Unlock()
	return nil
}

// GetPiece get a LimitReadCloser from task data with seeked, caller should read bytes and close it.
func (t *simpleLocalTaskStore) ReadPiece(ctx context.Context, req *ReadPieceRequest) (io.Reader, io.Closer, error) {
	t.touch()
	file, err := os.Open(t.dataFilePath)
	if err != nil {
		return nil, nil, err
	}
	// who call ReadPiece, who close the io.ReadCloser
	if _, err = file.Seek(req.Range.Start, io.SeekStart); err != nil {
		return nil, nil, err
	}
	return io.LimitReader(file, req.Range.Length), file, nil
}

func (t *simpleLocalTaskStore) Store(ctx context.Context, req *StoreRequest) error {
	err := t.saveMetadata()
	if err != nil {
		logger.Warnf("save task %s metadata error: %s", t.TaskID, err)
		return err
	}
	// 1. try to link
	err = os.Link(path.Join(t.dataDir, taskData), req.Destination)
	if err == nil {
		return nil
	}
	logger.Warnf("task %s link to file %q error: %s", t.TaskID, req.Destination, err)
	// 2. link failed, copy it
	file, err := os.Open(t.dataFilePath)
	if err != nil {
		logger.Debugf("open tasks %s data error: %s", t.TaskID, err)
		return err
	}
	defer file.Close()

	_, err = file.Seek(0, io.SeekStart)
	if err != nil {
		logger.Debugf("task %s seek file error: %s", t.TaskID, err)
		return err
	}
	dstFile, err := os.OpenFile(req.Destination, os.O_CREATE|os.O_RDWR|os.O_TRUNC, defaultFileMode)
	if err != nil {
		logger.Debugf("open tasks %s destination file error: %s", t.TaskID, err)
		return err
	}
	defer dstFile.Close()
	// copy_file_range is valid in linux
	// https://go-review.googlesource.com/c/go/+/229101/
	n, err := io.Copy(dstFile, file)
	logger.Debugf("copied tasks %s data %d bytes to %s", t.TaskID, n, req.Destination)
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
	_, err = t.metadataFile.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}
	_, err = t.metadataFile.Write(data)
	return err
}
