package storage

import (
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/pkg/errors"

	logger "github.com/dragonflyoss/Dragonfly2/pkg/dflog"
	"github.com/dragonflyoss/Dragonfly2/pkg/rpc/base"
)

// advanceLocalTaskStorage handles all peer tasks with advance storage policy
// soft link: data -> target device cache file
// hard link: target device cache file -> target device cache file
type advanceLocalTaskStorage struct {
	*sync.Mutex
	taskID string
	// key: peerID
	peerTasks map[string]*simpleLocalTaskStore
}

func init() {
	Register(AdvanceLocalTaskStoreDriver, NewAdvanceLocalTaskStoreExecutor)
}

type advanceLocalTaskStoreExecutor struct {
	*sync.Mutex
	// key: task id
	tasks        *sync.Map
	opt          *Option
	dataPathStat *syscall.Stat_t
}

func NewAdvanceLocalTaskStoreExecutor(opt *Option) (TaskStorageExecutor, error) {
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
	return advanceLocalTaskStoreExecutor{
		Mutex:        &sync.Mutex{},
		tasks:        &sync.Map{},
		opt:          opt,
		dataPathStat: stat.Sys().(*syscall.Stat_t),
	}, nil
}

func (e advanceLocalTaskStoreExecutor) LoadTask(taskID string, peerID string) (TaskStorageDriver, bool) {
	d, ok := e.tasks.Load(taskID)
	if peerID != "" {
		_, ok := d.(*advanceLocalTaskStorage).peerTasks[peerID]
		if !ok {
			return nil, ok
		}
	}
	if !ok {
		return nil, false
	}
	return d.(TaskStorageDriver), ok
}

func (e advanceLocalTaskStoreExecutor) CreateTask(req RegisterTaskRequest) error {
	d, ok := e.tasks.Load(req.TaskID)
	var s *advanceLocalTaskStorage
	if !ok {
		e.Lock()
		// double check
		d, ok = e.tasks.Load(req.TaskID)
		if !ok {
			if e.dataPathStat == nil {

			}
			s = &advanceLocalTaskStorage{
				Mutex:     &sync.Mutex{},
				taskID:    req.TaskID,
				peerTasks: map[string]*simpleLocalTaskStore{},
			}
			e.tasks.Store(req.TaskID, s)
			d = s
		}
		e.Unlock()
	}
	s = d.(*advanceLocalTaskStorage)

	logger.Debugf("init advance local task storage, peer id: %s, task id: %s", req.PeerID, req.TaskID)
	filename := filepath.Base(req.Destination)
	dir := filepath.Dir(req.Destination)
	dataFilePath := path.Join(dir, fmt.Sprintf(".%s.d7s.cache", filename))
	dirStat, err := os.Stat(dir)
	if err != nil {
		return err
	}

	dataDir := path.Join(e.opt.DataPath, string(AdvanceLocalTaskStoreDriver), req.TaskID)
	t := &simpleLocalTaskStore{
		persistentPeerTaskMetadata: persistentPeerTaskMetadata{
			TaskID:   req.TaskID,
			TaskMeta: map[string]string{},
			PeerID:   req.PeerID,
			Pieces:   map[int32]PieceMetaData{},
		},
		lock:             &sync.Mutex{},
		dataDir:          dataDir,
		metadataFilePath: path.Join(dataDir, taskMetaData),
		dataFilePath:     dataFilePath,
		expireTime:       e.opt.TaskExpireTime,
	}
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

	stat := dirStat.Sys().(*syscall.Stat_t)
	if stat.Dev != e.dataPathStat.Dev {
		// symbol link
		if err := os.Symlink(t.dataFilePath, path.Join(dataDir, taskData)); err != nil {
			return err
		}
	} else {
		// hard link
		if err := os.Link(t.dataFilePath, path.Join(dataDir, taskData)); err != nil {
			return err
		}
	}

	s.Lock()
	s.peerTasks[req.PeerID] = t
	s.Unlock()
	return nil
}

func (e advanceLocalTaskStoreExecutor) ReloadPersistentTask() error {
	//stat, err := os.Stat(path.Join(opt.DataPath, string(AdvanceLocalTaskStoreDriver)))
	//if err != nil {
	//	return err
	//}
	panic("not implement")
}

func (e advanceLocalTaskStoreExecutor) TryGC() (bool, error) {
	var tasks []string
	e.tasks.Range(func(key, value interface{}) bool {
		ok, err := value.(*advanceLocalTaskStorage).TryGC()
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

func (a advanceLocalTaskStorage) WritePiece(ctx context.Context, req *WritePieceRequest) error {
	lts, ok := a.peerTasks[req.PeerID]
	if !ok {
		return ErrTaskNotFound
	}
	return lts.WritePiece(ctx, req)
}

func (a advanceLocalTaskStorage) ReadPiece(ctx context.Context, req *ReadPieceRequest) (io.Reader, io.Closer, error) {
	lts, ok := a.peerTasks[req.PeerID]
	if !ok {
		return nil, nil, ErrTaskNotFound
	}
	return lts.ReadPiece(ctx, req)
}

func (a advanceLocalTaskStorage) GetPieces(ctx context.Context, req *base.PieceTaskRequest) (*base.PiecePacket, error) {
	var (
		pieces []*base.PieceInfo

		peerID  string
		md5sign string
		total   int32
		length  int64
	)
FindNextPiece:
	for i := int32(0); i < req.Limit; i++ {
		for pid, store := range a.peerTasks {
			if piece, ok := store.Pieces[req.StartNum+i]; ok {
				pieces = append(pieces, &base.PieceInfo{
					PieceNum:    piece.Num,
					RangeStart:  uint64(piece.Range.Start),
					RangeSize:   int32(piece.Range.Length),
					PieceMd5:    piece.Md5,
					PieceOffset: piece.Offset,
					PieceStyle:  piece.Style,
				})
				// TODO use last store info
				peerID = pid
				total = int32(len(store.Pieces))
				length = store.ContentLength
				continue FindNextPiece
			}
		}
	}
	return &base.PiecePacket{
		State: &base.ResponseState{
			Success: true,
			Code:    base.Code_SUCCESS,
			Msg:     "",
		},
		TaskId: req.TaskId,
		DstPid: peerID,
		//DstAddr:       "",
		PieceInfos:    pieces,
		TotalPiece:    total,
		ContentLength: length,
		PieceMd5Sign:  md5sign,
	}, nil
}

func (a advanceLocalTaskStorage) Store(ctx context.Context, req *StoreRequest) error {
	lts, ok := a.peerTasks[req.PeerID]
	if !ok {
		return ErrTaskNotFound
	}
	return os.Rename(lts.dataFilePath, req.Destination)
}

func (a advanceLocalTaskStorage) TryGC() (bool, error) {
	var (
		notAll bool
		err    error
	)
	for peer, task := range a.peerTasks {
		ok, e := a.tryGC(task)
		if !ok {
			notAll = true
		}
		if e != nil {
			err = errors.Wrapf(err, fmt.Sprintf("gc peer %s error: %s", peer, e.Error()))
		}
	}
	return !notAll, err
}

func (a advanceLocalTaskStorage) tryGC(t *simpleLocalTaskStore) (bool, error) {
	if t.lastAccess.Add(t.expireTime).Before(time.Now()) {
		log := logger.With("gc", AdvanceLocalTaskStoreDriver, "task", t.TaskID)
		log.Infof("start gc task data")
		var err error

		// close and remove data
		if err = t.dataFile.Close(); err != nil {
			log.Warnf("close task data %q error: %s", t.dataFilePath, err)
			return false, err
		}
		// just remove local cache
		if err = os.Remove(filepath.Join(t.dataDir, taskData)); err != nil && !os.IsNotExist(err) {
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
