package storage

import (
	"context"
	"encoding/json"
	"io"
	"io/ioutil"
	"os"
	"path"
	"sync"
	"time"

	"d7y.io/dragonfly/v2/pkg/dfcodes"
	logger "d7y.io/dragonfly/v2/pkg/dflog"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
)

type localTaskStore struct {
	*logger.SugaredLoggerOnWith
	persistentMetadata

	*sync.RWMutex

	dataDir string

	metadataFile     *os.File
	metadataFilePath string

	// TODO currently, we open a new *os.File for all operations, we need a cache for it
	// open file syscall costs about 700ns, while close syscall costs about 800ns
	//dataFile     *os.File
	dataFilePath string

	expireTime time.Duration
	lastAccess time.Time
	gcCallback func(CommonTaskRequest)
}

func (t *localTaskStore) init() error {
	if err := os.MkdirAll(t.dataDir, defaultDirectoryMode); err != nil && !os.IsExist(err) {
		return err
	}
	metadata, err := os.OpenFile(t.metadataFilePath, os.O_CREATE|os.O_RDWR, defaultFileMode)
	if err != nil {
		return err
	}
	t.metadataFile = metadata

	// just create data file
	data, err := os.OpenFile(t.dataFilePath, os.O_CREATE|os.O_RDWR, defaultFileMode)
	if err != nil {
		return err
	}
	//t.dataFile = data
	return data.Close()
}

func (t *localTaskStore) touch() {
	t.lastAccess = time.Now()
}

func (t *localTaskStore) WritePiece(ctx context.Context, req *WritePieceRequest) (int64, error) {
	t.touch()

	// piece already exists
	t.RLock()
	if _, ok := t.Pieces[req.Num]; ok {
		t.RUnlock()
		return 0, nil
	}
	t.RUnlock()

	file, err := os.OpenFile(t.dataFilePath, os.O_RDWR, defaultFileMode)
	if err != nil {
		return 0, err
	}
	defer file.Close()
	if _, err = file.Seek(req.Range.Start, io.SeekStart); err != nil {
		return 0, err
	}
	n, err := io.Copy(file, io.LimitReader(req.Reader, req.Range.Length))
	if err != nil && err != io.EOF {
		return 0, err
	}
	if n != req.Range.Length {
		if req.UnknownLength {
			// when back source, and can not detect content length, we need update real length
			req.Range.Length = n
			// when n == 0, skip
			if n == 0 {
				return 0, nil
			}
		} else {
			return n, ErrShortRead
		}
	}
	t.Debugf("wrote %d bytes to file %s, piece %d, start %d, length: %d",
		n, t.dataFilePath, req.Num, req.Range.Start, req.Range.Length)
	t.Lock()
	defer t.Unlock()
	// double check
	if _, ok := t.Pieces[req.Num]; ok {
		return n, nil
	}
	t.Pieces[req.Num] = req.PieceMetaData
	return n, nil
}

func (t *localTaskStore) UpdateTask(ctx context.Context, req *UpdateTaskRequest) error {
	t.Lock()
	defer t.Unlock()
	t.persistentMetadata.ContentLength = req.ContentLength
	return nil
}

// GetPiece get a LimitReadCloser from task data with seeked, caller should read bytes and close it.
func (t *localTaskStore) ReadPiece(ctx context.Context, req *ReadPieceRequest) (io.Reader, io.Closer, error) {
	t.touch()
	file, err := os.Open(t.dataFilePath)
	if err != nil {
		return nil, nil, err
	}
	if req.Num != -1 {
		t.RLock()
		if piece, ok := t.persistentMetadata.Pieces[req.Num]; ok {
			t.RUnlock()
			req.Range = piece.Range
		} else {
			t.RUnlock()
			return nil, nil, ErrPieceNotFound
		}
	}
	// who call ReadPiece, who close the io.ReadCloser
	if _, err = file.Seek(req.Range.Start, io.SeekStart); err != nil {
		return nil, nil, err
	}
	return io.LimitReader(file, req.Range.Length), file, nil
}

func (t *localTaskStore) Store(ctx context.Context, req *StoreRequest) error {
	err := t.saveMetadata()
	if err != nil {
		t.Warnf("save task metadata error: %s", err)
		return err
	}
	if req.MetadataOnly {
		return nil
	}
	switch t.StoreStrategy {
	case string(SimpleLocalTaskStoreStrategy):

	case string(AdvanceLocalTaskStoreStrategy):
		// is already done by init
		return nil
	}
	_, err = os.Stat(req.Destination)
	if err == nil {
		// remove exist file
		t.Infof("destination file %q exists, purge it first", req.Destination)
		os.Remove(req.Destination)
	}
	// 1. try to link
	err = os.Link(path.Join(t.dataDir, taskData), req.Destination)
	if err == nil {
		t.Infof("task data link to file %q success", req.Destination)
		return nil
	}
	t.Warnf("task data link to file %q error: %s", req.Destination, err)
	// 2. link failed, copy it
	file, err := os.Open(t.dataFilePath)
	if err != nil {
		t.Debugf("open tasks data error: %s", err)
		return err
	}
	defer file.Close()

	_, err = file.Seek(0, io.SeekStart)
	if err != nil {
		t.Debugf("task seek file error: %s", err)
		return err
	}
	dstFile, err := os.OpenFile(req.Destination, os.O_CREATE|os.O_RDWR|os.O_TRUNC, defaultFileMode)
	if err != nil {
		t.Errorf("open tasks destination file error: %s", err)
		return err
	}
	defer dstFile.Close()
	// copy_file_range is valid in linux
	// https://go-review.googlesource.com/c/go/+/229101/
	n, err := io.Copy(dstFile, file)
	t.Debugf("copied tasks data %d bytes to %s", n, req.Destination)
	return err
}

func (t *localTaskStore) GetPieces(ctx context.Context, req *base.PieceTaskRequest) (*base.PiecePacket, error) {
	var pieces []*base.PieceInfo
	t.RLock()
	defer t.RUnlock()
	for i := int32(0); i < req.Limit; i++ {
		if piece, ok := t.Pieces[req.StartNum+i]; ok {
			pieces = append(pieces, &base.PieceInfo{
				PieceNum:    piece.Num,
				RangeStart:  uint64(piece.Range.Start),
				RangeSize:   int32(piece.Range.Length),
				PieceMd5:    piece.Md5,
				PieceOffset: piece.Offset,
				PieceStyle:  piece.Style,
			})
		}
	}
	return &base.PiecePacket{
		State: &base.ResponseState{
			Success: true,
			Code:    dfcodes.Success,
		},
		TaskId: req.TaskId,
		DstPid: t.PeerID,
		//DstAddr:       "", // filled by peer service
		PieceInfos:    pieces,
		TotalPiece:    int32(len(t.Pieces)),
		ContentLength: t.ContentLength,
		PieceMd5Sign:  t.PieceMd5Sign,
	}, nil
}

func (t *localTaskStore) TryGC() (bool, error) {
	if t.lastAccess.Add(t.expireTime).Before(time.Now()) {
		if t.gcCallback != nil {
			t.gcCallback(CommonTaskRequest{
				PeerID: t.PeerID,
				TaskID: t.TaskID,
			})
		}
		log := logger.With("gc", t.StoreStrategy, "task", t.TaskID)
		log.Infof("start gc task data")
		var err error

		// close and remove data
		//if err = t.dataFile.Close(); err != nil {
		//	log.Warnf("close task data %q error: %s", t.dataFilePath, err)
		//	return false, err
		//}
		if err = os.Remove(path.Join(t.dataDir, taskData)); err != nil && !os.IsNotExist(err) {
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

		taskDir := path.Dir(t.dataDir)
		if dirs, err := ioutil.ReadDir(taskDir); err != nil {
			log.Warnf("stat task directory %q error: %s", taskDir, err)
		} else {
			if len(dirs) == 0 {
				if err := os.Remove(taskDir); err != nil {
					log.Warnf("remove unused task directory %q error: %s", taskDir, err)
				}
			}
		}
		return true, nil
	}
	return false, nil
}

func (t *localTaskStore) saveMetadata() error {
	t.RLock()
	defer t.RUnlock()
	data, err := json.Marshal(t.persistentMetadata)
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
