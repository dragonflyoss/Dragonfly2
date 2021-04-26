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
	"io"
	"io/ioutil"
	"os"
	"path"
	"sync"
	"time"

	"d7y.io/dragonfly/v2/client/clientutil"
	"d7y.io/dragonfly/v2/pkg/dferrors"
	logger "d7y.io/dragonfly/v2/pkg/dflog"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	"d7y.io/dragonfly/v2/pkg/util/digestutils"
)

type localTaskStore struct {
	*logger.SugaredLoggerOnWith
	persistentMetadata

	*sync.RWMutex

	dataDir string

	metadataFile     *os.File
	metadataFilePath string

	expireTime    time.Duration
	lastAccess    time.Time
	reclaimMarked bool
	gcCallback    func(CommonTaskRequest)
}

func (t *localTaskStore) touch() {
	t.lastAccess = time.Now()
}

func (t *localTaskStore) WritePiece(ctx context.Context, req *WritePieceRequest) (int64, error) {
	t.touch()

	// piece already exists
	t.RLock()
	if piece, ok := t.Pieces[req.Num]; ok {
		t.RUnlock()
		return piece.Range.Length, nil
	}
	t.RUnlock()

	file, err := os.OpenFile(t.DataFilePath, os.O_RDWR, defaultFileMode)
	if err != nil {
		return 0, err
	}
	defer file.Close()
	if _, err = file.Seek(req.Range.Start, io.SeekStart); err != nil {
		return 0, err
	}
	var (
		r  *io.LimitedReader
		ok bool
		bn int64 // copied bytes from BufferedReader.B
	)
	if r, ok = req.Reader.(*io.LimitedReader); ok {
		// by jim: drain buffer and use raw reader(normally tcp connection) for using optimised operator, like splice
		if br, bok := r.R.(*clientutil.BufferedReader); bok {
			bn, err := io.CopyN(file, br.B, int64(br.B.Buffered()))
			if err != nil && err != io.EOF {
				return 0, err
			}
			r = io.LimitReader(br.R, r.N-bn).(*io.LimitedReader)
		}
	} else {
		r = io.LimitReader(req.Reader, req.Range.Length).(*io.LimitedReader)
	}
	n, err := io.Copy(file, r)
	if err != nil {
		return 0, err
	}
	// update copied bytes from BufferedReader.B
	n += bn
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
	// when Md5 is empty, try to get md5 from reader
	if req.PieceMetaData.Md5 == "" {
		if get, ok := req.Reader.(digestutils.DigestReader); ok {
			req.PieceMetaData.Md5 = get.Digest()
		}
	}
	t.Debugf("wrote %d bytes to file %s, piece %d, start %d, length: %d",
		n, t.DataFilePath, req.Num, req.Range.Start, req.Range.Length)
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
	if t.TotalPieces == 0 {
		t.TotalPieces = req.TotalPieces
	}
	return nil
}

// GetPiece get a LimitReadCloser from task data with seeked, caller should read bytes and close it.
func (t *localTaskStore) ReadPiece(ctx context.Context, req *ReadPieceRequest) (io.Reader, io.Closer, error) {
	t.touch()
	file, err := os.Open(t.DataFilePath)
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
	if req.TotalPieces > 0 {
		t.TotalPieces = req.TotalPieces
	}
	err := t.saveMetadata()
	if err != nil {
		t.Warnf("save task metadata error: %s", err)
		return err
	}
	if req.MetadataOnly {
		return nil
	}
	_, err = os.Stat(req.Destination)
	if err == nil {
		// remove exist file
		t.Infof("destination file %q exists, purge it first", req.Destination)
		os.Remove(req.Destination)
	}
	// 1. try to link
	err = os.Link(t.DataFilePath, req.Destination)
	if err == nil {
		t.Infof("task data link to file %q success", req.Destination)
		return nil
	}
	t.Warnf("task data link to file %q error: %s", req.Destination, err)
	// 2. link failed, copy it
	file, err := os.Open(t.DataFilePath)
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
	if t.TotalPieces > 0 && req.StartNum >= t.TotalPieces {
		logger.Errorf("invalid start num: %d", req.StartNum)
		return nil, dferrors.ErrInvalidArgument
	}
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
		TaskId: req.TaskId,
		DstPid: t.PeerID,
		//DstAddr:       "", // filled by peer service
		PieceInfos:    pieces,
		TotalPiece:    t.TotalPieces,
		ContentLength: t.ContentLength,
		PieceMd5Sign:  t.PieceMd5Sign,
	}, nil
}

func (t *localTaskStore) CanReclaim() bool {
	return t.lastAccess.Add(t.expireTime).Before(time.Now())
}

// MarkReclaim will try to invoke gcCallback (normal leave peer task)
func (t *localTaskStore) MarkReclaim() {
	if t.reclaimMarked {
		return
	}
	// leave task
	t.gcCallback(CommonTaskRequest{
		PeerID: t.PeerID,
		TaskID: t.TaskID,
	})
	t.reclaimMarked = true
	logger.Infof("task %s/%s will be reclaimed, marked", t.TaskID, t.PeerID)
}

func (t *localTaskStore) Reclaim() error {
	log := logger.With("gc", t.StoreStrategy, "task", t.TaskID)
	log.Infof("start gc task data")
	err := t.reclaimData(log)
	if err != nil && !os.IsNotExist(err) {
		return err
	}

	// close and remove metadata
	err = t.reclaimMeta(log)
	if err != nil && !os.IsNotExist(err) {
		return err
	}

	// remove task work metaDir
	if err = os.Remove(t.dataDir); err != nil && !os.IsNotExist(err) {
		log.Warnf("remove task data directory %q error: %s", t.dataDir, err)
		return err
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
		} else {
			log.Warnf("task directory %q is not empty", taskDir)
		}
	}
	return nil
}

func (t *localTaskStore) reclaimData(log *logger.SugaredLoggerOnWith) error {
	// remove data
	data := path.Join(t.dataDir, taskData)
	stat, err := os.Lstat(data)
	if err != nil {
		log.Errorf("stat task data %q error: %s", data, err)
		return err
	}
	// remove sym link cache file
	if stat.Mode()&os.ModeSymlink == os.ModeSymlink {
		dest, err0 := os.Readlink(data)
		if err0 == nil {
			if err = os.Remove(dest); err != nil {
				log.Warnf("remove symlink target file %s error: %s", dest, err)
			} else {
				log.Infof("remove data file %s", dest)
			}
		}
	} else { // remove cache file
		if err = os.Remove(t.DataFilePath); err != nil {
			log.Errorf("remove data file %s error: %s", data, err)
			return err
		}
	}
	if err = os.Remove(data); err != nil {
		log.Errorf("remove data file %s error: %s", data, err)
		return err
	}
	log.Infof("purged task data: %s", data)
	return nil
}

func (t *localTaskStore) reclaimMeta(log *logger.SugaredLoggerOnWith) error {
	if err := t.metadataFile.Close(); err != nil {
		log.Warnf("close task meta data %q error: %s", t.metadataFilePath, err)
		return err
	}
	log.Infof("start gc task metadata")
	if err := os.Remove(t.metadataFilePath); err != nil && !os.IsNotExist(err) {
		log.Warnf("remove task meta data %q error: %s", t.metadataFilePath, err)
		return err
	}
	log.Infof("purged task mata data: %s", t.metadataFilePath)
	return nil
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
