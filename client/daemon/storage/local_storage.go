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
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path"
	"sync"
	"syscall"
	"time"

	"github.com/MysteriousPotato/go-lockable"
	"github.com/go-http-utils/headers"
	"go.uber.org/atomic"

	commonv1 "d7y.io/api/v2/pkg/apis/common/v1"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/internal/util"
	"d7y.io/dragonfly/v2/pkg/digest"
	"d7y.io/dragonfly/v2/pkg/net/http"
	"d7y.io/dragonfly/v2/pkg/source"
)

var globalFSWriteLock = lockable.NewMutexMap[string]()

type localTaskStore struct {
	*logger.SugaredLoggerOnWith
	persistentMetadata

	sync.RWMutex

	dataDir          string
	metadataFilePath string

	expireTime    time.Duration
	lastAccess    atomic.Int64
	reclaimMarked atomic.Bool
	gcCallback    func(CommonTaskRequest)

	// when digest not match, invalid will be set
	invalid atomic.Bool

	// content stores tiny file which length less than 128 bytes
	content []byte

	subtasks map[PeerTaskMetadata]*localSubTaskStore
}

var _ TaskStorageDriver = (*localTaskStore)(nil)
var _ Reclaimer = (*localTaskStore)(nil)

func (t *localTaskStore) touch() {
	access := time.Now().UnixNano()
	t.lastAccess.Store(access)
}

func (t *localTaskStore) SubTask(req *RegisterSubTaskRequest) *localSubTaskStore {
	subtask := &localSubTaskStore{
		parent: t,
		Range:  req.Range,
		persistentMetadata: persistentMetadata{
			TaskID:        req.SubTask.TaskID,
			TaskMeta:      map[string]string{},
			ContentLength: req.Range.Length,
			TotalPieces:   -1,
			PeerID:        req.SubTask.PeerID,
			Pieces:        map[int32]PieceMetadata{},
			PieceMd5Sign:  "",
			DataFilePath:  "",
			Done:          false,
		},
		SugaredLoggerOnWith: logger.With("task", req.SubTask.TaskID,
			"parent", req.Parent.TaskID, "peer", req.SubTask.PeerID, "component", "localSubTaskStore"),
	}
	t.Lock()
	t.subtasks[req.SubTask] = subtask
	t.Unlock()
	return subtask
}

func (t *localTaskStore) WritePiece(ctx context.Context, req *WritePieceRequest) (n int64, err error) {
	t.touch()

	// piece already exists
	t.RLock()
	if piece, ok := t.Pieces[req.Num]; ok {
		t.RUnlock()
		t.Debugf("piece %d already exist, ignore writing piece", req.Num)
		// discard already downloaded data for back source
		n, err = io.CopyN(io.Discard, req.Reader, piece.Range.Length)
		if err != nil && err != io.EOF {
			return n, err
		}
		if n != piece.Range.Length {
			return n, ErrShortRead
		}
		// NeedGenMetadata need to be called when using concurrent download, a Counter will increase in NeedGenMetadata
		if req.NeedGenMetadata != nil {
			req.NeedGenMetadata(n)
		}

		return piece.Range.Length, nil
	}
	t.RUnlock()

	start := time.Now().UnixNano()
	file, err := os.OpenFile(t.DataFilePath, os.O_RDWR, defaultFileMode)
	if err != nil {
		return 0, err
	}
	defer func() {
		if cerr := file.Close(); cerr != nil {
			err = errors.Join(err, cerr)
		}
	}()

	if _, err = file.Seek(req.Range.Start, io.SeekStart); err != nil {
		return 0, err
	}

	n, err = io.Copy(file, io.LimitReader(req.Reader, req.Range.Length))
	if err != nil {
		return n, err
	}

	// when UnknownLength and size is align to piece num
	if req.UnknownLength && n == 0 {
		t.Lock()
		t.genMetadata(n, req)
		t.Unlock()
		return 0, nil
	}

	if n != req.Range.Length {
		if req.UnknownLength {
			// when back source, and can not detect content length, we need update real length
			req.Range.Length = n
			// when n == 0, skip
			if n == 0 {
				t.Lock()
				t.genMetadata(n, req)
				t.Unlock()
				return 0, nil
			}
		} else {
			return n, ErrShortRead
		}
	}

	// when Md5 is empty, try to get md5 from reader, it's useful for back source
	if req.PieceMetadata.Md5 == "" {
		t.Debugf("piece %d md5 not found in metadata, read from reader", req.PieceMetadata.Num)
		if get, ok := req.Reader.(digest.Reader); ok {
			req.PieceMetadata.Md5 = get.Encoded()
			t.Infof("read piece %d md5 from reader, value: %s", req.PieceMetadata.Num, req.PieceMetadata.Md5)
		} else {
			t.Warnf("piece %d reader is not a digest.Reader", req.PieceMetadata.Num)
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
	req.PieceMetadata.Cost = uint64(time.Now().UnixNano() - start)
	t.Pieces[req.Num] = req.PieceMetadata
	t.genMetadata(n, req)
	return n, nil
}

func (t *localTaskStore) genMetadata(n int64, req *WritePieceRequest) {
	if req.NeedGenMetadata == nil {
		return
	}

	total, contentLength, gen := req.NeedGenMetadata(n)
	if !gen {
		return
	}

	t.TotalPieces = total
	t.ContentLength = contentLength

	var pieceDigests []string
	for i := int32(0); i < t.TotalPieces; i++ {
		pieceDigests = append(pieceDigests, t.Pieces[i].Md5)
	}

	digest := digest.SHA256FromStrings(pieceDigests...)
	t.PieceMd5Sign = digest
	t.Infof("generated digest: %s, total pieces: %d, content length: %d", digest, t.TotalPieces, t.ContentLength)
}

func (t *localTaskStore) UpdateTask(ctx context.Context, req *UpdateTaskRequest) error {
	t.touch()
	t.Lock()
	defer t.Unlock()
	if req.ContentLength > t.persistentMetadata.ContentLength {
		t.ContentLength = req.ContentLength
		t.Debugf("update content length: %d", t.ContentLength)
		// update empty file TotalPieces
		// the default req.TotalPieces is 0, need check ContentLength
		if t.ContentLength == 0 {
			t.TotalPieces = 0
		}
	}
	if req.TotalPieces > 0 {
		t.TotalPieces = req.TotalPieces
		t.Debugf("update total pieces: %d", t.TotalPieces)
	}
	if len(t.PieceMd5Sign) == 0 && len(req.PieceMd5Sign) > 0 {
		t.PieceMd5Sign = req.PieceMd5Sign
		t.Debugf("update piece md5 sign: %s", t.PieceMd5Sign)
	}
	if t.Header == nil && req.Header != nil && len(*req.Header) > 0 {
		t.Header = req.Header
		t.Debugf("update header: %#v", t.Header)
	}
	return nil
}

func (t *localTaskStore) ValidateDigest(*PeerTaskMetadata) error {
	t.Lock()
	defer t.Unlock()
	if t.ContentLength == 0 {
		return nil
	}

	if t.persistentMetadata.PieceMd5Sign == "" {
		t.invalid.Store(true)
		return ErrDigestNotSet
	}
	if t.TotalPieces <= 0 {
		t.Errorf("total piece count not set when validate digest")
		t.invalid.Store(true)
		return ErrPieceCountNotSet
	}

	var pieceDigests []string
	for i := int32(0); i < t.TotalPieces; i++ {
		pieceDigests = append(pieceDigests, t.Pieces[i].Md5)
	}

	digest := digest.SHA256FromStrings(pieceDigests...)
	if digest != t.PieceMd5Sign {
		t.Errorf("invalid digest, desired: %s, actual: %s", t.PieceMd5Sign, digest)
		t.invalid.Store(true)
		return ErrInvalidDigest
	}
	return nil
}

func (t *localTaskStore) IsInvalid(*PeerTaskMetadata) (bool, error) {
	return t.invalid.Load(), nil
}

// ReadPiece get a LimitReadCloser from task data with sought, caller should read bytes and close it.
func (t *localTaskStore) ReadPiece(ctx context.Context, req *ReadPieceRequest) (io.Reader, io.Closer, error) {
	if t.invalid.Load() {
		t.Errorf("invalid digest, refuse to get pieces")
		return nil, nil, ErrInvalidDigest
	}

	t.touch()
	file, err := os.Open(t.DataFilePath)
	if err != nil {
		return nil, nil, err
	}

	// If req.Num is equal to -1, range has a fixed value.
	if req.Num != -1 {
		t.RLock()
		if piece, ok := t.persistentMetadata.Pieces[req.Num]; ok {
			t.RUnlock()
			req.Range = piece.Range
		} else {
			t.RUnlock()
			file.Close()
			t.Errorf("invalid piece num: %d", req.Num)
			return nil, nil, ErrPieceNotFound
		}
	}

	if _, err = file.Seek(req.Range.Start, io.SeekStart); err != nil {
		file.Close()
		t.Errorf("file seek failed: %v", err)
		return nil, nil, err
	}
	// who call ReadPiece, who close the io.ReadCloser
	return io.LimitReader(file, req.Range.Length), file, nil
}

func (t *localTaskStore) ReadAllPieces(ctx context.Context, req *ReadAllPiecesRequest) (io.ReadCloser, error) {
	if t.invalid.Load() {
		t.Errorf("invalid digest, refuse to read all pieces")
		return nil, ErrInvalidDigest
	}

	t.touch()

	// who call ReadPiece, who close the io.ReadCloser
	file, err := os.Open(t.DataFilePath)
	if err != nil {
		return nil, err
	}

	if req.Range == nil {
		// by jim: for some corner case, avoid the io.Copy call superfluous sendfile syscall
		// then increase network latency
		return &limitedReadFile{
			reader: io.LimitReader(file, t.ContentLength),
			closer: file,
		}, nil
	}

	if _, err = file.Seek(req.Range.Start, io.SeekStart); err != nil {
		file.Close()
		t.Errorf("file seek to %d failed: %v", req.Range.Start, err)
		return nil, err
	}

	return &limitedReadFile{
		reader: io.LimitReader(file, req.Range.Length),
		closer: file,
	}, nil
}

func (t *localTaskStore) Store(ctx context.Context, req *StoreRequest) (err error) {
	// Store is called in callback.Done, mark local task store done, for fast search
	t.Done = true
	t.touch()
	if req.TotalPieces > 0 && t.TotalPieces == -1 {
		t.Lock()
		t.TotalPieces = req.TotalPieces
		t.Unlock()
	}

	if !req.StoreDataOnly {
		err = t.saveMetadata()
		if err != nil {
			t.Warnf("save task metadata error: %s", err)
			return err
		}
	}

	if req.MetadataOnly {
		return nil
	}

	globalFSWriteLock.LockKey(req.Destination)
	defer globalFSWriteLock.UnlockKey(req.Destination)

	if req.OriginalOffset {
		return hardlink(t.SugaredLoggerOnWith, req.Destination, t.DataFilePath)
	}

	_, err = os.Stat(req.Destination)
	if err == nil {
		// remove exist file
		t.Infof("destination file %q exists, purge it first", req.Destination)
		err = os.Remove(req.Destination)
		if err != nil {
			err = fmt.Errorf("purge destination file %q exists error: %s", req.Destination, err)
			t.Errorf(err.Error())
			return err
		}
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
	defer func() {
		if cerr := file.Close(); cerr != nil {
			err = errors.Join(err, cerr)
		}
	}()

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
	defer func() {
		if cerr := dstFile.Close(); cerr != nil {
			err = errors.Join(err, cerr)
		}
	}()
	// copy_file_range is valid in linux
	// https://go-review.googlesource.com/c/go/+/229101/
	n, err := io.Copy(dstFile, file)
	t.Debugf("copied tasks data %d bytes to %s", n, req.Destination)
	return err
}

func (t *localTaskStore) GetPieces(ctx context.Context, req *commonv1.PieceTaskRequest) (*commonv1.PiecePacket, error) {
	if req == nil {
		return nil, ErrBadRequest
	}
	if t.invalid.Load() {
		t.Errorf("invalid digest, refuse to get pieces")
		return nil, ErrInvalidDigest
	}

	t.RLock()
	defer t.RUnlock()
	t.touch()
	piecePacket := &commonv1.PiecePacket{
		TaskId:        req.TaskId,
		DstPid:        t.PeerID,
		TotalPiece:    t.TotalPieces,
		ContentLength: t.ContentLength,
		PieceMd5Sign:  t.PieceMd5Sign,
	}
	if t.TotalPieces > 0 && int32(req.StartNum) >= t.TotalPieces {
		t.Warnf("invalid start num: %d", req.StartNum)
	}
	for i := int32(0); i < int32(req.Limit); i++ {
		num := int32(req.StartNum) + i
		if t.TotalPieces > -1 && num >= t.TotalPieces {
			break
		}
		if piece, ok := t.Pieces[num]; ok {
			piecePacket.PieceInfos = append(piecePacket.PieceInfos,
				&commonv1.PieceInfo{
					PieceNum:     piece.Num,
					RangeStart:   uint64(piece.Range.Start),
					RangeSize:    uint32(piece.Range.Length),
					PieceMd5:     piece.Md5,
					PieceOffset:  piece.Offset,
					PieceStyle:   piece.Style,
					DownloadCost: piece.Cost / 1000,
				})
		}
	}
	return piecePacket, nil
}

func (t *localTaskStore) GetTotalPieces(ctx context.Context, req *PeerTaskMetadata) (int32, error) {
	if t.invalid.Load() {
		t.Errorf("invalid digest, refuse to get total pieces")
		return -1, ErrInvalidDigest
	}

	t.touch()
	return t.TotalPieces, nil
}

func (t *localTaskStore) GetExtendAttribute(ctx context.Context, req *PeerTaskMetadata) (*commonv1.ExtendAttribute, error) {
	if t.invalid.Load() {
		t.Errorf("invalid digest, refuse to get total pieces")
		return nil, ErrInvalidDigest
	}
	if t.Header == nil {
		return nil, nil
	}
	hdr := map[string]string{}
	for k, v := range *t.Header {
		if len(v) > 0 {
			hdr[k] = t.Header.Get(k)
		}
	}
	return &commonv1.ExtendAttribute{Header: hdr}, nil
}

func (t *localTaskStore) CanReclaim() bool {
	// task is invalid
	if t.invalid.Load() {
		return true
	}

	// don't gc if expire time is 0
	if t.expireTime == 0 {
		return false
	}

	now := time.Now()
	// task soft cache time reached
	access := time.Unix(0, t.lastAccess.Load())
	reclaim := access.Add(t.expireTime).Before(now)
	t.Debugf("reclaim check, last access: %v, reclaim: %v", access, reclaim)
	if reclaim || t.Header == nil {
		return reclaim
	}

	// task hard cache time reached
	if expire := t.Header.Get(headers.Expires); expire != "" {
		t.Debugf("reclaim check Expire header: %s", expire)
		expireTime, err := time.Parse(source.ExpireLayout, expire)
		if err == nil && now.After(expireTime) {
			t.Debugf("Expire header time reached")
			return true
		}
		if err != nil {
			t.Errorf("parse Expire header error: %s", err)
		}
	}
	return false
}

// MarkReclaim will try to invoke gcCallback (normal leave peer task)
func (t *localTaskStore) MarkReclaim() {
	if t.reclaimMarked.Load() {
		return
	}
	// leave task
	t.gcCallback(CommonTaskRequest{
		PeerID: t.PeerID,
		TaskID: t.TaskID,
	})
	t.reclaimMarked.Store(true)
	t.Infof("task %s/%s will be reclaimed, marked", t.TaskID, t.PeerID)

	t.Lock()
	var keys []PeerTaskMetadata
	for key := range t.subtasks {
		t.gcCallback(CommonTaskRequest{
			PeerID: key.PeerID,
			TaskID: key.TaskID,
		})
		t.Infof("sub task %s/%s will be reclaimed, marked", key.TaskID, key.PeerID)
		keys = append(keys, key)
	}
	for _, key := range keys {
		delete(t.subtasks, key)
	}
	t.Unlock()
}

func (t *localTaskStore) Reclaim() error {
	t.Infof("start gc task data")
	err := t.reclaimData()
	if err != nil && !os.IsNotExist(err) {
		return err
	}

	// close and remove metadata
	err = t.reclaimMeta()
	if err != nil && !os.IsNotExist(err) {
		return err
	}

	// remove task work metaDir
	if err = os.Remove(t.dataDir); err != nil && !os.IsNotExist(err) {
		t.Warnf("remove task data directory %q error: %s", t.dataDir, err)
		return err
	}
	t.Infof("purged task work directory: %s", t.dataDir)

	taskDir := path.Dir(t.dataDir)
	dirs, err := os.ReadDir(taskDir)
	if err != nil {
		t.Warnf("stat task directory %q error: %s", taskDir, err)
	} else {
		if len(dirs) == 0 {
			if err = os.Remove(taskDir); err != nil {
				t.Warnf("remove unused task directory %q error: %s", taskDir, err)
			}
		} else {
			t.Warnf("task directory %q is not empty", taskDir)
		}
	}
	return err
}

func (t *localTaskStore) reclaimData() error {
	// remove data
	data := path.Join(t.dataDir, taskData)
	stat, err := os.Lstat(data)
	if err != nil {
		t.Errorf("stat task data %q error: %s", data, err)
		return err
	}
	// remove symbol link cache file
	if stat.Mode()&os.ModeSymlink == os.ModeSymlink {
		dest, err0 := os.Readlink(data)
		if err0 == nil {
			if err = os.Remove(dest); err != nil && !os.IsNotExist(err) {
				t.Warnf("remove symlink target file %s error: %s", dest, err)
			} else {
				t.Infof("remove data file %s", dest)
			}
		}
	} else { // remove cache file
		if err = os.Remove(t.DataFilePath); err != nil && !os.IsNotExist(err) {
			t.Errorf("remove data file %s error: %s", data, err)
			return err
		}
	}
	if err = os.Remove(data); err != nil && !os.IsNotExist(err) {
		t.Errorf("remove data file %s error: %s", data, err)
		return err
	}
	t.Infof("purged task data: %s", data)
	return nil
}

func (t *localTaskStore) reclaimMeta() error {
	t.Infof("start gc task metadata")
	if err := os.Remove(t.metadataFilePath); err != nil && !os.IsNotExist(err) {
		t.Warnf("remove task meta data %q error: %s", t.metadataFilePath, err)
		return err
	}
	t.Infof("purged task mata data: %s", t.metadataFilePath)
	return nil
}

func (t *localTaskStore) saveMetadata() (err error) {
	t.Lock()
	defer t.Unlock()
	data, err := json.Marshal(t.persistentMetadata)
	if err != nil {
		return err
	}
	metadata, err := os.OpenFile(t.metadataFilePath, os.O_CREATE|os.O_RDWR, defaultFileMode)
	if err != nil {
		return err
	}
	defer func() {
		if cerr := metadata.Close(); cerr != nil {
			err = errors.Join(err, cerr)
		}
	}()
	_, err = metadata.Write(data)
	if err != nil {
		t.Errorf("save metadata error: %s", err)
	}
	return err
}

func (t *localTaskStore) partialCompleted(rg *http.Range) bool {
	t.RLock()
	defer t.RUnlock()

	if t.ContentLength == -1 {
		return false
	}

	realRange := &http.Range{
		Start:  rg.Start,
		Length: rg.Length,
	}

	// handle range like: bytes=1024-
	if realRange.Start+realRange.Length > t.ContentLength {
		realRange.Length = t.ContentLength - realRange.Start
	}

	start, end := computePiecePosition(t.ContentLength, realRange, util.ComputePieceSize)
	// fix int overflow
	if start < 0 || end < 0 {
		t.Warnf("wrong start and end piece num, %d, %d", start, end)
		return false
	}
	for i := start; i <= end; i++ {
		if _, ok := t.Pieces[i]; !ok {
			return false
		}
	}
	return true
}

func computePiecePosition(total int64, rg *http.Range, compute func(length int64) uint32) (start, end int32) {
	pieceSize := compute(total)
	start = int32(math.Floor(float64(rg.Start) / float64(pieceSize)))
	end = int32(math.Floor(float64(rg.Start+rg.Length-1) / float64(pieceSize)))
	return
}

// limitedReadFile implements io optimize for zero copy
type limitedReadFile struct {
	reader io.Reader
	closer io.Closer
}

func (l *limitedReadFile) Read(p []byte) (n int, err error) {
	return l.reader.Read(p)
}

func (l *limitedReadFile) Close() error {
	return l.closer.Close()
}

func (l *limitedReadFile) WriteTo(w io.Writer) (n int64, err error) {
	if r, ok := w.(io.ReaderFrom); ok {
		return r.ReadFrom(l.reader)
	}
	return io.Copy(w, l.reader)
}

func hardlink(log *logger.SugaredLoggerOnWith, dst, src string) error {
	dstStat, err := os.Stat(dst)
	if os.IsNotExist(err) {
		// hard link
		err = os.Link(src, dst)
		if err != nil {
			log.Errorf("hardlink from %q to %q error: %s", src, dst, err)
			return err
		}
		log.Infof("hardlink from %q to %q success", src, dst)
		return nil
	}

	if err != nil {
		log.Errorf("stat %q error: %s", src, err)
		return err
	}

	// target already exists, check inode
	srcStat, err := os.Stat(src)
	if err != nil {
		log.Errorf("stat %q error: %s", src, err)
		return err
	}

	dstSysStat, ok := dstStat.Sys().(*syscall.Stat_t)
	if !ok {
		log.Errorf("can not get inode for %q", dst)
		return err
	}

	srcSysStat, ok := srcStat.Sys().(*syscall.Stat_t)
	if ok {
		log.Errorf("can not get inode for %q", src)
		return err
	}

	if dstSysStat.Dev == srcSysStat.Dev && dstSysStat.Ino == srcSysStat.Ino {
		log.Debugf("target inode match underlay data inode, skip hard link")
		return nil
	}

	err = fmt.Errorf("target file %q exists, with different inode with underlay data %q", dst, src)
	return err
}
