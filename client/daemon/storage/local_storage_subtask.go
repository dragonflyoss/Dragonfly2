/*
 *     Copyright 2022 The Dragonfly Authors
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
	"errors"
	"io"
	"os"
	"sync"

	"go.uber.org/atomic"

	commonv1 "d7y.io/api/v2/pkg/apis/common/v1"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/digest"
	"d7y.io/dragonfly/v2/pkg/net/http"
)

// TODO need refactor with localTaskStore, currently, localSubTaskStore code copies from localTaskStore
type localSubTaskStore struct {
	sync.RWMutex
	persistentMetadata
	*logger.SugaredLoggerOnWith
	parent *localTaskStore

	// when digest not match, invalid will be set
	invalid atomic.Bool

	Range *http.Range
}

func (t *localSubTaskStore) WritePiece(ctx context.Context, req *WritePieceRequest) (n int64, err error) {
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

	// TODO different with localTaskStore
	file, err := os.OpenFile(t.parent.DataFilePath, os.O_RDWR, defaultFileMode)
	if err != nil {
		return 0, err
	}
	defer func() {
		if cerr := file.Close(); cerr != nil {
			err = errors.Join(err, cerr)
		}
	}()
	// TODO different with localTaskStore
	if _, err = file.Seek(t.Range.Start+req.Range.Start, io.SeekStart); err != nil {
		return 0, err
	}

	n, err = io.Copy(file, io.LimitReader(req.Reader, req.Range.Length))
	if err != nil {
		return 0, err
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
		n, t.parent.DataFilePath, req.Num, req.Range.Start, req.Range.Length)
	t.Lock()
	defer t.Unlock()
	// double check
	if _, ok := t.Pieces[req.Num]; ok {
		return n, nil
	}
	t.Pieces[req.Num] = req.PieceMetadata
	t.genMetadata(n, req)
	return n, nil
}

func (t *localSubTaskStore) ReadPiece(ctx context.Context, req *ReadPieceRequest) (io.Reader, io.Closer, error) {
	if t.invalid.Load() {
		t.Errorf("invalid digest, refuse to get pieces")
		return nil, nil, ErrInvalidDigest
	}

	// TODO different with localTaskStore
	t.parent.touch()
	file, err := os.Open(t.parent.DataFilePath)
	if err != nil {
		return nil, nil, err
	}

	// If req.Num is equal to -1, range has a fixed value.
	if req.Num != -1 {
		t.RLock()
		if piece, ok := t.Pieces[req.Num]; ok {
			t.RUnlock()
			req.Range = piece.Range
		} else {
			t.RUnlock()
			file.Close()
			t.Errorf("invalid piece num: %d", req.Num)
			return nil, nil, ErrPieceNotFound
		}
	}

	// TODO different with localTaskStore
	if _, err = file.Seek(t.Range.Start+req.Range.Start, io.SeekStart); err != nil {
		file.Close()
		t.Errorf("file seek failed: %v", err)
		return nil, nil, err
	}
	// who call ReadPiece, who close the io.ReadCloser
	return io.LimitReader(file, req.Range.Length), file, nil
}

func (t *localSubTaskStore) ReadAllPieces(ctx context.Context, req *ReadAllPiecesRequest) (io.ReadCloser, error) {
	if t.invalid.Load() {
		t.Errorf("invalid digest, refuse to read all pieces")
		return nil, ErrInvalidDigest
	}

	t.parent.touch()

	// who call ReadPiece, who close the io.ReadCloser
	file, err := os.Open(t.parent.DataFilePath)
	if err != nil {
		return nil, err
	}

	var (
		start  int64
		length int64
	)

	if req.Range == nil {
		start, length = t.Range.Start, t.Range.Length
	} else {
		start, length = t.Range.Start+req.Range.Start, t.Range.Length
	}

	if _, err = file.Seek(start, io.SeekStart); err != nil {
		file.Close()
		t.Errorf("file seek to %d failed: %v", start, err)
		return nil, err
	}

	return &limitedReadFile{
		reader: io.LimitReader(file, length),
		closer: file,
	}, nil
}

func (t *localSubTaskStore) GetPieces(ctx context.Context, req *commonv1.PieceTaskRequest) (*commonv1.PiecePacket, error) {
	if t.invalid.Load() {
		t.Errorf("invalid digest, refuse to get pieces")
		return nil, ErrInvalidDigest
	}

	t.RLock()
	defer t.RUnlock()
	t.parent.touch()
	piecePacket := &commonv1.PiecePacket{
		TaskId:        req.TaskId,
		DstPid:        t.PeerID,
		TotalPiece:    t.TotalPieces,
		ContentLength: t.ContentLength,
		PieceMd5Sign:  t.PieceMd5Sign,
	}

	if t.TotalPieces > -1 && int32(req.StartNum) >= t.TotalPieces {
		t.Warnf("invalid start num: %d", req.StartNum)
	}

	for i := int32(0); i < int32(req.Limit); i++ {
		if piece, ok := t.Pieces[int32(req.StartNum)+i]; ok {
			piecePacket.PieceInfos = append(piecePacket.PieceInfos, &commonv1.PieceInfo{
				PieceNum:    piece.Num,
				RangeStart:  uint64(piece.Range.Start),
				RangeSize:   uint32(piece.Range.Length),
				PieceMd5:    piece.Md5,
				PieceOffset: piece.Offset,
				PieceStyle:  piece.Style,
			})
		}
	}
	return piecePacket, nil
}

func (t *localSubTaskStore) GetTotalPieces(ctx context.Context, req *PeerTaskMetadata) (int32, error) {
	if t.invalid.Load() {
		t.Errorf("invalid digest, refuse to get total pieces")
		return -1, ErrInvalidDigest
	}

	t.parent.touch()
	return t.TotalPieces, nil
}

func (t *localSubTaskStore) UpdateTask(ctx context.Context, req *UpdateTaskRequest) error {
	t.parent.touch()
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
	return nil
}

func (t *localSubTaskStore) Store(ctx context.Context, req *StoreRequest) (err error) {
	// Store is called in callback.Done, mark local task store done, for fast search
	t.Done = true
	t.parent.touch()
	if req.TotalPieces > 0 {
		t.Lock()
		t.TotalPieces = req.TotalPieces
		t.Unlock()
	}

	if req.MetadataOnly {
		return nil
	}

	globalFSWriteLock.LockKey(req.Destination)
	defer globalFSWriteLock.UnlockKey(req.Destination)

	if req.OriginalOffset {
		return hardlink(t.SugaredLoggerOnWith, req.Destination, t.parent.DataFilePath)
	}

	_, err = os.Stat(req.Destination)
	if err == nil {
		// remove exist file
		t.Infof("destination file %q exists, purge it first", req.Destination)
		os.Remove(req.Destination)
	}

	file, err := os.Open(t.parent.DataFilePath)
	if err != nil {
		t.Debugf("open tasks data error: %s", err)
		return err
	}
	defer func() {
		if cerr := file.Close(); cerr != nil {
			err = errors.Join(err, cerr)
		}
	}()

	_, err = file.Seek(t.Range.Start, io.SeekStart)
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
	n, err := io.Copy(dstFile, io.LimitReader(file, t.ContentLength))
	t.Debugf("copied tasks data %d bytes to %s", n, req.Destination)
	return err
}

func (t *localSubTaskStore) ValidateDigest(req *PeerTaskMetadata) error {
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

func (t *localSubTaskStore) IsInvalid(req *PeerTaskMetadata) (bool, error) {
	return t.invalid.Load(), nil
}

func (t *localSubTaskStore) genMetadata(n int64, req *WritePieceRequest) {
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

func (t *localSubTaskStore) CanReclaim() bool {
	if t.parent.Done || t.invalid.Load() {
		return true
	}

	return false
}

func (t *localSubTaskStore) MarkReclaim() {
	// gc this subtask with gcCallback from parent
	t.parent.gcCallback(CommonTaskRequest{
		PeerID: t.PeerID,
		TaskID: t.TaskID,
	})
	t.Infof("sub task %s/%s will be reclaimed, marked", t.TaskID, t.PeerID)

	t.parent.Lock()
	// remove subtask from parent
	delete(t.parent.subtasks, PeerTaskMetadata{
		PeerID: t.PeerID,
		TaskID: t.TaskID,
	})
	t.parent.Unlock()
}

func (t *localSubTaskStore) Reclaim() error {
	return nil
}

func (t *localSubTaskStore) GetExtendAttribute(ctx context.Context, req *PeerTaskMetadata) (*commonv1.ExtendAttribute, error) {
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
