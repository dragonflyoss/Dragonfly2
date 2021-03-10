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

package cdn

import (
	"bytes"
	"context"
	"d7y.io/dragonfly/v2/cdnsystem/daemon/mgr/cdn/storage"
	"d7y.io/dragonfly/v2/cdnsystem/types"
	logger "d7y.io/dragonfly/v2/pkg/dflog"
	"github.com/pkg/errors"
	"io"
	"sync"
)

type protocolContent struct {
	TaskId       string
	pieceNum     int32
	pieceSize    int32
	pieceContent *bytes.Buffer
}

type downloadMetadata struct {
	backSourceLength     int64 // back to source download file length
	realCdnFileLength    int64 // the actual length of the stored file
	realSourceFileLength int64 // actually read the length of the source
	pieceTotalCount      int32 // piece total count
	pieceMd5Sign         string
}

type cacheWriter struct {
	cdnStore    storage.StorageMgr
	cdnReporter *reporter
	metaDataMgr *metaDataManager
}

func newCacheWriter(cdnStore storage.StorageMgr, cdnReporter *reporter, metaDataMgr *metaDataManager) *cacheWriter {
	return &cacheWriter{
		cdnStore:    cdnStore,
		cdnReporter: cdnReporter,
		metaDataMgr: metaDataMgr,
	}
}

// startWriter writes the stream data from the reader to the underlying storage.
func (cw *cacheWriter) startWriter(ctx context.Context, reader io.Reader, task *types.SeedTask,
	detectResult *cacheResult) (*downloadMetadata, error) {
	// currentSourceFileLength is used to calculate the source file Length dynamically
	currentSourceFileLength := int64(detectResult.breakNum) * int64(task.PieceSize)
	// backSourceFileLength back source length
	var backSourceFileLength int64 = 0
	// the pieceNum currently processed
	curPieceNum := detectResult.breakNum
	// the left size of data for a complete piece
	pieceContLeft := task.PieceSize
	buf := make([]byte, 256*1024)
	var bb = &bytes.Buffer{}

	// start writer pool
	routineCount := calculateRoutineCount(task.SourceFileLength-currentSourceFileLength, task.PieceSize)
	var wg = &sync.WaitGroup{}
	jobCh := make(chan *protocolContent, 6)
	cw.writerPool(ctx, wg, routineCount, jobCh)

	for {
		n, err := reader.Read(buf)
		if n > 0 {
			backSourceFileLength += int64(n)
			if int(pieceContLeft) <= n {
				bb.Write(buf[:pieceContLeft])
				pc := &protocolContent{
					TaskId:       task.TaskId,
					pieceNum:     curPieceNum,
					pieceSize:    task.PieceSize,
					pieceContent: bb,
				}
				jobCh <- pc
				logger.WithTaskID(task.TaskId).Debugf("send protocolContent to jobCh, pieceNum: %d", curPieceNum)
				curPieceNum++

				// write the data left to a new buffer
				bb = bytes.NewBuffer([]byte{})
				n -= int(pieceContLeft)
				if n > 0 {
					bb.Write(buf[pieceContLeft : int(pieceContLeft)+n])
				}
				pieceContLeft = task.PieceSize
			} else {
				bb.Write(buf[:n])
			}
			pieceContLeft -= int32(n)
		}

		if err == io.EOF {
			if bb.Len() > 0 {
				pc := &protocolContent{
					TaskId:       task.TaskId,
					pieceNum:     curPieceNum,
					pieceSize:    task.PieceSize,
					pieceContent: bb,
				}
				jobCh <- pc
				curPieceNum++
				logger.WithTaskID(task.TaskId).Debugf("send the last protocolContent, pieceNum: %d", curPieceNum)
			}
			logger.WithTaskID(task.TaskId).Info("send all protocolContents and wait for cdnWriter")
			break
		}
		if err != nil {
			close(jobCh)
			// download fail
			return &downloadMetadata{backSourceLength: backSourceFileLength}, err
		}
	}

	close(jobCh)
	wg.Wait()

	storageInfo, err := cw.cdnStore.StatDownloadFile(ctx, task.TaskId)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get cdn file length")
	}

	pieceMd5Sign, _, err := cw.metaDataMgr.getPieceMd5Sign(ctx, task.TaskId)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get piece md5 sign")
	}
	return &downloadMetadata{
		backSourceLength:     backSourceFileLength,
		realCdnFileLength:    storageInfo.Size,
		realSourceFileLength: currentSourceFileLength + backSourceFileLength,
		pieceTotalCount:      curPieceNum,
		pieceMd5Sign:         pieceMd5Sign,
	}, nil
}
