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
	"crypto/md5"
	"encoding/binary"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/types"
	logger "github.com/dragonflyoss/Dragonfly2/pkg/dflog"
	"github.com/pkg/errors"
	"hash"
	"sync"

	"github.com/dragonflyoss/Dragonfly2/cdnsystem/config"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/store"
	"github.com/dragonflyoss/Dragonfly2/pkg/util/fileutils"
)

// calculate need how many goroutine
func calculateRoutineCount(remainingFileLength int64, pieceSize int32) int {
	routineSize := config.CDNWriterRoutineLimit
	if remainingFileLength < 0 || pieceSize <= 0 {
		return routineSize
	}

	if remainingFileLength == 0 {
		return 1
	}

	tmpSize := (int)((remainingFileLength + int64(pieceSize-1)) / int64(pieceSize))
	if tmpSize == 0 {
		tmpSize = 1
	}
	if tmpSize < routineSize {
		routineSize = tmpSize
	}
	return routineSize
}

func (cw *cacheWriter) writerPool(ctx context.Context, wg *sync.WaitGroup, writeRoutineCount int, jobCh chan *protocolContent) {
	for i := 0; i < writeRoutineCount; i++ {
		wg.Add(1)
		go func(i int) {
			for job := range jobCh {
				var pieceMd5 = md5.New()
				if err := cw.writeToFile(ctx, job.taskID, job.pieceContent, job.cdnFileOffset, job.pieceContentLen, pieceMd5); err != nil {
					logger.Named(job.taskID).Errorf("failed to write file, pieceNum %d file: %v", job.pieceNum, err)
					// todo redo the job?
					continue
				}

				// report piece status
				pieceMd5Sum := fileutils.GetMd5Sum(pieceMd5, nil)
				pieceRecord := pieceMetaRecord{
					PieceNum:   job.pieceNum,
					PieceLen:   job.pieceContentLen,
					Md5:        pieceMd5Sum,
					Range:      job.pieceRange,
					Offset:     job.sourceFileOffset,
					PieceStyle: types.PlainUnspecified,
				}
				// write piece meta
				go func(record pieceMetaRecord) {
					if err := cw.appendPieceMetaDataToFile(ctx, job.taskID, record); err != nil {
						logger.Named(job.taskID).Errorf("failed to append piece meta data to file:%v", err)
					}
				}(pieceRecord)

				if cw.cdnReporter != nil {
					if err := cw.cdnReporter.reportPieceMetaRecord(job.taskID, pieceRecord); err != nil {
						// NOTE: should we do this job again?
						logger.Named(job.taskID).Errorf("failed to report piece status, pieceNum %d pieceMetaRecord %s: %v", job.pieceNum, pieceRecord, err)
						continue
					}
				}
			}
			wg.Done()
		}(i)
	}
}

func (cw *cacheWriter) writeToFile(ctx context.Context, taskID string, bytesBuffer *bytes.Buffer, cdnFileOffset int64, pieceContLen int32, pieceMd5 hash.Hash) error {
	var resultBuf = &bytes.Buffer{}
	// write piece content
	var pieceContent []byte
	if pieceContLen > 0 {
		pieceContent = make([]byte, pieceContLen)
		if _, err := bytesBuffer.Read(pieceContent); err != nil {
			return err
		}
		bytesBuffer.Reset()
		if err := binary.Write(resultBuf, binary.BigEndian, pieceContent); err != nil {
			return errors.Wrapf(err, "write data file fail")
		}
	}
	if pieceMd5 != nil {
		if len(pieceContent) > 0 {
			pieceMd5.Write(pieceContent)
		}
	}
	// write to the storage
	return cw.cdnStore.Put(ctx, &store.Raw{
		Bucket: config.DownloadHome,
		Key:    getDownloadKey(taskID),
		Offset: cdnFileOffset,
		Length: int64(pieceContLen),
	}, resultBuf)
}

func (cw *cacheWriter) appendPieceMetaDataToFile(ctx context.Context, taskID string, record pieceMetaRecord) error {
	pieceMeta := getPieceMetaValue(record)
	// write to the storage
	return cw.cdnStore.AppendBytes(ctx, &store.Raw{
		Bucket: config.DownloadHome,
		Key:    getPieceMetaDataKey(taskID),
	}, []byte(pieceMeta+"\n"))
}
