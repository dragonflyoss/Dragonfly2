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

package localcdn

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/binary"
	logger "github.com/dragonflyoss/Dragonfly2/pkg/dflog"
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
				if err := cw.writeToFile(ctx, job.pieceContent, job.taskID, job.cdnFileOffset, job.pieceContentLen, pieceMd5); err != nil {
					logger.Errorf("failed to write taskID %s pieceNum %d file: %v", job.taskID, job.pieceNum, err)
					// NOTE: should we redo the job?
					continue
				}

				// report piece status
				pieceSum := fileutils.GetMd5Sum(pieceMd5, nil)
				pieceMetaRecord := pieceMetaRecord{
					PieceNum:  job.pieceNum,
					PieceLen:  job.pieceContentLen,
					Md5:       pieceSum,
					Range:     job.pieceRange,
					Offset:    job.sourceFileOffset,
				}
				if cw.cdnReporter != nil {
					// todo 写到channel中
					if err := cw.cdnReporter.pieceMetaDataManager.setPieceMetaRecord(job.taskID, job.pieceNum, pieceMetaRecord); err != nil {
						// NOTE: should we do this job again?
						logger.Errorf("failed to report piece status taskID %s pieceNum %d pieceMetaRecord %s: %v", job.taskID, job.pieceNum, pieceMetaRecord, err)
						continue
					}
				}
			}
			wg.Done()
		}(i)
	}
}

func (cw *cacheWriter) writeToFile(ctx context.Context, bytesBuffer *bytes.Buffer, taskID string, cdnFileOffset int64, pieceContSize int32, pieceMd5 hash.Hash) error {
	var resultBuf = &bytes.Buffer{}
	// write piece content
	var pieceContent []byte
	if pieceContSize > 0 {
		pieceContent = make([]byte, pieceContSize)
		if _, err := bytesBuffer.Read(pieceContent); err != nil {
			return err
		}
		bytesBuffer.Reset()
		binary.Write(resultBuf, binary.BigEndian, pieceContent)
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
		Length: int64(pieceContSize),
	}, resultBuf)
}
