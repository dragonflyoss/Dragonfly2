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
	"d7y.io/dragonfly/v2/cdnsystem/config"
	"d7y.io/dragonfly/v2/cdnsystem/store"
	"d7y.io/dragonfly/v2/cdnsystem/types"
	logger "d7y.io/dragonfly/v2/pkg/dflog"
	"d7y.io/dragonfly/v2/pkg/util/fileutils"
	"encoding/binary"
	"fmt"
	"github.com/pkg/errors"
	"hash"
	"sync"
)

// calculateRoutineCount calculate how many goroutines are needed to execute write goroutine
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

// writerPool
func (cw *cacheWriter) writerPool(ctx context.Context, wg *sync.WaitGroup, writeRoutineCount int, jobCh chan *protocolContent) {
	for i := 0; i < writeRoutineCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobCh {
				var pieceMd5 = md5.New()
				// todo 后续压缩等特性通过waitToWriteContent 和 pieceStyle 实现
				waitToWriteContent := job.pieceContent
				pieceLen := waitToWriteContent.Len()
				pieceStyle := types.PlainUnspecified

				if err := cw.writeToFile(ctx, job.taskID, waitToWriteContent, int64(job.pieceNum)*int64(job.pieceSize), pieceMd5); err != nil {
					logger.WithTaskID(job.taskID).Errorf("failed to write file, pieceNum %d: %v", job.pieceNum, err)
					// todo redo the job?
					continue
				}
				// report piece status
				pieceMd5Sum := fileutils.GetMd5Sum(pieceMd5, nil)
				pieceRecord := &pieceMetaRecord{
					PieceNum:   job.pieceNum,
					PieceLen:   int32(pieceLen),
					Md5:        pieceMd5Sum,
					Range:      fmt.Sprintf("%d-%d", job.pieceNum*job.pieceSize, job.pieceNum*job.pieceSize+int32(pieceLen)-1),
					Offset:     uint64(job.pieceNum) * uint64(job.pieceSize),
					PieceStyle: pieceStyle,
				}
				wg.Add(1)
				// write piece meta to storage
				go func(record *pieceMetaRecord) {
					defer wg.Done()
					// todo 可以先塞入channel，然后启动单独goroutine顺序写文件
					if err := cw.metaDataMgr.appendPieceMetaDataToFile(ctx, job.taskID, record); err != nil {
						logger.WithTaskID(job.taskID).Errorf("failed to append piece meta data to file:%v", err)
					}
				}(pieceRecord)

				if cw.cdnReporter != nil {
					if err := cw.cdnReporter.reportPieceMetaRecord(job.taskID, pieceRecord); err != nil {
						// NOTE: should we do this job again?
						logger.WithTaskID(job.taskID).Errorf("failed to report piece status, pieceNum %d pieceMetaRecord %s: %v", job.pieceNum, pieceRecord, err)
						continue
					}
				}
			}
		}()
	}
}

// writeToFile
func (cw *cacheWriter) writeToFile(ctx context.Context, taskID string, bytesBuffer *bytes.Buffer, offset int64, pieceMd5 hash.Hash) error {
	var resultBuf = &bytes.Buffer{}
	// write piece content
	var pieceContent []byte
	pieceContLen := bytesBuffer.Len()
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
		Offset: offset,
		Length: int64(pieceContLen),
	}, resultBuf)
}
