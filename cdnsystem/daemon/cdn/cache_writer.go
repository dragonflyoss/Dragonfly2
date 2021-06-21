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
	"crypto/md5"
	"fmt"
	"io"
	"sync"

	"d7y.io/dragonfly/v2/cdnsystem/config"
	"d7y.io/dragonfly/v2/cdnsystem/daemon/cdn/storage"
	"d7y.io/dragonfly/v2/cdnsystem/types"
	logger "d7y.io/dragonfly/v2/pkg/dflog"
	"d7y.io/dragonfly/v2/pkg/util/digestutils"
	"d7y.io/dragonfly/v2/pkg/util/rangeutils"
)

type piece struct {
	taskID       string
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
	cdnReporter      *reporter
	cacheDataManager *cacheDataManager
}

func newCacheWriter(cdnReporter *reporter, cacheDataManager *cacheDataManager) *cacheWriter {
	return &cacheWriter{
		cdnReporter:      cdnReporter,
		cacheDataManager: cacheDataManager,
	}
}

// startWriter writes the stream data from the reader to the underlying storage.
func (cw *cacheWriter) startWriter(reader io.Reader, task *types.SeedTask, detectResult *cacheResult) (*downloadMetadata, error) {
	if detectResult == nil {
		detectResult = &cacheResult{}
	}
	// currentSourceFileLength is used to calculate the source file Length dynamically
	currentSourceFileLength := detectResult.breakPoint
	// the pieceNum currently have been processed
	curPieceNum := len(detectResult.pieceMetaRecords)
	routineCount := calculateRoutineCount(task.SourceFileLength-currentSourceFileLength, task.PieceSize)
	// start writer pool
	backSourceFileLength, totalPieceCount, err := cw.doWrite(reader, task, routineCount, curPieceNum)
	if err != nil {
		return &downloadMetadata{backSourceLength: backSourceFileLength}, fmt.Errorf("write data: %v", err)
	}
	storageInfo, err := cw.cacheDataManager.statDownloadFile(task.TaskID)
	if err != nil {
		return &downloadMetadata{backSourceLength: backSourceFileLength}, fmt.Errorf("stat cdn download file: %v", storageInfo)
	}
	// todo Try getting it from the ProgressManager first
	pieceMd5Sign, _, err := cw.cacheDataManager.getPieceMd5Sign(task.TaskID)
	if err != nil {
		return &downloadMetadata{backSourceLength: backSourceFileLength}, fmt.Errorf("get piece md5 sign: %v", pieceMd5Sign)
	}
	return &downloadMetadata{
		backSourceLength:     backSourceFileLength,
		realCdnFileLength:    storageInfo.Size,
		realSourceFileLength: currentSourceFileLength + backSourceFileLength,
		pieceTotalCount:      int32(totalPieceCount),
		pieceMd5Sign:         pieceMd5Sign,
	}, nil
}

func (cw *cacheWriter) doWrite(reader io.Reader, task *types.SeedTask, routineCount int, curPieceNum int) (n int64, totalPiece int, err error) {
	var bufPool = &sync.Pool{
		New: func() interface{} {
			return new(bytes.Buffer)
		},
	}
	var backSourceFileLength int64
	buf := make([]byte, 256*1024)
	jobCh := make(chan *piece)
	var wg = &sync.WaitGroup{}
	cw.writerPool(wg, routineCount, jobCh, bufPool)
	for {
		var bb = bufPool.Get().(*bytes.Buffer)
		bb.Reset()
		limitReader := io.LimitReader(reader, int64(task.PieceSize))
		n, err := io.CopyBuffer(bb, limitReader, buf)
		if err != nil {
			close(jobCh)
			return backSourceFileLength, 0, fmt.Errorf("read source taskID %s pieceNum %d piece: %v", task.TaskID, curPieceNum, err)
		}
		backSourceFileLength = backSourceFileLength + n

		jobCh <- &piece{
			taskID:       task.TaskID,
			pieceNum:     int32(curPieceNum),
			pieceSize:    task.PieceSize,
			pieceContent: bb,
		}
		curPieceNum++
		if n < int64(task.PieceSize) {
			break
		}
	}
	close(jobCh)
	wg.Wait()
	return backSourceFileLength, curPieceNum, nil
}

func (cw *cacheWriter) writerPool(wg *sync.WaitGroup, routineCount int, pieceCh chan *piece, bufPool *sync.Pool) {
	wg.Add(routineCount)
	for i := 0; i < routineCount; i++ {
		go func() {
			defer wg.Done()
			for piece := range pieceCh {
				// todo Subsequent compression and other features are implemented through waitToWriteContent and pieceStyle
				waitToWriteContent := piece.pieceContent
				originPieceLen := waitToWriteContent.Len() // the length of the original data that has not been processed
				pieceLen := originPieceLen                 // the real length written to the storage medium after processing
				pieceStyle := types.PlainUnspecified
				pieceMd5 := md5.New()
				err := cw.cacheDataManager.writeDownloadFile(piece.taskID, int64(piece.pieceNum)*int64(piece.pieceSize), int64(waitToWriteContent.Len()),
					io.TeeReader(io.LimitReader(piece.pieceContent, int64(waitToWriteContent.Len())), pieceMd5))
				// Recycle Buffer
				bufPool.Put(waitToWriteContent)
				if err != nil {
					logger.Errorf("write taskID %s pieceNum %d file: %v", piece.taskID, piece.pieceNum, err)
					continue
				}
				pieceRecord := &storage.PieceMetaRecord{
					PieceNum: piece.pieceNum,
					PieceLen: int32(pieceLen),
					Md5:      digestutils.ToHashString(pieceMd5),
					Range: &rangeutils.Range{
						StartIndex: uint64(piece.pieceNum * piece.pieceSize),
						EndIndex:   uint64(piece.pieceNum*piece.pieceSize + int32(pieceLen) - 1),
					},
					OriginRange: &rangeutils.Range{
						StartIndex: uint64(piece.pieceNum * piece.pieceSize),
						EndIndex:   uint64(piece.pieceNum*piece.pieceSize + int32(originPieceLen) - 1),
					},
					PieceStyle: pieceStyle,
				}
				// write piece meta to storage
				if err := cw.cacheDataManager.appendPieceMetaData(piece.taskID, pieceRecord); err != nil {
					logger.Errorf("write piece meta file: %v", err)
					continue
				}

				if cw.cdnReporter != nil {
					if err := cw.cdnReporter.reportPieceMetaRecord(piece.taskID, pieceRecord, DownloaderReport); err != nil {
						// NOTE: should we do this job again?
						logger.Errorf("report piece status, pieceNum %d pieceMetaRecord %s: %v", piece.pieceNum, pieceRecord, err)
					}
				}
			}
		}()
	}
}

/*
 	helper functions
	max goroutine count is CDNWriterRoutineLimit
*/
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
