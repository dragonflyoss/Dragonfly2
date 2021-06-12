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
	"io"

	"d7y.io/dragonfly/v2/cdnsystem/config"
	"d7y.io/dragonfly/v2/cdnsystem/daemon/cdn/storage"
	"d7y.io/dragonfly/v2/cdnsystem/types"
	logger "d7y.io/dragonfly/v2/pkg/dflog"
	"d7y.io/dragonfly/v2/pkg/sync/work"
	"d7y.io/dragonfly/v2/pkg/util/digestutils"
	"d7y.io/dragonfly/v2/pkg/util/rangeutils"
	"github.com/pkg/errors"
)

type protocolContent struct {
	TaskID       string
	pieceNum     int32
	pieceSize    int32
	pieceContent *bytes.Buffer
}

type writePieceWorker struct {
	pc *protocolContent
	cw *cacheWriter
}

var _ work.Worker = (*writePieceWorker)(nil)

func (worker *writePieceWorker) Work() {
	var job = worker.pc
	// todo Subsequent compression and other features are implemented through waitToWriteContent and pieceStyle
	waitToWriteContent := job.pieceContent
	originPieceLen := waitToWriteContent.Len() // the length of the original data that has not been processed
	pieceLen := originPieceLen                 // the real length written to the storage medium after processing
	pieceStyle := types.PlainUnspecified
	pieceMd5Sum := digestutils.Md5Bytes(waitToWriteContent.Bytes())

	if err := worker.cw.cacheDataManager.writeDownloadFile(job.TaskID, int64(job.pieceNum)*int64(job.pieceSize), int64(waitToWriteContent.Len()),
		waitToWriteContent); err != nil {
		logger.WithTaskID(job.TaskID).Errorf("failed to write file, pieceNum %d: %v", job.pieceNum, err)
		return
	}
	pieceRecord := &storage.PieceMetaRecord{
		PieceNum: job.pieceNum,
		PieceLen: int32(pieceLen),
		Md5:      pieceMd5Sum,
		Range: &rangeutils.Range{
			StartIndex: uint64(job.pieceNum * job.pieceSize),
			EndIndex:   uint64(job.pieceNum*job.pieceSize + int32(pieceLen) - 1),
		},
		OriginRange: &rangeutils.Range{
			StartIndex: uint64(job.pieceNum * job.pieceSize),
			EndIndex:   uint64(job.pieceNum*job.pieceSize + int32(originPieceLen) - 1),
		},
		PieceStyle: pieceStyle,
	}
	// write piece meta to storage

	if err := worker.cw.cacheDataManager.appendPieceMetaData(job.TaskID, pieceRecord); err != nil {
		logger.WithTaskID(job.TaskID).Errorf("failed to append piece meta data to file:%v", err)
	}

	if worker.cw.cdnReporter != nil {
		if err := worker.cw.cdnReporter.reportPieceMetaRecord(job.TaskID, pieceRecord, DownloaderReport); err != nil {
			// NOTE: should we do this job again?
			logger.WithTaskID(job.TaskID).Errorf("failed to report piece status, pieceNum %d pieceMetaRecord %s: %v", job.pieceNum, pieceRecord, err)
		}
	}
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
	// currentSourceFileLength is used to calculate the source file Length dynamically
	currentSourceFileLength := detectResult.breakPoint
	// backSourceFileLength back source length
	var backSourceFileLength int64 = 0
	// the pieceNum currently processed
	curPieceNum := int32(len(detectResult.pieceMetaRecords))
	// the left size of data for a complete piece
	pieceContLeft := task.PieceSize
	buf := make([]byte, 256*1024)
	var bb = new(bytes.Buffer)

	// start writer pool
	routineCount := calculateRoutineCount(task.SourceFileLength-currentSourceFileLength, task.PieceSize)
	p := work.New(routineCount)

	for {
		n, err := reader.Read(buf)
		if n > 0 {
			backSourceFileLength += int64(n)
			if int(pieceContLeft) <= n {
				bb.Write(buf[:pieceContLeft])
				p.Run(&writePieceWorker{
					pc: &protocolContent{
						TaskID:       task.TaskID,
						pieceNum:     curPieceNum,
						pieceSize:    task.PieceSize,
						pieceContent: bb,
					},
					cw: cw,
				})
				logger.WithTaskID(task.TaskID).Debugf("submit a protocolContent to worker pool, pieceNum: %d", curPieceNum)
				curPieceNum++

				// write the data left to a new buffer
				bb = new(bytes.Buffer)
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
				p.Run(&writePieceWorker{
					pc: &protocolContent{
						TaskID:       task.TaskID,
						pieceNum:     curPieceNum,
						pieceSize:    task.PieceSize,
						pieceContent: bb,
					},
					cw: cw,
				})
				curPieceNum++
				logger.WithTaskID(task.TaskID).Debugf("submit the last protocolContent to worker pool, pieceNum: %d", curPieceNum)
			}
			logger.WithTaskID(task.TaskID).Info("success send all protocolContents to worker pool and wait for cdnWriter")
			break
		}
		if err != nil {
			// download fail
			p.Shutdown()
			return &downloadMetadata{backSourceLength: backSourceFileLength}, err
		}
	}
	p.Shutdown()

	storageInfo, err := cw.cacheDataManager.statDownloadFile(task.TaskID)
	if err != nil {
		return &downloadMetadata{backSourceLength: backSourceFileLength}, errors.Wrapf(err, "failed to get cdn file length")
	}

	pieceMd5Sign, _, err := cw.cacheDataManager.getPieceMd5Sign(task.TaskID)
	if err != nil {
		return &downloadMetadata{backSourceLength: backSourceFileLength}, errors.Wrapf(err, "failed to get piece md5 sign")
	}
	return &downloadMetadata{
		backSourceLength:     backSourceFileLength,
		realCdnFileLength:    storageInfo.Size,
		realSourceFileLength: currentSourceFileLength + backSourceFileLength,
		pieceTotalCount:      curPieceNum,
		pieceMd5Sign:         pieceMd5Sign,
	}, nil
}

/*
   helper functions
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
