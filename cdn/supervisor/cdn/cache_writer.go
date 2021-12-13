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
	"encoding/json"
	"io"
	"sync"

	"github.com/pkg/errors"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/errgroup"

	"d7y.io/dragonfly/v2/cdn/constants"
	"d7y.io/dragonfly/v2/cdn/supervisor/cdn/storage"
	"d7y.io/dragonfly/v2/cdn/supervisor/task"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/ratelimiter/limitreader"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	"d7y.io/dragonfly/v2/pkg/util/digestutils"
	"d7y.io/dragonfly/v2/pkg/util/rangeutils"
)

type piece struct {
	taskID       string
	pieceNum     uint32
	pieceSize    uint32
	pieceContent *bytes.Buffer
}

type downloadMetadata struct {
	backSourceLength     int64 // back to source download file length
	realCdnFileLength    int64 // the actual length of the stored file
	realSourceFileLength int64 // actually read the length of the source
	totalPieceCount      int32 // total number of pieces
	pieceMd5Sign         string
	sourceRealDigest     string
}

type cacheWriter struct {
	cdnReporter     *reporter
	cacheStore      storage.Manager
	metadataManager *metadataManager
}

func newCacheWriter(cdnReporter *reporter, metadataManager *metadataManager, cacheStore storage.Manager) *cacheWriter {
	return &cacheWriter{
		cdnReporter:     cdnReporter,
		cacheStore:      cacheStore,
		metadataManager: metadataManager,
	}
}

// startWriter writes the stream data from the reader to the underlying storage.
func (cw *cacheWriter) startWriter(ctx context.Context, reader *limitreader.LimitReader, seedTask *task.SeedTask, breakPoint int64,
	writerRoutineLimit int) (*downloadMetadata, error) {
	var writeSpan trace.Span
	ctx, writeSpan = tracer.Start(ctx, constants.SpanWriteData)
	defer writeSpan.End()
	// currentSourceFileLength is used to calculate the source file Length dynamically
	currentSourceFileLength := breakPoint
	routineCount := calculateRoutineCount(seedTask.SourceFileLength-currentSourceFileLength, seedTask.PieceSize, writerRoutineLimit)
	writeSpan.SetAttributes(constants.AttributeWriteGoroutineCount.Int(routineCount))
	// start writer pool
	backSourceLength, totalPieceCount, err := cw.doWrite(ctx, reader, seedTask, routineCount, breakPoint)
	if err != nil {
		return &downloadMetadata{backSourceLength: backSourceLength}, errors.Wrap(err, "do write data action")
	}
	storageInfo, err := cw.cacheStore.StatDownloadFile(seedTask.ID)
	if err != nil {
		return &downloadMetadata{backSourceLength: backSourceLength}, errors.Wrap(err, "stat cdn download file")
	}
	storageInfoBytes, _ := json.Marshal(storageInfo)
	writeSpan.SetAttributes(constants.AttributeDownloadFileInfo.String(string(storageInfoBytes)))
	// TODO Try getting it from the ProgressManager first
	pieceMd5Sign, _, err := cw.metadataManager.getPieceMd5Sign(seedTask.ID)
	if err != nil {
		return &downloadMetadata{backSourceLength: backSourceLength}, errors.Wrap(err, "get piece md5 sign")
	}
	return &downloadMetadata{
		backSourceLength:     backSourceLength,
		realCdnFileLength:    storageInfo.Size,
		realSourceFileLength: currentSourceFileLength + backSourceLength,
		totalPieceCount:      totalPieceCount,
		pieceMd5Sign:         pieceMd5Sign,
		sourceRealDigest:     reader.Digest(),
	}, nil
}

// doWrite do actual write data to storage
func (cw *cacheWriter) doWrite(ctx context.Context, reader io.Reader, seedTask *task.SeedTask, routineCount int, breakPoint int64) (n int64, totalPiece int32,
	err error) {
	// the pieceNum currently have been processed
	curPieceNum := int32(breakPoint / int64(seedTask.PieceSize))
	seedTask.Log().Infof("start writing resource to storage from piece %d", curPieceNum)
	var bufPool = &sync.Pool{
		New: func() interface{} {
			return new(bytes.Buffer)
		},
	}
	var backSourceLength int64
	buf := make([]byte, 256*1024)
	jobCh := make(chan *piece)
	var g, writeCtx = errgroup.WithContext(ctx)
	cw.writerPool(writeCtx, g, routineCount, jobCh, bufPool)
loop:
	for {
		select {
		case <-writeCtx.Done():
			break loop
		default:
			var bb = bufPool.Get().(*bytes.Buffer)
			bb.Reset()
			limitReader := io.LimitReader(reader, int64(seedTask.PieceSize))
			n, err = io.CopyBuffer(bb, limitReader, buf)
			if err != nil {
				close(jobCh)
				return backSourceLength, 0, errors.Errorf("read taskID %s pieceNum %d piece from source failed: %v", seedTask.ID, curPieceNum, err)
			}
			seedTask.Log().Debugf("success read %d bytes from source", n)
			if n == 0 {
				break loop
			}
			backSourceLength += n

			jobCh <- &piece{
				taskID:       seedTask.ID,
				pieceNum:     uint32(curPieceNum),
				pieceSize:    uint32(seedTask.PieceSize),
				pieceContent: bb,
			}
			curPieceNum++
			if n < int64(seedTask.PieceSize) {
				break loop
			}
		}
	}
	close(jobCh)
	seedTask.Log().Infof("finish read from resource, backSourceLength %d", backSourceLength)
	if err := g.Wait(); err != nil {
		return backSourceLength, 0, errors.Wrapf(err, "write pool")
	}
	return backSourceLength, curPieceNum, nil
}

func (cw *cacheWriter) writerPool(ctx context.Context, g *errgroup.Group, routineCount int, pieceCh chan *piece, bufPool *sync.Pool) {
	for i := 0; i < routineCount; i++ {
		g.Go(func() error {
			for p := range pieceCh {
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
					// TODO Subsequent compression and other features are implemented through waitToWriteContent and pieceStyle
					waitToWriteContent := p.pieceContent
					originPieceLen := waitToWriteContent.Len() // the length of the original data that has not been processed
					pieceLen := originPieceLen                 // the real length written to the storage driver after processed
					pieceStyle := int32(base.PieceStyle_PLAIN.Number())
					pieceMd5 := md5.New()
					err := cw.cacheStore.WriteDownloadFile(
						p.taskID, int64(p.pieceNum)*int64(p.pieceSize), int64(waitToWriteContent.Len()),
						io.TeeReader(io.LimitReader(p.pieceContent, int64(waitToWriteContent.Len())), pieceMd5))
					if err != nil {
						return errors.Errorf("write taskID %s pieceNum %d to download file failed: %v", p.taskID, p.pieceNum, err)
					}
					logger.WithTaskID(p.taskID).Debugf("success write pieceNum %d content to storage", p.pieceNum)
					// Recycle Buffer
					bufPool.Put(waitToWriteContent)
					start := uint64(p.pieceNum) * uint64(p.pieceSize)
					end := start + uint64(pieceLen) - 1
					pieceRecord := &storage.PieceMetaRecord{
						PieceNum: p.pieceNum,
						PieceLen: uint32(pieceLen),
						Md5:      digestutils.ToHashString(pieceMd5),
						Range: &rangeutils.Range{
							StartIndex: start,
							EndIndex:   end,
						},
						OriginRange: &rangeutils.Range{
							StartIndex: start,
							EndIndex:   end,
						},
						PieceStyle: pieceStyle,
					}
					// write piece meta to storage
					if err = cw.metadataManager.appendPieceMetadata(p.taskID, pieceRecord); err != nil {
						return errors.Errorf("write piece meta to piece meta file failed: %v", err)
					}
					logger.WithTaskID(p.taskID).Debugf("success write pieceNum %d meta to storage", p.pieceNum)
					// report piece info
					if err = cw.cdnReporter.reportPieceMetaRecord(ctx, p.taskID, pieceRecord, DownloaderReport); err != nil {
						// NOTE: should we do this job again?
						return errors.Errorf("report piece status, pieceNum %d pieceMetaRecord %s: %v", p.pieceNum, pieceRecord, err)
					}
				}
			}
			return nil
		})
	}
}

/*
	helper functions
*/

// calculateRoutineCount max goroutine count is CDNWriterRoutineLimit
func calculateRoutineCount(remainingFileLength int64, pieceSize int32, writerRoutineLimit int) int {
	routineSize := writerRoutineLimit
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
