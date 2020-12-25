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
	"fmt"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/store"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/types"
	logger "github.com/dragonflyoss/Dragonfly2/pkg/dflog"
	"io"
	"sync"
)

type protocolContent struct {
	taskID           string
	pieceNum         int32
	pieceStyle       string
	pieceContentLen  int32
	pieceRange       string
	pieceContent     *bytes.Buffer
	sourceFileOffset int64
	cdnFileOffset    int64
}

type downloadMetadata struct {
	realCdnFileLength    int64
	realSourceFileLength int64
	pieceCount           int32
}

type cacheWriter struct {
	cdnStore    *store.Store
	cdnReporter *reporter
}

func newCacheWriter(cdnStore *store.Store, cdnReporter *reporter) *cacheWriter {
	return &cacheWriter{
		cdnStore:    cdnStore,
		cdnReporter: cdnReporter,
	}
}

// startWriter writes the stream data from the reader to the underlying storage.
func (cw *cacheWriter) startWriter(ctx context.Context, reader io.Reader, task *types.SeedTask,
	detectResult *detectCacheResult) (*downloadMetadata, error) {
	// currentCdnFileLength is used to calculate the cdn file Length dynamically
	currentCdnFileLength := detectResult.downloadedFileLength
	// currentSourceFileLength is used to calculate the source file Length dynamically
	currentSourceFileLength := int64(detectResult.breakNum) * int64(task.PieceSize)
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
			logger.Debugf("success to read content with length: %d", n)
			currentSourceFileLength += int64(n)
			if int(pieceContLeft) <= n {
				bb.Write(buf[:pieceContLeft])
				pieceContentLen := int32(bb.Len())
				pc := &protocolContent{
					taskID:           task.TaskID,
					pieceNum:         curPieceNum,
					pieceContentLen:  pieceContentLen,
					pieceStyle:       "PLAIN_UNSPECIFIED",
					pieceContent:     bb,
					pieceRange:       fmt.Sprintf("%d-%d", currentCdnFileLength, currentCdnFileLength+int64(pieceContentLen-1)),
					cdnFileOffset:    currentCdnFileLength,
					sourceFileOffset: int64(curPieceNum) * int64(task.PieceSize),
				}
				currentCdnFileLength = currentCdnFileLength + int64(pieceContentLen)
				jobCh <- pc
				logger.Debugf("send the protocolContent taskID: %s pieceNum: %d", task.TaskID, curPieceNum)
				curPieceNum++

				// write the data left to a new buffer
				// todo recycling bytes.Buffer
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
			if currentCdnFileLength == 0 || bb.Len() > 0 {
				pieceContentLen := int32(bb.Len())
				jobCh <- &protocolContent{
					taskID:           task.TaskID,
					pieceNum:         curPieceNum,
					pieceContentLen:  pieceContentLen,
					pieceStyle:       "PLAIN_UNSPECIFIED",
					pieceContent:     bb,
					pieceRange:       fmt.Sprintf("%d-%d", currentCdnFileLength, currentCdnFileLength+int64(pieceContentLen)),
					sourceFileOffset: int64(curPieceNum) * int64(task.PieceSize),
					cdnFileOffset:    currentCdnFileLength,
				}
				logger.Debugf("send the protocolContent taskID: %s pieceNum: %d", task.TaskID, curPieceNum)
				currentCdnFileLength = currentCdnFileLength + int64(pieceContentLen)
			}
			logger.Infof("send all protocolContents with realCdnFileLength(%d) and wait for cdnWriter", currentCdnFileLength)
			break
		}
		if err != nil {
			close(jobCh)
			return nil, err
		}
	}

	close(jobCh)
	wg.Wait()
	return &downloadMetadata{
		realCdnFileLength:    currentCdnFileLength,
		realSourceFileLength: currentSourceFileLength,
		pieceCount:           curPieceNum,
	}, nil
}
