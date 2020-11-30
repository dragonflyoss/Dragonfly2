/*
 * Copyright The Dragonfly Authors.
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
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/types"
	"io"
	"sync"

	"github.com/dragonflyoss/Dragonfly2/cdnsystem/config"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/store"

	"github.com/sirupsen/logrus"
)

type protocolContent struct {
	taskID           string
	pieceNum         int
	pieceSize        int32
	pieceContentSize int32
	pieceContent     *bytes.Buffer
}

type downloadMetadata struct {
	realCdnFileLength    int64
	realSourceFileLength int64
	pieceCount           int
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
func (cw *cacheWriter) startWriter(ctx context.Context, cfg *config.Config, reader io.Reader,
	task *types.CdnTaskInfo, startPieceNum int, sourceFileLength int64, pieceSize int32) (*downloadMetadata, error) {
	// realCdnFileLength is used to calculate the cdn file Length dynamically
	realCdnFileLength := int64(startPieceNum) * int64(task.PieceSize)
	// realSourceFileLength is used to calculate the source file Length dynamically
	realSourceFileLength := int64(startPieceNum) * int64(pieceContSize)
	// the left size of data for a complete piece
	pieceContLeft := pieceSize
	// the pieceNum currently processed
	curPieceNum := startPieceNum

	buf := make([]byte, pieceSize)
	var bb = &bytes.Buffer{}

	// start writer pool
	routineCount := calculateRoutineCount(sourceFileLength, task.PieceSize)
	var wg = &sync.WaitGroup{}
	jobCh := make(chan *protocolContent)
	cw.writerPool(ctx, wg, routineCount, jobCh)

	for {
		n, e := reader.Read(buf)
		if n > 0 {
			logrus.Debugf("success to read content with length: %d", n)
			realCdnFileLength += int64(n)
			realSourceFileLength += int64(n)
			if int(pieceContLeft) <= n {
				bb.Write(buf[:pieceContLeft])
				pc := &protocolContent{
					taskID:           task.TaskID,
					pieceNum:         curPieceNum,
					pieceSize:        pieceSize,
					pieceContentSize: task.PieceSize,
					pieceContent:     bb,
				}
				jobCh <- pc
				logrus.Debugf("send the protocolContent taskID: %s pieceNum: %d", task.TaskID, curPieceNum)

				curPieceNum++

				// write the data left to a new buffer
				// TODO: recycling bytes.Buffer
				bb = bytes.NewBuffer([]byte{})
				n -= int(pieceContLeft)
				if n > 0 {
					bb.Write(buf[pieceContLeft : int(pieceContLeft)+n])
				}
				pieceContLeft = pieceContSize
			} else {
				bb.Write(buf[:n])
			}
			pieceContLeft -= int32(n)
		}

		if e == io.EOF {
			if realFileLength == 0 || bb.Len() > 0 {
				jobCh <- &protocolContent{
					taskID:           task.TaskID,
					pieceNum:         curPieceNum,
					pieceSize:        task.PieceSize,
					pieceContentSize: int32(bb.Len()),
					pieceContent:     bb,
				}
				logrus.Debugf("send the protocolContent taskID: %s pieceNum: %d", task.TaskID, curPieceNum)

				realFileLength += config.PieceWrapSize
			}
			logrus.Infof("send all protocolContents with realCdnFileLength(%d) and wait for cdnWriter", realCdnFileLength)
			break
		}
		if e != nil {
			close(jobCh)
			return nil, e
		}
	}

	close(jobCh)
	wg.Wait()
	return &downloadMetadata{
		realCdnFileLength:    realCdnFileLength,
		realSourceFileLength: realSourceFileLength,
		pieceCount:           curPieceNum,
	}, nil
}
