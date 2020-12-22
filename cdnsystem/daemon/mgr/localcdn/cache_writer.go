package localcdn

import (
	"bytes"
	"context"
	"fmt"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/types"
	"io"
	"sync"

	"github.com/dragonflyoss/Dragonfly2/cdnsystem/store"

	"github.com/sirupsen/logrus"
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
func (cw *cacheWriter) startWriter(ctx context.Context, reader io.Reader, task *types.SeedTaskInfo,
	detectResult *detectCacheResult) (*downloadMetadata, error) {
	defer func() {
		// todo go routine write pieceMeta
	}()
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
	jobCh := make(chan *protocolContent)
	cw.writerPool(ctx, wg, routineCount, jobCh)

	for {
		n, e := reader.Read(buf)
		if n > 0 {
			logrus.Debugf("success to read content with length: %d", n)
			currentSourceFileLength += int64(n)
			if int(pieceContLeft) <= n {
				bb.Write(buf[:pieceContLeft])
				pieceContentLen := int32(bb.Len())
				pc := &protocolContent{
					taskID:           task.TaskID,
					pieceNum:         curPieceNum,
					pieceContentLen:  pieceContentLen,
					pieceStyle:       "raw",
					pieceContent:     bb,
					pieceRange:       fmt.Sprintf("%d-%d", currentCdnFileLength, currentCdnFileLength+int64(pieceContentLen-1)),
					cdnFileOffset:    currentCdnFileLength,
					sourceFileOffset: int64(curPieceNum) * int64(task.PieceSize),
				}
				currentCdnFileLength = currentCdnFileLength + int64(pieceContentLen)
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
				pieceContLeft = task.PieceSize
			} else {
				bb.Write(buf[:n])
			}
			pieceContLeft -= int32(n)
		}

		if e == io.EOF {
			if currentCdnFileLength == 0 || bb.Len() > 0 {
				pieceContentLen := int32(bb.Len())
				jobCh <- &protocolContent{
					taskID:           task.TaskID,
					pieceNum:         curPieceNum,
					pieceContentLen:  pieceContentLen,
					pieceStyle:       "raw",
					pieceContent:     bb,
					pieceRange:       fmt.Sprintf("%d-%d", currentCdnFileLength, currentCdnFileLength+int64(pieceContentLen)),
					sourceFileOffset: int64(curPieceNum) * int64(task.PieceSize),
					cdnFileOffset:    currentCdnFileLength,
				}
				logrus.Debugf("send the protocolContent taskID: %s pieceNum: %d", task.TaskID, curPieceNum)
				currentCdnFileLength = currentCdnFileLength + int64(pieceContentLen)
			}
			logrus.Infof("send all protocolContents with realCdnFileLength(%d) and wait for cdnWriter", currentCdnFileLength)
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
		realCdnFileLength:    currentCdnFileLength,
		realSourceFileLength: currentSourceFileLength,
		pieceCount:           curPieceNum,
	}, nil
}
