
package localcdn

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/binary"
	"hash"
	"sync"

	"github.com/dragonflyoss/Dragonfly2/cdnsystem/config"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/store"
	"github.com/dragonflyoss/Dragonfly2/pkg/fileutils"

	"github.com/sirupsen/logrus"
)
// calculate need how many goroutine
func calculateRoutineCount(sourceFileLength int64, pieceSize int32) int {
	routineSize := config.CDNWriterRoutineLimit
	if sourceFileLength < 0 || pieceSize <= 0 {
		return routineSize
	}

	if sourceFileLength == 0 {
		return 1
	}

	tmpSize := (int)((sourceFileLength + int64(pieceSize-1)) / int64(pieceSize))
	if tmpSize == 0 {
		tmpSize = 1
	}
	if tmpSize < routineSize {
		routineSize = tmpSize
	}
	return routineSize
}

func (cw *cacheWriter) writerPool(ctx context.Context, wg *sync.WaitGroup, n int, jobCh chan *protocolContent) {
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			for job := range jobCh {
				var pieceMd5 = md5.New()
				if err := cw.writeToFile(ctx, job.pieceContent, job.taskID, job.pieceNum, job.pieceContentSize, job.pieceSize, pieceMd5); err != nil {
					logrus.Errorf("failed to write taskID %s pieceNum %d file: %v", job.taskID, job.pieceNum, err)
					// NOTE: should we redo the job?
					continue
				}

				// report piece status
				pieceSum := fileutils.GetMd5Sum(pieceMd5, nil)
				pieceMd5Value := pieceMetaRecord{
					PieceNum:  job.pieceNum,
					PieceLen:  job.pieceContentSize,
					Md5:       pieceSum,
					Range:     "",
					Offset:    0,
					StartByte: job,
					EndByte:   0,
				}
				if cw.cdnReporter != nil {
					if err := cw.cdnReporter.reportPieceStatus(ctx, job.taskID, job.pieceNum, pieceMd5Value, config.PieceSUCCESS); err != nil {
						// NOTE: should we do this job again?
						logrus.Errorf("failed to report piece status taskID %s pieceNum %d pieceMD5 %s: %v", job.taskID, job.pieceNum, pieceMd5Value, err)
						continue
					}
				}
			}
			wg.Done()
		}(i)
	}
}

// writeToFile wraps the piece content with piece header and tailer,
// and then writes to the storage.
func (cw *cacheWriter) writeToFile(ctx context.Context, bytesBuffer *bytes.Buffer, taskID string, pieceNum int, pieceContSize, pieceSize int32, pieceMd5 hash.Hash) error {
	var resultBuf = &bytes.Buffer{}

	// write piece header
	var header = make([]byte, 4)
	binary.BigEndian.PutUint32(header, getPieceHeader(pieceContSize, pieceSize))
	resultBuf.Write(header)

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

	// write piece tailer
	tailer := []byte{config.PieceTailChar}
	binary.Write(resultBuf, binary.BigEndian, tailer)

	if pieceMd5 != nil {
		pieceMd5.Write(header)
		if len(pieceContent) > 0 {
			pieceMd5.Write(pieceContent)
		}
		pieceMd5.Write(tailer)
	}
	// write to the storage
	return cw.cdnStore.Put(ctx, &store.Raw{
		Bucket: config.DownloadHome,
		Key:    getDownloadKey(taskID),
		Offset: int64(pieceNum) * int64(pieceSize),
		Length: int64(pieceContSize) + config.PieceWrapSize,
	}, resultBuf)
}
