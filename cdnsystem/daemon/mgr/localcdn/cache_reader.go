package localcdn

import (
	"context"
	"crypto/md5"
	"encoding/binary"
	"fmt"
	"hash"
	"io"
	"sort"

	"github.com/dragonflyoss/Dragonfly2/pkg/fileutils"
	"github.com/dragonflyoss/Dragonfly2/pkg/util"

	"github.com/sirupsen/logrus"
)

// meta info already downloaded
type cdnCacheResult struct {
	breakNum         int
	fileLength       int64             // length of file has been downloaded
	pieceMetaRecords []pieceMetaRecord // piece meta data of taskId
	fileMd5          hash.Hash         // md5 of content that has been downloaded
}

type cacheReader struct {
}

func newCacheReader() *cacheReader {
	return &cacheReader{}
}

func (sr *cacheReader) readFile(ctx context.Context, reader io.Reader, pieceMetaRecords []pieceMetaRecord) (result *cdnCacheResult, err error) {
	result = &cdnCacheResult{}
	result.fileMd5 = md5.New()
	sort.Slice(pieceMetaRecords, func(i, j int) bool {
		return pieceMetaRecords[i].PieceNum < pieceMetaRecords[j].PieceNum
	})
	var breakIndex int
	for index, record := range pieceMetaRecords {
		if index != record.PieceNum {
			breakIndex = index
			break
		}
		// read content
		if err := readContent(reader, record, result.fileMd5); err != nil {
			logrus.Errorf("failed to read content for count %d: %v", index, err)
			breakIndex = index
			break
		}
		result.fileLength += int64(record.PieceLen)
	}
	result.pieceMetaRecords = pieceMetaRecords[0:breakIndex]
	result.breakNum = breakIndex
	return result, nil
}

//readContent read piece content
func readContent(reader io.Reader, pieceMetaRecord pieceMetaRecord, fileMd5 hash.Hash) error {
	bufSize := int32(256 * 1024)
	pieceLen := pieceMetaRecord.PieceLen
	if pieceLen < bufSize {
		bufSize = pieceLen
	}
	pieceContent := make([]byte, bufSize)
	var curContent int32
	pieceMd5 := md5.New()
	var start, end byte
	for {
		if curContent+bufSize <= pieceLen {
			if err := binary.Read(reader, binary.BigEndian, pieceContent); err != nil {
				return err
			}
			// the first byte of piece content
			if curContent == 0 {
				start = pieceContent[0]
			}
			curContent += bufSize
			// the end byte of piece content
			if curContent == pieceLen {
				end = pieceContent[bufSize-1]
			}
			// calculate the md5
			pieceMd5.Write(pieceContent)

			if !util.IsNil(fileMd5) {
				fileMd5.Write(pieceContent)
			}
		} else {
			readLen := pieceLen - curContent
			if err := binary.Read(reader, binary.BigEndian, pieceContent[:readLen]); err != nil {
				return err
			}
			curContent += readLen
			// the end byte of piece content
			end = pieceContent[readLen-1]
			// calculate the md5
			pieceMd5.Write(pieceContent[:readLen])
			if !util.IsNil(fileMd5) {
				fileMd5.Write(pieceContent[:readLen])
			}
		}
		if curContent >= pieceLen {
			break
		}
	}
	if start != pieceMetaRecord.StartByte {
		return fmt.Errorf("piece content first letter check fail, real letter (%c), expected letter (%c)", start, pieceMetaRecord.StartByte)
	}
	if end != pieceMetaRecord.EndByte {
		return fmt.Errorf("piece content end letter check fail, real letter (%c), expected letter (%c)", end, pieceMetaRecord.EndByte)
	}
	realMd5 := fileutils.GetMd5Sum(pieceMd5, nil)
	if realMd5 != pieceMetaRecord.Md5 {
		return fmt.Errorf("piece md5 check fail, read md5 (%s), expected md5 (%s)", realMd5, pieceMetaRecord.Md5)
	}

	return nil
}

