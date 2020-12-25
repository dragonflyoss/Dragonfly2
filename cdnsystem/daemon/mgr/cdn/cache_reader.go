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
	"crypto/md5"
	"encoding/binary"
	"github.com/dragonflyoss/Dragonfly2/pkg/util"
	"github.com/dragonflyoss/Dragonfly2/pkg/util/fileutils"
	"github.com/pkg/errors"
	"hash"
	"io"
)

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
	for {
		if curContent+bufSize <= pieceLen {
			if err := binary.Read(reader, binary.BigEndian, pieceContent); err != nil {
				return err
			}
			curContent += bufSize
			// calculate the md5
			pieceMd5.Write(pieceContent)

			if !util.IsNil(fileMd5) {
				// todo 应该存放原始文件的md5
				fileMd5.Write(pieceContent)
			}
		} else {
			readLen := pieceLen - curContent
			if err := binary.Read(reader, binary.BigEndian, pieceContent[:readLen]); err != nil {
				return err
			}
			curContent += readLen
			// calculate the md5
			pieceMd5.Write(pieceContent[:readLen])
			if !util.IsNil(fileMd5) {
				// todo 应该存放原始文件的md5
				fileMd5.Write(pieceContent[:readLen])
			}
		}
		if curContent >= pieceLen {
			break
		}
	}
	realPieceMd5 := fileutils.GetMd5Sum(pieceMd5, nil)
	// check piece content integrity
	if realPieceMd5 != pieceMetaRecord.Md5 {
		return errors.Errorf("piece md5 check fail, realPieceMd5 md5 (%s), expected md5 (%s)", realPieceMd5, pieceMetaRecord.Md5)
	}
	return nil
}

