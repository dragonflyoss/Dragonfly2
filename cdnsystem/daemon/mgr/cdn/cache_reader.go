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
	"github.com/dragonflyoss/Dragonfly/v2/cdnsystem/cdnerrors"
	"github.com/dragonflyoss/Dragonfly/v2/pkg/util"
	"github.com/dragonflyoss/Dragonfly/v2/pkg/util/fileutils"
	"github.com/pkg/errors"
	"hash"
	"io"
)

//checkPieceContent read piece content from reader and check data integrity by pieceMetaRecord
func checkPieceContent(reader io.Reader, pieceRecord *pieceMetaRecord, fileMd5 hash.Hash) error {
	bufSize := int32(256 * 1024)
	pieceLen := pieceRecord.PieceLen
	if pieceLen < bufSize {
		bufSize = pieceLen
	}
	// todo 针对分片格式解析出原始数据来计算fileMd5
	pieceContent := make([]byte, bufSize)
	var curContent int32
	pieceMd5 := md5.New()
	for {
		if curContent+bufSize <= pieceLen {
			if err := binary.Read(reader, binary.BigEndian, pieceContent); err != nil {
				return errors.Wrapf(err, "read file content error")
			}
			curContent += bufSize
			// calculate the md5
			if _, err := pieceMd5.Write(pieceContent); err != nil {
				return errors.Wrapf(err, "write piece content md5 err")
			}

			if !util.IsNil(fileMd5) {
				// todo 应该存放原始文件的md5
				if _, err := fileMd5.Write(pieceContent); err != nil {
					return errors.Wrapf(err, "write file content md5 error")
				}
			}
		} else {
			readLen := pieceLen - curContent
			if err := binary.Read(reader, binary.BigEndian, pieceContent[:readLen]); err != nil {
				return errors.Wrapf(err, "read file content error")
			}
			curContent += readLen
			// calculate the md5
			if _, err := pieceMd5.Write(pieceContent[:readLen]); err != nil {
				return errors.Wrapf(err, "write piece content md5 err")
			}
			if !util.IsNil(fileMd5) {
				// todo 应该存放原始文件的md5
				if _, err := fileMd5.Write(pieceContent[:readLen]); err != nil {
					return errors.Wrapf(err, "write file content md5 err")
				}
			}
		}
		if curContent >= pieceLen {
			break
		}
	}
	realPieceMd5 := fileutils.GetMd5Sum(pieceMd5, nil)
	// check piece content
	if realPieceMd5 != pieceRecord.Md5 {
		return errors.Wrapf(cdnerrors.ErrPieceMd5CheckFail, "realPieceMd5 md5 (%s), expected md5 (%s)", realPieceMd5, pieceRecord.Md5)
	}
	return nil
}
