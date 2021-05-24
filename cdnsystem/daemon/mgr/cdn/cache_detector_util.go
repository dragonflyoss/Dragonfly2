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
	"hash"
	"io"

	"d7y.io/dragonfly/v2/cdnsystem/cdnerrors"
	"d7y.io/dragonfly/v2/cdnsystem/daemon/mgr/cdn/storage"
	"d7y.io/dragonfly/v2/cdnsystem/types"
	"d7y.io/dragonfly/v2/pkg/util/digestutils"
	"d7y.io/dragonfly/v2/pkg/util/ifaceutils"
	"d7y.io/dragonfly/v2/pkg/util/stringutils"
	"github.com/pkg/errors"
)

// checkSameFile check whether meta file is modified
func checkSameFile(task *types.SeedTask, metaData *storage.FileMetaData) error {
	if task == nil || metaData == nil {
		return errors.Errorf("task or metaData is nil, task:%v, metaData:%v", task, metaData)
	}

	if metaData.PieceSize != task.PieceSize {
		return errors.Errorf("meta piece size(%d) is not equals with task piece size(%d)", metaData.PieceSize,
			task.PieceSize)
	}

	if metaData.TaskId != task.TaskID {
		return errors.Errorf("meta task TaskId(%s) is not equals with task TaskId(%s)", metaData.TaskId, task.TaskID)
	}

	if metaData.TaskURL != task.TaskURL {
		return errors.Errorf("meta task taskUrl(%s) is not equals with task taskUrl(%s)", metaData.TaskURL, task.URL)
	}
	if !stringutils.IsBlank(metaData.SourceRealMd5) && !stringutils.IsBlank(task.RequestMd5) &&
		metaData.SourceRealMd5 != task.RequestMd5 {
		return errors.Errorf("meta task source md5(%s) is not equals with task request md5(%s)",
			metaData.SourceRealMd5, task.RequestMd5)
	}
	return nil
}

//checkPieceContent read piece content from reader and check data integrity by pieceMetaRecord
func checkPieceContent(reader io.Reader, pieceRecord *storage.PieceMetaRecord, fileMd5 hash.Hash) error {
	bufSize := int32(256 * 1024)
	pieceLen := pieceRecord.PieceLen
	if pieceLen > 0 && pieceLen < bufSize {
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

			if !ifaceutils.IsNil(fileMd5) {
				// todo 需要存放原始文件的md5，如果是压缩文件，这里需要先解压获取原始文件来得到fileMd5
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
			if !ifaceutils.IsNil(fileMd5) {
				// todo 需要存放原始文件的md5，如果是压缩文件，这里需要先解压获取原始文件来得到fileMd5
				if _, err := fileMd5.Write(pieceContent[:readLen]); err != nil {
					return errors.Wrapf(err, "write file content md5 err")
				}
			}
		}
		if curContent >= pieceLen {
			break
		}
	}
	realPieceMd5 := digestutils.ToHashString(pieceMd5)
	// check piece content
	if realPieceMd5 != pieceRecord.Md5 {
		return errors.Wrapf(cdnerrors.ErrPieceMd5NotMatch, "realPieceMd5 md5 (%s), expected md5 (%s)",
			realPieceMd5, pieceRecord.Md5)
	}
	return nil
}
