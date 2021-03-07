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

package disk

import (
	"d7y.io/dragonfly/v2/cdnsystem/daemon/mgr/cdn/storage"
	"d7y.io/dragonfly/v2/cdnsystem/types"
	"fmt"
	"github.com/pkg/errors"
	"strconv"
	"strings"
)

const fieldSeparator = ":"

func getPieceMetaValue(record *storage.PieceMetaRecord) string {
	return fmt.Sprintf("%d:%d:%s:%s:%d:%d", record.PieceNum, record.PieceLen, record.Md5, record.Range, record.Offset, record.PieceStyle)
}

func parsePieceMetaRecord(value string) (record *storage.PieceMetaRecord, err error) {
	defer func() {
		if msg := recover(); msg != nil {
			err = errors.Errorf("%v", msg)
		}
	}()
	fields := strings.Split(value, fieldSeparator)
	pieceNum, err := strconv.Atoi(fields[0])
	if err != nil {
		return nil, errors.Wrapf(err, "invalid pieceNum:%s", fields[0])
	}
	pieceLen, err := strconv.Atoi(fields[1])
	if err != nil {
		return nil, errors.Wrapf(err, "invalid pieceLen:%s", fields[1])
	}
	md5 := fields[2]
	rangeStr := fields[3]
	offSet, err := strconv.ParseUint(fields[4], 10, 64)
	if err != nil {
		return nil, errors.Wrapf(err, "invalid offset:%s", fields[4])
	}
	pieceStyle, err := strconv.Atoi(fields[5])
	if err != nil {
		return nil, errors.Wrapf(err, "invalid pieceStyle:%s", fields[5])
	}
	return &storage.PieceMetaRecord{
		PieceNum:   int32(pieceNum),
		PieceLen:   int32(pieceLen),
		Md5:        md5,
		Range:      rangeStr,
		Offset:     offSet,
		PieceStyle: types.PieceFormat(pieceStyle),
	}, nil
}
