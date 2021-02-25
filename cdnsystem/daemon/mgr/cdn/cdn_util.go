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
	"d7y.io/dragonfly/v2/cdnsystem/types"
	"d7y.io/dragonfly/v2/pkg/util/timeutils"
	"fmt"
	"github.com/pkg/errors"
	"strconv"
	"strings"
)

var getCurrentTimeMillisFunc = timeutils.GetCurrentTimeMillis

// getUpdateTaskInfoWithStatusOnly
func getUpdateTaskInfoWithStatusOnly(cdnStatus string) *types.SeedTask {
	return getUpdateTaskInfo(cdnStatus, "", "",0, 0)
}

func getUpdateTaskInfo(cdnStatus, realMD5 , pieceMd5Sign string, sourceFileLength, cdnFileLength int64) *types.SeedTask {
	return &types.SeedTask{
		CdnStatus:        cdnStatus,
		SourceRealMd5:    realMD5,
		SourceFileLength: sourceFileLength,
		CdnFileLength:    cdnFileLength,
	}
}

func getPieceMetaValue(record *pieceMetaRecord) string {
	return fmt.Sprintf("%d:%d:%s:%s:%d:%d", record.PieceNum, record.PieceLen, record.Md5, record.Range, record.Offset, record.PieceStyle)
}

func getPieceMetaRecord(value string) (record *pieceMetaRecord, err error) {
	defer func() {
		if msg := recover(); msg != nil {
			err = errors.Errorf("%v", msg)
		}
	}()
	fields := strings.Split(value, ":")
	pieceNum, _ := strconv.Atoi(fields[0])
	pieceLen, _ := strconv.Atoi(fields[1])
	md5 := fields[2]
	rangeStr := fields[3]
	offSet, _ := strconv.ParseUint(fields[4], 10, 64)
	pieceStyle, _ := strconv.Atoi(fields[5])
	return &pieceMetaRecord{
		PieceNum:   int32(pieceNum),
		PieceLen:   int32(pieceLen),
		Md5:        md5,
		Range:      rangeStr,
		Offset:     offSet,
		PieceStyle: types.PieceFormat(pieceStyle),
	}, nil
}

func convertPieceMeta2SeedPiece(record *pieceMetaRecord) *types.SeedPiece {
	return &types.SeedPiece{
		Type:        types.PieceType,
		PieceStyle:  record.PieceStyle,
		PieceNum:    record.PieceNum,
		PieceMd5:    record.Md5,
		PieceRange:  record.Range,
		PieceOffset: record.Offset,
		PieceLen:    record.PieceLen,
		Last:        false,
	}
}

func convertTaskInfo2SeedPiece(task *types.SeedTask, msg string) *types.SeedPiece {
	return &types.SeedPiece{
		Type:          types.TaskType,
		Last:          true,
		ContentLength: task.CdnFileLength,
		Result: types.Result{
			Success: task.CdnStatus == types.TaskInfoCdnStatusSUCCESS,
			Msg:     msg,
		},
	}
}
