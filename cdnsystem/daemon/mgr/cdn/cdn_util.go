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
	"fmt"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/types"
	"strconv"
	"strings"

	"github.com/dragonflyoss/Dragonfly2/pkg/util/timeutils"
)

var getCurrentTimeMillisFunc = timeutils.GetCurrentTimeMillis

func getUpdateTaskInfoWithStatusOnly(cdnStatus string) *types.SeedTask {
	return getUpdateTaskInfo(cdnStatus, "", 0)
}

func getUpdateTaskInfo(cdnStatus, realMD5 string, cdnFileLength int64) *types.SeedTask {
	return &types.SeedTask{
		CdnStatus:     cdnStatus,
		SourceRealMd5: realMD5,
		CdnFileLength: cdnFileLength,
	}
}

func getPieceMetaValue(record PieceMetaRecord) string {
	return fmt.Sprintf("%d:%d:%s:%s:%d", record.PieceNum, record.PieceLen, record.Md5, record.Range, record.Offset)
}

func getPieceMetaRecord(value string) PieceMetaRecord {
	fields := strings.Split(value, ":")
	pieceNum, _ := strconv.Atoi(fields[0])
	pieceLen, _ := strconv.Atoi(fields[1])
	offSet, _ := strconv.ParseInt(fields[4], 10, 64)
	return PieceMetaRecord{
		PieceNum: int32(pieceNum),
		PieceLen: int32(pieceLen),
		Md5:      fields[2],
		Range:    fields[3],
		Offset:   offSet,
	}
}
