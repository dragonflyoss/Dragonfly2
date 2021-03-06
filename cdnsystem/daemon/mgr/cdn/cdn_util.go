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
)

var getCurrentTimeMillisFunc = timeutils.CurrentTimeMillis

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

func convertPieceMeta2SeedPiece(record *PieceMetaRecord) *types.SeedPiece {
	return &types.SeedPiece{
		PieceStyle:  record.PieceStyle,
		PieceNum:    record.PieceNum,
		PieceMd5:    record.Md5,
		PieceRange:  record.Range,
		PieceOffset: record.Offset,
		PieceLen:    record.PieceLen,
	}
}