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
	"d7y.io/dragonfly.v2/cdnsystem/daemon"
	"d7y.io/dragonfly.v2/cdnsystem/daemon/cdn/storage"
	"d7y.io/dragonfly.v2/cdnsystem/types"
	logger "d7y.io/dragonfly.v2/internal/dflog"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

type reporter struct {
	progress daemon.SeedProgressMgr
}

const (
	CacheReport      = "cache"
	DownloaderReport = "download"
)

func newReporter(publisher daemon.SeedProgressMgr) *reporter {
	return &reporter{
		progress: publisher,
	}
}

// report cache result
func (re *reporter) reportCache(taskID string, detectResult *cacheResult) error {
	// report cache pieces status
	if detectResult != nil && detectResult.pieceMetaRecords != nil {
		for _, record := range detectResult.pieceMetaRecords {
			if err := re.reportPieceMetaRecord(taskID, record, CacheReport); err != nil {
				return errors.Wrapf(err, "publish pieceMetaRecord:%v, seedPiece:%v", record,
					convertPieceMeta2SeedPiece(record))
			}
		}
	}
	return nil
}

// reportPieceMetaRecord
func (re *reporter) reportPieceMetaRecord(taskID string, record *storage.PieceMetaRecord,
	from string) error {
	// report cache pieces status
	logger.DownloaderLogger.Info(taskID,
		zap.Int32("pieceNum", record.PieceNum),
		zap.String("md5", record.Md5),
		zap.String("from", from))
	return re.progress.PublishPiece(taskID, convertPieceMeta2SeedPiece(record))
}

/*
	helper functions
*/
func convertPieceMeta2SeedPiece(record *storage.PieceMetaRecord) *types.SeedPiece {
	return &types.SeedPiece{
		PieceStyle:  record.PieceStyle,
		PieceNum:    record.PieceNum,
		PieceMd5:    record.Md5,
		PieceRange:  record.Range,
		OriginRange: record.OriginRange,
		PieceLen:    record.PieceLen,
	}
}
