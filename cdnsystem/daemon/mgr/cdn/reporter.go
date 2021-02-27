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
	"context"
	"d7y.io/dragonfly/v2/cdnsystem/daemon/mgr"
	"github.com/pkg/errors"
)

type reporter struct {
	progress mgr.SeedProgressMgr
}

func newReporter(publisher mgr.SeedProgressMgr) *reporter {
	return &reporter{
		progress: publisher,
	}
}

// report cache result
func (re *reporter) reportCache(ctx context.Context, taskID string, detectResult *cacheResult) error {
	// report cache pieces status
	if detectResult != nil && detectResult.pieceMetaRecords != nil {
		for _, record := range detectResult.pieceMetaRecords {
			if err := re.progress.PublishPiece(ctx, taskID, convertPieceMeta2SeedPiece(record)); err != nil {
				return errors.Wrapf(err, "failed to publish pieceMetaRecord:%v, seedPiece:%v", record, convertPieceMeta2SeedPiece(record))
			}
		}
	}
	return nil
}

// reportPieceMetaRecord
func (re *reporter) reportPieceMetaRecord(ctx context.Context, taskID string, record *pieceMetaRecord) error {
	// report cache pieces status
	return re.progress.PublishPiece(ctx, taskID, convertPieceMeta2SeedPiece(record))
}
