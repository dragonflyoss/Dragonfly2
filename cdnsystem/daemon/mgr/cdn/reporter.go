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

import "github.com/dragonflyoss/Dragonfly2/cdnsystem/daemon/mgr"

type reporter struct {
	seedPieceManager mgr.SeedPieceMgr
}

func newReporter(seedPublisher mgr.SeedPieceMgr) *reporter {
	return &reporter{
		seedPieceManager: seedPublisher,
	}
}

func (re *reporter) reportCache(taskID string, detectResult *cacheResult) error {
	// report cache pieces status
	if detectResult != nil && detectResult.pieceMetaRecords != nil {
		for _, record := range detectResult.pieceMetaRecords {
			if err := re.seedPieceManager.Publish(taskID, record); err != nil {
				return err
			}
		}
	}
	return nil
}

func (re *reporter) reportPieceMetaRecord(taskID string, record PieceMetaRecord) error {
	// report cache pieces status
	return re.seedPieceManager.Publish(taskID, record)
}
