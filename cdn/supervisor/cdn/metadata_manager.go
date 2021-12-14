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
	"sort"

	"github.com/pkg/errors"

	"d7y.io/dragonfly/v2/cdn/supervisor/cdn/storage"
	"d7y.io/dragonfly/v2/cdn/supervisor/task"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	pkgsync "d7y.io/dragonfly/v2/pkg/sync"
	"d7y.io/dragonfly/v2/pkg/util/digestutils"
	"d7y.io/dragonfly/v2/pkg/util/stringutils"
	"d7y.io/dragonfly/v2/pkg/util/timeutils"
)

// metadataManager manages the meta file and piece meta file of each TaskID.
type metadataManager struct {
	storage storage.Manager
	kmu     *pkgsync.Krwmutex
}

func newMetadataManager(storageManager storage.Manager) *metadataManager {
	return &metadataManager{
		storage: storageManager,
		kmu:     pkgsync.NewKrwmutex(),
	}
}

// writeFileMetadataByTask stores metadata of task
func (mm *metadataManager) writeFileMetadataByTask(seedTask *task.SeedTask) (*storage.FileMetadata, error) {
	mm.kmu.Lock(seedTask.ID)
	defer mm.kmu.Unlock(seedTask.ID)

	metadata := &storage.FileMetadata{
		TaskID:          seedTask.ID,
		TaskURL:         seedTask.TaskURL,
		PieceSize:       seedTask.PieceSize,
		SourceFileLen:   seedTask.SourceFileLength,
		AccessTime:      getCurrentTimeMillisFunc(),
		CdnFileLength:   seedTask.CdnFileLength,
		Digest:          seedTask.Digest,
		Tag:             seedTask.Tag,
		TotalPieceCount: seedTask.TotalPieceCount,
		Range:           seedTask.Range,
		Filter:          seedTask.Filter,
	}

	if err := mm.storage.WriteFileMetadata(seedTask.ID, metadata); err != nil {
		return nil, errors.Wrapf(err, "write task metadata file")
	}

	return metadata, nil
}

// updateAccessTime update access and interval
func (mm *metadataManager) updateAccessTime(taskID string, accessTime int64) error {
	mm.kmu.Lock(taskID)
	defer mm.kmu.Unlock(taskID)

	originMetadata, err := mm.readFileMetadata(taskID)
	if err != nil {
		return err
	}
	// access interval
	interval := accessTime - originMetadata.AccessTime
	originMetadata.Interval = interval
	if interval <= 0 {
		logger.WithTaskID(taskID).Warnf("file hit interval: %d, accessTime: %s", interval, timeutils.MillisUnixTime(accessTime))
		originMetadata.Interval = 0
	}

	originMetadata.AccessTime = accessTime

	return mm.storage.WriteFileMetadata(taskID, originMetadata)
}

func (mm *metadataManager) updateExpireInfo(taskID string, expireInfo map[string]string) error {
	mm.kmu.Lock(taskID)
	defer mm.kmu.Unlock(taskID)

	originMetadata, err := mm.readFileMetadata(taskID)
	if err != nil {
		return err
	}

	originMetadata.ExpireInfo = expireInfo

	return mm.storage.WriteFileMetadata(taskID, originMetadata)
}

func (mm *metadataManager) updateStatusAndResult(taskID string, metadata *storage.FileMetadata) error {
	mm.kmu.Lock(taskID)
	defer mm.kmu.Unlock(taskID)

	originMetadata, err := mm.readFileMetadata(taskID)
	if err != nil {
		return err
	}

	originMetadata.Finish = metadata.Finish
	originMetadata.Success = metadata.Success
	if originMetadata.Success {
		originMetadata.CdnFileLength = metadata.CdnFileLength
		originMetadata.SourceFileLen = metadata.SourceFileLen
		if metadata.TotalPieceCount > 0 {
			originMetadata.TotalPieceCount = metadata.TotalPieceCount
		}
		if !stringutils.IsBlank(metadata.SourceRealDigest) {
			originMetadata.SourceRealDigest = metadata.SourceRealDigest
		}
		if !stringutils.IsBlank(metadata.PieceMd5Sign) {
			originMetadata.PieceMd5Sign = metadata.PieceMd5Sign
		}
	}
	return mm.storage.WriteFileMetadata(taskID, originMetadata)
}

func (mm *metadataManager) readFileMetadata(taskID string) (*storage.FileMetadata, error) {
	return mm.storage.ReadFileMetadata(taskID)
}

// appendPieceMetadata append piece meta info to storage
func (mm *metadataManager) appendPieceMetadata(taskID string, record *storage.PieceMetaRecord) error {
	mm.kmu.Lock(taskID)
	defer mm.kmu.Unlock(taskID)
	// write to the storage
	return mm.storage.AppendPieceMetadata(taskID, record)
}

// appendPieceMetadata append piece meta info to storage
func (mm *metadataManager) writePieceMetaRecords(taskID string, records []*storage.PieceMetaRecord) error {
	mm.kmu.Lock(taskID)
	defer mm.kmu.Unlock(taskID)
	// write to the storage
	return mm.storage.WritePieceMetaRecords(taskID, records)
}

// readPieceMetaRecords reads pieceMetaRecords from storage and without check data integrity
func (mm *metadataManager) readPieceMetaRecords(taskID string) ([]*storage.PieceMetaRecord, error) {
	mm.kmu.RLock(taskID)
	defer mm.kmu.RUnlock(taskID)
	pieceMetaRecords, err := mm.storage.ReadPieceMetaRecords(taskID)
	if err != nil {
		return nil, errors.Wrapf(err, "read piece meta file")
	}
	// sort piece meta records by pieceNum
	sort.Slice(pieceMetaRecords, func(i, j int) bool {
		return pieceMetaRecords[i].PieceNum < pieceMetaRecords[j].PieceNum
	})
	return pieceMetaRecords, nil
}

func (mm *metadataManager) getPieceMd5Sign(taskID string) (string, []*storage.PieceMetaRecord, error) {
	mm.kmu.RLock(taskID)
	defer mm.kmu.RUnlock(taskID)
	pieceMetaRecords, err := mm.storage.ReadPieceMetaRecords(taskID)
	if err != nil {
		return "", nil, errors.Wrapf(err, "read piece meta file")
	}

	var pieceMd5 []string
	sort.Slice(pieceMetaRecords, func(i, j int) bool {
		return pieceMetaRecords[i].PieceNum < pieceMetaRecords[j].PieceNum
	})
	for _, piece := range pieceMetaRecords {
		pieceMd5 = append(pieceMd5, piece.Md5)
	}
	return digestutils.Sha256(pieceMd5...), pieceMetaRecords, nil
}
