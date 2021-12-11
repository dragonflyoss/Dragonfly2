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
	"crypto/md5"
	"fmt"
	"hash"
	"io"
	"sort"

	"github.com/pkg/errors"
	"go.opentelemetry.io/otel/trace"

	"d7y.io/dragonfly/v2/cdn/constants"
	"d7y.io/dragonfly/v2/cdn/supervisor/cdn/storage"
	"d7y.io/dragonfly/v2/cdn/supervisor/task"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/source"
	"d7y.io/dragonfly/v2/pkg/util/digestutils"
)

// cacheDetector detect task cache
type cacheDetector struct {
	metadataManager *metadataManager
	storageManager  storage.Manager
}

// cacheResult cache result of detect
type cacheResult struct {
	BreakPoint       int64                      `json:"break_point"`        // break-point of task file
	PieceMetaRecords []*storage.PieceMetaRecord `json:"piece_meta_records"` // piece metadata records of task
	FileMetadata     *storage.FileMetadata      `json:"file_metadata"`      // file meta data of task
}

// newCacheDetector create a new cache detector
func newCacheDetector(metadataManager *metadataManager, storageManager storage.Manager) *cacheDetector {
	return &cacheDetector{
		metadataManager: metadataManager,
		storageManager:  storageManager,
	}
}

func (cd *cacheDetector) detectCache(ctx context.Context, seedTask *task.SeedTask, fileDigest hash.Hash) (result *cacheResult, err error) {
	var span trace.Span
	ctx, span = tracer.Start(ctx, constants.SpanDetectCache)
	defer span.End()
	result, err = cd.doDetect(ctx, seedTask, fileDigest)
	if err != nil {
		metadata, err := cd.resetCache(seedTask)
		if err != nil {
			return nil, errors.Wrapf(err, "reset cache")
		}
		return &cacheResult{
			FileMetadata: metadata,
		}, nil
	}
	if err := cd.metadataManager.updateAccessTime(seedTask.ID, getCurrentTimeMillisFunc()); err != nil {
		seedTask.Log().Warnf("failed to update task access time ")
	}
	return result, nil
}

// doDetect do the actual detect action which detects file metadata and pieces metadata of specific task
func (cd *cacheDetector) doDetect(ctx context.Context, seedTask *task.SeedTask, fileDigest hash.Hash) (*cacheResult, error) {
	if _, err := cd.storageManager.StatDownloadFile(seedTask.ID); err != nil {
		return nil, err
	}
	fileMetadata, err := cd.metadataManager.readFileMetadata(seedTask.ID)
	if err != nil {
		return nil, errors.Wrapf(err, "read file metadata")
	}
	if ok, cause := checkMetadata(seedTask, fileMetadata); !ok {
		return nil, errors.Errorf("fileMetadata is inconsistent with task: %s", cause)
	}
	checkExpiredRequest, err := source.NewRequestWithContext(ctx, seedTask.RawURL, seedTask.Header)
	if err != nil {
		return nil, errors.Wrapf(err, "create request")
	}
	expired, err := source.IsExpired(checkExpiredRequest, &source.ExpireInfo{
		LastModified: fileMetadata.ExpireInfo[source.LastModified],
		ETag:         fileMetadata.ExpireInfo[source.ETag],
	})
	if err != nil {
		// If the check fails, the resource is regarded as not expired to prevent the source from being knocked down
		seedTask.Log().Warnf("failed to check whether the source is expired. To prevent the source from being suspended, "+
			"assume that the source is not expired: %v", err)
	}
	seedTask.Log().Debugf("task resource expired result: %t", expired)
	if expired {
		return nil, errors.Errorf("resource %s has expired", seedTask.TaskURL)
	}
	// not expired
	if fileMetadata.Finish {
		// quickly detect the cache situation through the metadata
		return cd.detectByReadMetaFile(seedTask.ID, fileMetadata)
	}
	// check if the resource supports range request. if so,
	// detect the cache situation by reading piece meta and data file
	checkSupportRangeRequest, err := source.NewRequestWithContext(ctx, seedTask.RawURL, seedTask.Header)
	if err != nil {
		return nil, errors.Wrapf(err, "create check support range request")
	}
	checkSupportRangeRequest.Header.Add(source.Range, "0-0")
	supportRange, err := source.IsSupportRange(checkSupportRangeRequest)
	if err != nil {
		return nil, errors.Wrap(err, "check if support range")
	}
	if !supportRange {
		return nil, errors.Errorf("resource %s is not support range request", seedTask.TaskURL)
	}
	return cd.detectByReadFile(seedTask.ID, fileMetadata, fileDigest)
}

// detectByReadMetaFile detect cache by read metadata and pieceMeta files of specific task
func (cd *cacheDetector) detectByReadMetaFile(taskID string, fileMetadata *storage.FileMetadata) (*cacheResult, error) {
	if !fileMetadata.Success {
		return nil, errors.New("metadata success flag is false")
	}
	md5Sign, pieceMetaRecords, err := cd.metadataManager.getPieceMd5Sign(taskID)
	if err != nil {
		return nil, errors.Wrap(err, "get pieces md5 sign")
	}
	if fileMetadata.TotalPieceCount > 0 && len(pieceMetaRecords) != int(fileMetadata.TotalPieceCount) {
		return nil, errors.Errorf("total piece count is inconsistent, expected is %d, but got %d", fileMetadata.TotalPieceCount, len(pieceMetaRecords))
	}
	if fileMetadata.PieceMd5Sign != "" && md5Sign != fileMetadata.PieceMd5Sign {
		return nil, errors.Errorf("piece md5 sign is inconsistent, expected is %s, but got %s", fileMetadata.PieceMd5Sign, md5Sign)
	}
	storageInfo, err := cd.storageManager.StatDownloadFile(taskID)
	if err != nil {
		return nil, errors.Wrap(err, "stat download file info")
	}
	// check file data integrity by file size
	if fileMetadata.CdnFileLength != storageInfo.Size {
		return nil, errors.Errorf("file size is inconsistent, expected is %d, but got %d", fileMetadata.CdnFileLength, storageInfo.Size)
	}
	// TODO For hybrid storage mode, synchronize disk data to memory
	return &cacheResult{
		BreakPoint:       -1,
		PieceMetaRecords: pieceMetaRecords,
		FileMetadata:     fileMetadata,
	}, nil
}

// parseByReadFile detect cache by read pieceMeta and data files of task
func (cd *cacheDetector) detectByReadFile(taskID string, metadata *storage.FileMetadata, fileDigest hash.Hash) (*cacheResult, error) {
	reader, err := cd.storageManager.ReadDownloadFile(taskID)
	if err != nil {
		return nil, errors.Wrapf(err, "read download data file")
	}
	defer reader.Close()
	tempRecords, err := cd.metadataManager.readPieceMetaRecords(taskID)
	if err != nil {
		return nil, errors.Wrapf(err, "read piece meta records")
	}
	// sort piece meta records by pieceNum
	sort.Slice(tempRecords, func(i, j int) bool {
		return tempRecords[i].PieceNum < tempRecords[j].PieceNum
	})
	var breakPoint int64 = 0
	pieceMetaRecords := make([]*storage.PieceMetaRecord, 0, len(tempRecords))
	for index := range tempRecords {
		if uint32(index) != tempRecords[index].PieceNum {
			break
		}
		// read content TODO concurrent by multi-goroutine
		if err := checkPieceContent(reader, tempRecords[index], fileDigest); err != nil {
			logger.WithTaskID(taskID).Errorf("check content of pieceNum %d failed: %v", tempRecords[index].PieceNum, err)
			break
		}
		breakPoint = int64(tempRecords[index].OriginRange.EndIndex + 1)
		pieceMetaRecords = append(pieceMetaRecords, tempRecords[index])
	}
	if len(tempRecords) != len(pieceMetaRecords) {
		if err := cd.metadataManager.writePieceMetaRecords(taskID, pieceMetaRecords); err != nil {
			return nil, errors.Wrapf(err, "write piece meta records failed")
		}
	}
	// TODO already download done, piece 信息已经写完但是meta信息还没有完成更新
	//if metadata.SourceFileLen >=0 && int64(breakPoint) == metadata.SourceFileLen {
	//	return &cacheResult{
	//		breakPoint:       -1,
	//		pieceMetaRecords: pieceMetaRecords,
	//		fileMetadata:     metadata,
	//		fileMd5:          fileMd5,
	//	}, nil
	//}
	// TODO 整理数据文件 truncate breakpoint 之后的数据内容
	return &cacheResult{
		BreakPoint:       breakPoint,
		PieceMetaRecords: pieceMetaRecords,
		FileMetadata:     metadata,
	}, nil
}

// resetCache file
func (cd *cacheDetector) resetCache(seedTask *task.SeedTask) (*storage.FileMetadata, error) {
	err := cd.storageManager.ResetRepo(seedTask)
	if err != nil {
		return nil, err
	}
	// initialize meta data file
	return cd.metadataManager.writeFileMetadataByTask(seedTask)
}

/*
   helper functions
*/

// checkMetadata check whether meta file is modified
func checkMetadata(seedTask *task.SeedTask, metadata *storage.FileMetadata) (bool, string) {
	if seedTask == nil || metadata == nil {
		return false, fmt.Sprintf("task or metadata is nil, task: %v, metadata: %v", seedTask, metadata)
	}

	if metadata.TaskID != seedTask.ID {
		return false, fmt.Sprintf("metadata TaskID(%s) is not equals with task ID(%s)", metadata.TaskID, seedTask.ID)
	}

	if metadata.TaskURL != seedTask.TaskURL {
		return false, fmt.Sprintf("metadata taskURL(%s) is not equals with task taskURL(%s)", metadata.TaskURL, seedTask.TaskURL)
	}

	if metadata.PieceSize != seedTask.PieceSize {
		return false, fmt.Sprintf("metadata piece size(%d) is not equals with task piece size(%d)", metadata.PieceSize, seedTask.PieceSize)
	}

	if seedTask.Range != metadata.Range {
		return false, fmt.Sprintf("metadata range(%s) is not equals with task range(%s)", metadata.Range, seedTask.Range)
	}

	if seedTask.Digest != metadata.Digest {
		return false, fmt.Sprintf("meta digest(%s) is not equals with task request digest(%s)",
			metadata.SourceRealDigest, seedTask.Digest)
	}

	if seedTask.Tag != metadata.Tag {
		return false, fmt.Sprintf("metadata tag(%s) is not equals with task tag(%s)", metadata.Range, seedTask.Range)
	}

	if seedTask.Filter != metadata.Filter {
		return false, fmt.Sprintf("metadata filter(%s) is not equals with task filter(%s)", metadata.Filter, seedTask.Filter)
	}
	return true, ""
}

// checkPieceContent read piece content from reader and check data integrity by pieceMetaRecord
func checkPieceContent(reader io.Reader, pieceRecord *storage.PieceMetaRecord, fileDigest hash.Hash) error {
	// TODO Analyze the original data for the slice format to calculate fileMd5
	pieceMd5 := md5.New()
	tee := io.TeeReader(io.TeeReader(io.LimitReader(reader, int64(pieceRecord.PieceLen)), pieceMd5), fileDigest)
	if n, err := io.Copy(io.Discard, tee); n != int64(pieceRecord.PieceLen) || err != nil {
		return errors.Wrap(err, "read piece content")
	}
	realPieceMd5 := digestutils.ToHashString(pieceMd5)
	// check piece content
	if realPieceMd5 != pieceRecord.Md5 {
		return errors.Errorf("piece md5 sign is inconsistent, expected is %s, but got %s", pieceRecord.Md5, realPieceMd5)
	}
	return nil
}
