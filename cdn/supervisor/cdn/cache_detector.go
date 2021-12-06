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
	"io/ioutil"
	"sort"

	"github.com/pkg/errors"
	"go.opentelemetry.io/otel/trace"

	"d7y.io/dragonfly/v2/cdn/config"
	cdnerrors "d7y.io/dragonfly/v2/cdn/errors"
	"d7y.io/dragonfly/v2/cdn/supervisor/cdn/storage"
	"d7y.io/dragonfly/v2/cdn/types"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/source"
	"d7y.io/dragonfly/v2/pkg/util/digestutils"
	"d7y.io/dragonfly/v2/pkg/util/stringutils"
)

// cacheDetector detect task cache
type cacheDetector struct {
	cacheDataManager *cacheDataManager
}

// cacheResult cache result of detect
type cacheResult struct {
	breakPoint       int64                      // break-point of task file
	pieceMetaRecords []*storage.PieceMetaRecord // piece meta data records of task
	fileMetadata     *storage.FileMetadata      // file meta data of task
}

func (s *cacheResult) String() string {
	return fmt.Sprintf("{breakNum: %d, pieceMetaRecords: %#v, fileMetadata: %#v}", s.breakPoint, s.pieceMetaRecords, s.fileMetadata)
}

// newCacheDetector create a new cache detector
func newCacheDetector(cacheDataManager *cacheDataManager) *cacheDetector {
	return &cacheDetector{
		cacheDataManager: cacheDataManager,
	}
}

func (cd *cacheDetector) detectCache(ctx context.Context, task *types.SeedTask, fileDigest hash.Hash) (result *cacheResult, err error) {
	//err := cd.cacheStore.CreateUploadLink(ctx, task.TaskId)
	//if err != nil {
	//	return nil, errors.Wrapf(err, "failed to create upload symbolic link")
	//}
	var span trace.Span
	ctx, span = tracer.Start(ctx, config.SpanDetectCache)
	defer span.End()
	defer func() {
		span.SetAttributes(config.AttributeDetectCacheResult.String(result.String()))
	}()
	result, err = cd.doDetect(ctx, task, fileDigest)
	if err != nil {
		task.Log().Infof("failed to detect cache, reset cache: %v", err)
		metaData, err := cd.resetCache(task)
		if err == nil {
			result = &cacheResult{
				fileMetadata: metaData,
			}
			return result, nil
		}
		return result, err
	}
	if err := cd.cacheDataManager.updateAccessTime(task.TaskID, getCurrentTimeMillisFunc()); err != nil {
		task.Log().Warnf("failed to update task access time ")
	}
	return result, nil
}

// doDetect the actual detect action which detects file metaData and pieces metaData of specific task
func (cd *cacheDetector) doDetect(ctx context.Context, task *types.SeedTask, fileDigest hash.Hash) (result *cacheResult, err error) {
	span := trace.SpanFromContext(ctx)
	fileMetadata, err := cd.cacheDataManager.readFileMetadata(task.TaskID)
	if err != nil {
		span.RecordError(err)
		return nil, errors.Wrapf(err, "read file meta data of task %s", task.TaskID)
	}
	span.SetAttributes()
	if err := checkSameFile(task, fileMetadata); err != nil {
		return nil, errors.Wrapf(err, "check same file")
	}
	checkExpiredRequest, err := source.NewRequestWithContext(ctx, task.URL, task.Header)
	if err != nil {
		return nil, errors.Wrapf(err, "create request")
	}
	expired, err := source.IsExpired(checkExpiredRequest, &source.ExpireInfo{
		LastModified: fileMetadata.ExpireInfo[source.LastModified],
		ETag:         fileMetadata.ExpireInfo[source.ETag],
	})
	if err != nil {
		// If the check fails, the resource is regarded as not expired to prevent the source from being knocked down
		task.Log().Warnf("failed to check whether the source is expired. To prevent the source from being suspended, "+
			"assume that the source is not expired: %v", err)
	}
	task.Log().Debugf("task resource expired result: %t", expired)
	if expired {
		return nil, errors.Errorf("resource %s has expired", task.TaskURL)
	}
	// not expired
	if fileMetadata.Finish {
		// quickly detect the cache situation through the metadata
		return cd.detectByReadMetaFile(task.TaskID, fileMetadata)
	}
	// check if the resource supports range request. if so,
	// detect the cache situation by reading piece meta and data file
	checkSupportRangeRequest, err := source.NewRequestWithContext(ctx, task.URL, task.Header)
	if err != nil {
		return nil, errors.Wrapf(err, "create check support range request")
	}
	checkSupportRangeRequest.Header.Add(source.Range, "0-0")
	supportRange, err := source.IsSupportRange(checkSupportRangeRequest)
	if err != nil {
		return nil, errors.Wrap(err, "check if support range")
	}
	if !supportRange {
		return nil, errors.Errorf("resource %s is not support range request", task.URL)
	}
	return cd.detectByReadFile(task.TaskID, fileMetadata, fileDigest)
}

// parseByReadMetaFile detect cache by read meta and pieceMeta files of task
func (cd *cacheDetector) detectByReadMetaFile(taskID string, fileMetadata *storage.FileMetadata) (*cacheResult, error) {
	if !fileMetadata.Success {
		return nil, fmt.Errorf("success flag of taskID %s is false", taskID)
	}
	pieceMetaRecords, err := cd.cacheDataManager.readAndCheckPieceMetaRecords(taskID, fileMetadata.PieceMd5Sign)
	if err != nil {
		return nil, errors.Wrapf(err, "check piece meta integrity")
	}
	if fileMetadata.TotalPieceCount > 0 && len(pieceMetaRecords) != int(fileMetadata.TotalPieceCount) {
		err := cdnerrors.ErrInconsistentValues{Expected: fileMetadata.TotalPieceCount, Actual: len(pieceMetaRecords)}
		return nil, errors.Wrapf(err, "compare file piece count")
	}
	storageInfo, err := cd.cacheDataManager.statDownloadFile(taskID)
	if err != nil {
		return nil, errors.Wrapf(err, "get cdn file length")
	}
	// check file data integrity by file size
	if fileMetadata.CdnFileLength != storageInfo.Size {
		err := cdnerrors.ErrInconsistentValues{
			Expected: fileMetadata.CdnFileLength,
			Actual:   storageInfo.Size,
		}
		return nil, errors.Wrapf(err, "compare file cdn file length")
	}
	return &cacheResult{
		breakPoint:       -1,
		pieceMetaRecords: pieceMetaRecords,
		fileMetadata:     fileMetadata,
	}, nil
}

// parseByReadFile detect cache by read pieceMeta and data files of task
func (cd *cacheDetector) detectByReadFile(taskID string, metadata *storage.FileMetadata, fileDigest hash.Hash) (*cacheResult, error) {
	reader, err := cd.cacheDataManager.readDownloadFile(taskID)
	if err != nil {
		return nil, errors.Wrapf(err, "read download data file")
	}
	defer reader.Close()
	tempRecords, err := cd.cacheDataManager.readPieceMetaRecords(taskID)
	if err != nil {
		return nil, errors.Wrapf(err, "read piece meta records")
	}

	// sort piece meta records by pieceNum
	sort.Slice(tempRecords, func(i, j int) bool {
		return tempRecords[i].PieceNum < tempRecords[j].PieceNum
	})

	var breakPoint uint64 = 0
	pieceMetaRecords := make([]*storage.PieceMetaRecord, 0, len(tempRecords))
	for index := range tempRecords {
		if uint32(index) != tempRecords[index].PieceNum {
			break
		}
		// read content TODO concurrent by multi-goroutine
		if err := checkPieceContent(reader, tempRecords[index], fileDigest); err != nil {
			logger.WithTaskID(taskID).Errorf("read content of pieceNum %d failed: %v", tempRecords[index].PieceNum, err)
			break
		}
		breakPoint = tempRecords[index].OriginRange.EndIndex + 1
		pieceMetaRecords = append(pieceMetaRecords, tempRecords[index])
	}
	if len(tempRecords) != len(pieceMetaRecords) {
		if err := cd.cacheDataManager.writePieceMetaRecords(taskID, pieceMetaRecords); err != nil {
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
		breakPoint:       int64(breakPoint),
		pieceMetaRecords: pieceMetaRecords,
		fileMetadata:     metadata,
	}, nil
}

// resetCache
func (cd *cacheDetector) resetCache(task *types.SeedTask) (*storage.FileMetadata, error) {
	err := cd.cacheDataManager.resetRepo(task)
	if err != nil {
		return nil, err
	}
	// initialize meta data file
	return cd.cacheDataManager.writeFileMetadataByTask(task)
}

/*
   helper functions
*/
// checkSameFile check whether meta file is modified
func checkSameFile(task *types.SeedTask, metaData *storage.FileMetadata) error {
	if task == nil || metaData == nil {
		return errors.Errorf("task or metaData is nil, task: %v, metaData: %v", task, metaData)
	}

	if metaData.PieceSize != task.PieceSize {
		return errors.Errorf("meta piece size(%d) is not equals with task piece size(%d)", metaData.PieceSize,
			task.PieceSize)
	}

	if metaData.TaskID != task.TaskID {
		return errors.Errorf("meta task TaskId(%s) is not equals with task TaskId(%s)", metaData.TaskID, task.TaskID)
	}

	if metaData.TaskURL != task.TaskURL {
		return errors.Errorf("meta task taskUrl(%s) is not equals with task taskUrl(%s)", metaData.TaskURL, task.URL)
	}
	if !stringutils.IsBlank(metaData.SourceRealDigest) && !stringutils.IsBlank(task.RequestDigest) &&
		metaData.SourceRealDigest != task.RequestDigest {
		return errors.Errorf("meta task source digest(%s) is not equals with task request digest(%s)",
			metaData.SourceRealDigest, task.RequestDigest)
	}
	return nil
}

//checkPieceContent read piece content from reader and check data integrity by pieceMetaRecord
func checkPieceContent(reader io.Reader, pieceRecord *storage.PieceMetaRecord, fileDigest hash.Hash) error {
	// TODO Analyze the original data for the slice format to calculate fileMd5
	pieceMd5 := md5.New()
	tee := io.TeeReader(io.TeeReader(io.LimitReader(reader, int64(pieceRecord.PieceLen)), pieceMd5), fileDigest)
	if n, err := io.Copy(ioutil.Discard, tee); n != int64(pieceRecord.PieceLen) || err != nil {
		return errors.Wrap(err, "read piece content")
	}
	realPieceMd5 := digestutils.ToHashString(pieceMd5)
	// check piece content
	if realPieceMd5 != pieceRecord.Md5 {
		err := cdnerrors.ErrInconsistentValues{
			Expected: pieceRecord.Md5,
			Actual:   realPieceMd5,
		}
		return errors.Wrap(err, "compare piece md5")
	}
	return nil
}
