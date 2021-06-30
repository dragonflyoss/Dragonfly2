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
	"time"

	"d7y.io/dragonfly/v2/cdnsystem/daemon/cdn/storage"
	cdnerrors "d7y.io/dragonfly/v2/cdnsystem/errors"
	"d7y.io/dragonfly/v2/cdnsystem/types"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/source"
	"d7y.io/dragonfly/v2/pkg/util/digestutils"
	"d7y.io/dragonfly/v2/pkg/util/stringutils"
	"github.com/pkg/errors"
)

// cacheDetector detect task cache
type cacheDetector struct {
	cacheDataManager *cacheDataManager
}

// cacheResult cache result of detect
type cacheResult struct {
	breakPoint       int64                      // break-point of task file
	pieceMetaRecords []*storage.PieceMetaRecord // piece meta data records of task
	fileMetaData     *storage.FileMetaData      // file meta data of task
}

func (s *cacheResult) String() string {
	return fmt.Sprintf("{breakNum:%d, pieceMetaRecords:%+v, fileMetaData:%+v}", s.breakPoint, s.pieceMetaRecords, s.fileMetaData)
}

// newCacheDetector create a new cache detector
func newCacheDetector(cacheDataManager *cacheDataManager) *cacheDetector {
	return &cacheDetector{
		cacheDataManager: cacheDataManager,
	}
}

func (cd *cacheDetector) detectCache(task *types.SeedTask, fileMd5 hash.Hash) (*cacheResult, error) {
	//err := cd.cacheStore.CreateUploadLink(ctx, task.TaskId)
	//if err != nil {
	//	return nil, errors.Wrapf(err, "failed to create upload symbolic link")
	//}
	result, err := cd.doDetect(task, fileMd5)
	if err != nil {
		logger.WithTaskID(task.TaskID).Infof("failed to detect cache, reset cache: %v", err)
		metaData, err := cd.resetCache(task)
		if err == nil {
			result = &cacheResult{
				fileMetaData: metaData,
			}
			return result, nil
		}
		return result, err
	}
	if err := cd.cacheDataManager.updateAccessTime(task.TaskID, getCurrentTimeMillisFunc()); err != nil {
		logger.WithTaskID(task.TaskID).Warnf("failed to update task access time ")
	}
	return result, nil
}

// doDetect the actual detect action which detects file metaData and pieces metaData of specific task
func (cd *cacheDetector) doDetect(task *types.SeedTask, fileMd5 hash.Hash) (result *cacheResult, err error) {
	fileMetaData, err := cd.cacheDataManager.readFileMetaData(task.TaskID)
	if err != nil {
		return nil, errors.Wrapf(err, "read file meta data of task %s", task.TaskID)
	}
	if err := checkSameFile(task, fileMetaData); err != nil {
		return nil, errors.Wrapf(err, "check same file")
	}
	ctx, expireCancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer expireCancel()
	expired, err := source.IsExpired(ctx, task.URL, task.Header, fileMetaData.ExpireInfo)
	if err != nil {
		// 如果获取失败，则认为没有过期，防止打爆源
		logger.WithTaskID(task.TaskID).Errorf("failed to check if the task expired: %v", err)
	}
	logger.WithTaskID(task.TaskID).Debugf("task expired result: %t", expired)
	if expired {
		return nil, cdnerrors.ErrResourceExpired{URL: task.URL}
	}
	// not expired
	if fileMetaData.Finish {
		// quickly detect the cache situation through the meta data
		return cd.parseByReadMetaFile(task.TaskID, fileMetaData)
	}
	// check if the resource supports range request. if so,
	// detect the cache situation by reading piece meta and data file
	ctx, rangeCancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer rangeCancel()
	supportRange, err := source.IsSupportRange(ctx, task.URL, task.Header)
	if err != nil {
		return nil, errors.Wrapf(err, "check if url(%s) supports range request", task.URL)
	}
	if !supportRange {
		return nil, cdnerrors.ErrResourceNotSupportRangeRequest{URL: task.URL}
	}
	return cd.parseByReadFile(task.TaskID, fileMetaData, fileMd5)
}

// parseByReadMetaFile detect cache by read meta and pieceMeta files of task
func (cd *cacheDetector) parseByReadMetaFile(taskID string, fileMetaData *storage.FileMetaData) (*cacheResult, error) {
	if !fileMetaData.Success {
		return nil, fmt.Errorf("success flag of taskID %s is false", taskID)
	}
	pieceMetaRecords, err := cd.cacheDataManager.readAndCheckPieceMetaRecords(taskID, fileMetaData.PieceMd5Sign)
	if err != nil {
		return nil, errors.Wrapf(err, "check piece meta integrity")
	}
	if fileMetaData.TotalPieceCount > 0 && len(pieceMetaRecords) != int(fileMetaData.TotalPieceCount) {
		err := cdnerrors.ErrInconsistentValues{Expected: fileMetaData.TotalPieceCount, Actual: len(pieceMetaRecords)}
		return nil, errors.Wrapf(err, "compare file piece count")
	}
	storageInfo, err := cd.cacheDataManager.statDownloadFile(taskID)
	if err != nil {
		return nil, errors.Wrapf(err, "get cdn file length")
	}
	// check file data integrity by file size
	if fileMetaData.CdnFileLength != storageInfo.Size {
		err := cdnerrors.ErrInconsistentValues{
			Expected: fileMetaData.CdnFileLength,
			Actual:   storageInfo.Size,
		}
		return nil, errors.Wrapf(err, "compare file cdn file length")
	}
	return &cacheResult{
		breakPoint:       -1,
		pieceMetaRecords: pieceMetaRecords,
		fileMetaData:     fileMetaData,
	}, nil
}

// parseByReadFile detect cache by read pieceMeta and data files of task
func (cd *cacheDetector) parseByReadFile(taskID string, metaData *storage.FileMetaData, fileMd5 hash.Hash) (*cacheResult, error) {
	reader, err := cd.cacheDataManager.readDownloadFile(taskID)
	if err != nil {
		return nil, errors.Wrapf(err, "read data file")
	}
	defer reader.Close()
	tempRecords, err := cd.cacheDataManager.readPieceMetaRecords(taskID)
	if err != nil {
		return nil, errors.Wrapf(err, "read piece meta file")
	}

	// sort piece meta records by pieceNum
	sort.Slice(tempRecords, func(i, j int) bool {
		return tempRecords[i].PieceNum < tempRecords[j].PieceNum
	})

	var breakPoint uint64 = 0
	pieceMetaRecords := make([]*storage.PieceMetaRecord, 0, len(tempRecords))
	for index := range tempRecords {
		if int32(index) != tempRecords[index].PieceNum {
			break
		}
		// read content
		if err := checkPieceContent(reader, tempRecords[index], fileMd5); err != nil {
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
	// todo already download done, piece 信息已经写完但是meta信息还没有完成更新
	//if metaData.SourceFileLen >=0 && int64(breakPoint) == metaData.SourceFileLen {
	//	return &cacheResult{
	//		breakPoint:       -1,
	//		pieceMetaRecords: pieceMetaRecords,
	//		fileMetaData:     metaData,
	//		fileMd5:          fileMd5,
	//	}, nil
	//}
	// todo 整理数据文件 truncate breakpoint之后的数据内容
	return &cacheResult{
		breakPoint:       int64(breakPoint),
		pieceMetaRecords: pieceMetaRecords,
		fileMetaData:     metaData,
	}, nil
}

// resetCache
func (cd *cacheDetector) resetCache(task *types.SeedTask) (*storage.FileMetaData, error) {
	err := cd.cacheDataManager.resetRepo(task)
	if err != nil {
		return nil, err
	}
	// initialize meta data file
	return cd.cacheDataManager.writeFileMetaDataByTask(task)
}

/*
   helper functions
*/
// checkSameFile check whether meta file is modified
func checkSameFile(task *types.SeedTask, metaData *storage.FileMetaData) error {
	if task == nil || metaData == nil {
		return errors.Errorf("task or metaData is nil, task:%v, metaData:%v", task, metaData)
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
	if !stringutils.IsBlank(metaData.SourceRealMd5) && !stringutils.IsBlank(task.RequestMd5) &&
		metaData.SourceRealMd5 != task.RequestMd5 {
		return errors.Errorf("meta task source md5(%s) is not equals with task request md5(%s)",
			metaData.SourceRealMd5, task.RequestMd5)
	}
	return nil
}

//checkPieceContent read piece content from reader and check data integrity by pieceMetaRecord
func checkPieceContent(reader io.Reader, pieceRecord *storage.PieceMetaRecord, fileMd5 hash.Hash) error {
	// todo Analyze the original data for the slice format to calculate fileMd5
	pieceMd5 := md5.New()
	tee := io.TeeReader(io.TeeReader(io.LimitReader(reader, int64(pieceRecord.PieceLen)), pieceMd5), fileMd5)
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
