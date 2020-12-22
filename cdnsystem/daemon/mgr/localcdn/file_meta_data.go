package localcdn

import (
	"context"
	"encoding/json"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/store"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/types"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/util"
	"github.com/dragonflyoss/Dragonfly2/pkg/util/stringutils"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

type fileMetaData struct {
	TaskID        string            `json:"taskID"`
	URL           string            `json:"url"`
	PieceSize     int32             `json:"pieceSize"`
	SourceFileLen int64             `json:"sourceFileLen"`
	AccessTime    int64             `json:"accessTime"`
	Interval      int64             `json:"interval"`
	CdnFileLength int64             `json:"cdnFileLength"`
	SourceMd5     string            `json:"sourceRealMd5"`
	ExpireInfo    map[string]string `json:"expireInfo"`
	Finish        bool              `json:"finish"`
	Success       bool              `json:"success"`
}

// fileMetaDataManager manages the meta file and piece meta file of each taskID.
type fileMetaDataManager struct {
	fileStore *store.Store
	locker    *util.LockerPool
}

func newFileMetaDataManager(store *store.Store) *fileMetaDataManager {
	return &fileMetaDataManager{
		fileStore: store,
		locker:    util.NewLockerPool(),
	}
}

// writeFileMetaData stores the metadata of task.ID to storage.
func (mm *fileMetaDataManager) writeFileMetaDataByTask(ctx context.Context, task *types.SeedTaskInfo) (*fileMetaData, error) {
	metaData := &fileMetaData{
		TaskID:        task.TaskID,
		URL:           task.Url,
		PieceSize:     task.PieceSize,
		SourceFileLen: task.SourceFileLength,
		AccessTime:    getCurrentTimeMillisFunc(),
		CdnFileLength: task.CdnFileLength,
	}

	if err := mm.writeFileMetaData(ctx, metaData); err != nil {
		return nil, err
	}

	return metaData, nil
}

// writeFileMetaData stores the metadata of task.ID to storage.
func (mm *fileMetaDataManager) writeFileMetaData(ctx context.Context, metaData *fileMetaData) error {
	data, err := json.Marshal(metaData)
	if err != nil {
		return errors.Wrapf(err, "failed to marshal metadata")
	}

	return mm.fileStore.PutBytes(ctx, getTaskMetaDataRawFunc(metaData.TaskID), data)
}

// readFileMetaData returns the fileMetaData info according to the taskID.
func (mm *fileMetaDataManager) readFileMetaData(ctx context.Context, taskID string) (*fileMetaData, error) {
	bytes, err := mm.fileStore.GetBytes(ctx, getTaskMetaDataRawFunc(taskID))
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get metadata bytes")
	}

	metaData := &fileMetaData{}
	if err := json.Unmarshal(bytes, metaData); err != nil {
		return nil, errors.Wrapf(err, "failed to unmarshal metadata bytes")
	}
	logrus.Debugf("success to read metadata: %+v for taskID: %s", metaData, taskID)

	return metaData, nil
}

// updateAccessTime update access and interval
func (mm *fileMetaDataManager) updateAccessTime(ctx context.Context, taskID string, accessTime int64) error {
	mm.locker.GetLock(taskID, false)
	defer mm.locker.ReleaseLock(taskID, false)

	originMetaData, err := mm.readFileMetaData(ctx, taskID)
	if err != nil {
		return errors.Wrapf(err, "failed to get origin metaData")
	}
	// access interval
	interval := accessTime - originMetaData.AccessTime
	originMetaData.Interval = interval
	if interval <= 0 {
		logrus.Warnf("taskId:%s file hit interval:%d", taskID, interval)
		originMetaData.Interval = 0
	}

	originMetaData.AccessTime = accessTime

	return mm.writeFileMetaData(ctx, originMetaData)
}

func (mm *fileMetaDataManager) updateExpireInfo(ctx context.Context, taskID string, expireInfo map[string]string) error {
	mm.locker.GetLock(taskID, false)
	defer mm.locker.ReleaseLock(taskID, false)

	originMetaData, err := mm.readFileMetaData(ctx, taskID)
	if err != nil {
		return err
	}

	originMetaData.ExpireInfo = expireInfo

	return mm.writeFileMetaData(ctx, originMetaData)
}

func (mm *fileMetaDataManager) updateStatusAndResult(ctx context.Context, taskID string, metaData *fileMetaData) error {
	mm.locker.GetLock(taskID, false)
	defer mm.locker.ReleaseLock(taskID, false)

	originMetaData, err := mm.readFileMetaData(ctx, taskID)
	if err != nil {
		return errors.Wrapf(err, "failed to get origin metadata")
	}

	originMetaData.Finish = metaData.Finish
	originMetaData.Success = metaData.Success
	if originMetaData.Success {
		originMetaData.CdnFileLength = metaData.CdnFileLength
		if !stringutils.IsEmptyStr(metaData.SourceMd5) {
			originMetaData.SourceMd5 = metaData.SourceMd5
		}
	}

	return mm.writeFileMetaData(ctx, originMetaData)
}
