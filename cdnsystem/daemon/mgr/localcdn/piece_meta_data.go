package localcdn

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/store"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/util"
	"github.com/dragonflyoss/Dragonfly2/pkg/digest"
	"github.com/dragonflyoss/Dragonfly2/pkg/errortypes"
	"github.com/dragonflyoss/Dragonfly2/pkg/syncmap"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"sort"
	"strconv"
)

type pieceMetaData struct {
	PieceMetaRecords []pieceMetaRecord
	FileMd5          string
	Sha1Value        string
}

type pieceMetaRecord struct {
	PieceNum  int `json:"pieceNum"`
	PieceLen  int32  `json:"pieceLen"`
	Md5       string `json:"md5"`
	Range     string `json:"range"`
	Offset    int64  `json:"offset"`
	StartByte byte   `json:"startByte"`
	EndByte   byte   `json:"endByte"`
}

type pieceMetaDataManager struct {
	taskPieceMetaRecords *syncmap.SyncMap
	fileStore            *store.Store
	locker               *util.LockerPool
}

func newPieceMetaDataMgr() *pieceMetaDataManager {
	return &pieceMetaDataManager{
		taskPieceMetaRecords: syncmap.NewSyncMap(),
	}
}

func (pmm *pieceMetaDataManager) getPieceMetaRecord(taskID string, pieceNum int) (pieceMetaRecord, error) {
	pieceMetaRecords, err := pmm.taskPieceMetaRecords.GetAsMap(taskID)
	if err != nil {
		return pieceMetaRecord{}, err
	}

	return pieceMetaRecords.GetAsPieceMetaRecord(strconv.Itoa(pieceNum))
}

func (pmm *pieceMetaDataManager) setPieceMetaRecord(taskID string, pieceNum int, pieceMetaRecord pieceMetaRecord) error {
	pieceRecords, err := pmm.taskPieceMetaRecords.GetAsMap(taskID)
	if err != nil && !errortypes.IsDataNotFound(err) {
		return err
	}

	if pieceRecords == nil {
		pieceRecords = syncmap.NewSyncMap()
		pmm.taskPieceMetaRecords.Add(taskID, pieceRecords)
	}

	return pieceRecords.Add(strconv.Itoa(pieceNum), pieceMetaRecord)
}

func (pmm *pieceMetaDataManager) getPieceMetaRecordsByTaskID(taskID string) (pieceMetaRecords []pieceMetaRecord, err error) {
	pieceMD5sMap, err := pmm.taskPieceMetaRecords.GetAsMap(taskID)
	if err != nil {
		return nil, err
	}
	pieceNums := pieceMD5sMap.ListKeyAsIntSlice()
	sort.Ints(pieceNums)

	for i := 0; i < len(pieceNums); i++ {
		pieceMD5, err := pieceMD5sMap.GetAsPieceMetaRecord(strconv.Itoa(pieceNums[i]))
		if err != nil {
			return nil, err
		}
		pieceMetaRecords = append(pieceMetaRecords, pieceMD5)
	}
	return pieceMetaRecords, nil
}

func (pmm *pieceMetaDataManager) removePieceMetaRecordsByTaskID(taskID string) error {
	return pmm.taskPieceMetaRecords.Remove(taskID)
}

// writePieceMD5s writes the piece md5s to storage for the md5 file of taskID.
//
// And it should append the fileMD5 which means that the md5 of the task file
// and the SHA-1 digest of fileMD5 at the end of the file.
func (pmm *pieceMetaDataManager) writePieceMetaRecords(ctx context.Context, taskID, fileMD5 string, pieceMetaRecords []pieceMetaRecord) error {
	pmm.locker.GetLock(taskID, false)
	defer pmm.locker.ReleaseLock(taskID, false)

	if len(pieceMetaRecords) == 0 {
		logrus.Warnf("failed to write empty pieceMetaRecords for taskID: %s", taskID)
		return nil
	}
	pieceMetaStr, err := json.Marshal(pieceMetaRecords)
	if err != nil {
		return errors.Wrapf(err, "failed to marshal piece records metadata")
	}
	sha1s := make([]string, 2)
	sha1s = append(sha1s, string(pieceMetaStr))
	sha1s = append(sha1s, fileMD5)
	sha1Value := digest.Sha1(sha1s)

	pieceMetaData := pieceMetaData{
		PieceMetaRecords: pieceMetaRecords,
		FileMd5:          fileMD5,
		Sha1Value:        sha1Value,
	}

	data, err := json.Marshal(pieceMetaData)
	if err != nil {
		return errors.Wrapf(err, "failed to marshal piece metadata")
	}
	return pmm.fileStore.PutBytes(ctx, getPieceMetaDataRawFunc(taskID), data)
}

// readPieceMetaDatas reads the md5 file of the taskID and returns the pieceMD5s.
func (pmm *pieceMetaDataManager) readAndCheckPieceMetaRecords(ctx context.Context, taskID, fileMD5 string) ([]pieceMetaRecord, error) {
	pmm.locker.GetLock(taskID, true)
	defer pmm.locker.ReleaseLock(taskID, true)

	bytes, err := pmm.fileStore.GetBytes(ctx, getPieceMetaDataRawFunc(taskID))
	if err != nil {
		return nil, err
	}

	pieceMetaData := &pieceMetaData{}
	if err := json.Unmarshal(bytes, pieceMetaData); err != nil {
		return nil, errors.Wrapf(err, "failed to unmarshal piece metadata bytes")
	}

	pieceMetaRecords := pieceMetaData.PieceMetaRecords

	pieceMetaStr, err := json.Marshal(pieceMetaRecords)
	if err != nil {
		return nil, nil
	}
	sha1s := make([]string, 2)
	sha1s = append(sha1s, string(pieceMetaStr))
	sha1s = append(sha1s, pieceMetaData.FileMd5)
	expectedSha1Value := digest.Sha1(sha1s)
	if expectedSha1Value != pieceMetaData.Sha1Value {
		return nil, fmt.Errorf("taskID: %s failed to validate the SHA-1 checksum of pieceMD5s, expected: %s, real: %s", taskID, expectedSha1Value, pieceMetaData.Sha1Value)
	}

	if fileMD5 != "" && pieceMetaData.FileMd5 != fileMD5 {
		return nil, fmt.Errorf("taskID: %s failed to validate the fileMD5, expected: %s, real: %s", taskID, fileMD5, pieceMetaData.FileMd5)
	}
	return pieceMetaRecords, nil
}

// readPieceMetaDatas reads the md5 file of the taskID and returns the pieceMD5s.
func (pmm *pieceMetaDataManager) readWithoutCheckPieceMetaRecords(ctx context.Context, taskID string) ([]pieceMetaRecord, error) {
	pmm.locker.GetLock(taskID, true)
	defer pmm.locker.ReleaseLock(taskID, true)

	bytes, err := pmm.fileStore.GetBytes(ctx, getPieceMetaDataRawFunc(taskID))
	if err != nil {
		return nil, err
	}

	pieceMetaData := &pieceMetaData{}
	if err := json.Unmarshal(bytes, pieceMetaData); err != nil {
		return nil, errors.Wrapf(err, "failed to unmarshal piece metadata bytes")
	}
	return pieceMetaData.PieceMetaRecords, nil
}