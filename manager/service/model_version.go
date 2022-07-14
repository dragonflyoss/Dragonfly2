package service

import (
	"context"
	"encoding/json"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/manager/cache"
	"d7y.io/dragonfly/v2/manager/model"
	"d7y.io/dragonfly/v2/manager/types"
)

func (s *service) GetVersion(ctx context.Context, params types.ModelParams, modelInfo types.ModelInfos) (*model.Version, error) {
	modelVal, err := s.rdb.Get(ctx, cache.MakeVersionKey(modelInfo.SchedulerClusterID, modelInfo.Hostname, modelInfo.IP, params.ID)).Result()
	if err != nil {
		return nil, err
	}
	var model model.Version
	err = json.Unmarshal([]byte(modelVal), &model)
	if err != nil {
		return nil, err
	}
	return &model, nil
}

func (s *service) GetVersions(ctx context.Context, modelInfo types.ModelInfos) ([]*model.Version, error) {
	var (
		versionCollections []*model.Version
		cursor             uint64
		keys               []string
		err                error
	)
	for {
		keys, cursor, err = s.rdb.Scan(ctx, cursor, cache.MakePrefixKey(modelInfo.SchedulerClusterID, modelInfo.Hostname, modelInfo.IP)+":?", 10).Result()
		if err != nil {
			return nil, err
		}

		for _, key := range keys {
			versionVal, err := s.rdb.Get(ctx, key).Result()
			if err != nil {
				return nil, err
			}
			var modelVersionFromDb model.Version
			err = json.Unmarshal([]byte(versionVal), &modelVersionFromDb)
			if err != nil {
				return nil, err
			}
			versionCollections = append(versionCollections, &modelVersionFromDb)
		}

		if cursor == 0 {
			break
		}
	}
	return versionCollections, nil
}

func (s *service) UpdateVersion(ctx context.Context, params types.ModelParams, modelInfo types.ModelInfos) error {
	versionStored := model.Version{
		VersionID: modelInfo.VersionID,
		Recall:    modelInfo.Recall,
		Precision: modelInfo.Precision,
	}
	logger.Debugf("%v", versionStored)
	_, err := s.rdb.Set(ctx, cache.MakeVersionKey(modelInfo.SchedulerClusterID, modelInfo.Hostname, modelInfo.IP, params.ID), versionStored, -1).Result()
	if err != nil {
		return err
	}
	return nil
}

func (s *service) DeleteVersion(ctx context.Context, params types.ModelParams, modelInfo types.ModelInfos) error {
	_, err := s.rdb.Del(ctx, cache.MakeVersionKey(modelInfo.SchedulerClusterID, modelInfo.Hostname, modelInfo.IP, params.ID)).Result()
	if err != nil {
		return err
	}
	return nil
}
