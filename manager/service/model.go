package service

import (
	"context"
	"d7y.io/dragonfly/v2/manager/cache"
	"d7y.io/dragonfly/v2/manager/model"
	"d7y.io/dragonfly/v2/manager/types"
	"encoding/json"
)

func (s *service) GetModel(ctx context.Context, params types.ModelParams, modelInfo types.ModelInfos) (*model.MachineModel, error) {
	modelVal, err := s.rdb.Get(ctx, cache.MakeModelKey(modelInfo.SchedulerClusterID, modelInfo.Hostname, modelInfo.IP, params.ID, params.VersionID)).Result()
	if err != nil {
		return nil, err
	}
	var modelFromDb model.MachineModel
	err = json.Unmarshal([]byte(modelVal), &modelFromDb)
	if err != nil {
		return nil, err
	}
	return &modelFromDb, nil
}

func (s *service) GetModels(ctx context.Context, params types.ModelParams, modelInfo types.ModelInfos) ([]*model.MachineModel, error) {
	var (
		modelCollections []*model.MachineModel
		cursor           uint64
		keys             []string
		err              error
	)
	for {
		keys, cursor, err = s.rdb.Scan(ctx, cursor, cache.MakeVersionKey(modelInfo.SchedulerClusterID, modelInfo.Hostname, modelInfo.IP, params.ID)+":*", 10).Result()
		if err != nil {
			return nil, err
		}

		for _, key := range keys {
			modelVal, err := s.rdb.Get(ctx, key).Result()
			if err != nil {
				return nil, err
			}
			var modelFromDb model.MachineModel
			err = json.Unmarshal([]byte(modelVal), &modelFromDb)
			if err != nil {
				return nil, err
			}
			modelCollections = append(modelCollections, &modelFromDb)
		}

		if cursor == 0 {
			break
		}
	}
	return modelCollections, nil
}

func (s *service) UpdateModel(ctx context.Context, params types.ModelParams, modelInfo types.ModelInfos) error {
	modelStored := model.MachineModel{
		ID:      modelInfo.SchedulerClusterID,
		Params:  modelInfo.Params,
		Version: params.VersionID,
	}
	_, err := s.rdb.Set(ctx, cache.MakeModelKey(modelInfo.SchedulerClusterID, modelInfo.Hostname, modelInfo.IP, params.ID, params.VersionID), modelStored, -1).Result()
	if err != nil {
		return err
	}
	return nil
}

func (s *service) DeleteModel(ctx context.Context, params types.ModelParams, modelInfo types.ModelInfos) error {
	_, err := s.rdb.Del(ctx, cache.MakeModelKey(modelInfo.SchedulerClusterID, modelInfo.Hostname, modelInfo.IP, params.ID, params.VersionID)).Result()
	if err != nil {
		return err
	}
	return nil
}
