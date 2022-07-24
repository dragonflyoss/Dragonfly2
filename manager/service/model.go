package service

import (
	"context"
	"encoding/json"

	"d7y.io/dragonfly/v2/manager/cache"
	"d7y.io/dragonfly/v2/manager/types"
)

func (s *service) GetModel(ctx context.Context, params types.ModelParams, modelInfo types.Model) (*types.StorageModel, error) {
	model, err := s.rdb.Get(ctx, cache.MakeModelVersionKey(modelInfo.SchedulerClusterID, modelInfo.Hostname, modelInfo.IP, params.ID, params.VersionID)).Result()
	if err != nil {
		return nil, err
	}

	var modelFromDb types.StorageModel
	err = json.Unmarshal([]byte(model), &modelFromDb)
	if err != nil {
		return nil, err
	}
	return &modelFromDb, nil
}

func (s *service) GetModels(ctx context.Context, params types.ModelParams, modelInfo types.Model) ([]*types.StorageModel, error) {
	var (
		models []*types.StorageModel
		cursor uint64
		keys   []string
		err    error
	)
	keys, cursor, err = s.rdb.Scan(ctx, cursor, cache.MakeModelKey(modelInfo.SchedulerClusterID, modelInfo.Hostname, modelInfo.IP, params.ID)+":*", 0).Result()
	if err != nil {
		return nil, err
	}

	for _, key := range keys {
		model, err := s.rdb.Get(ctx, key).Result()
		if err != nil {
			return nil, err
		}
		var modelFromDb types.StorageModel
		err = json.Unmarshal([]byte(model), &modelFromDb)
		if err != nil {
			return nil, err
		}
		models = append(models, &modelFromDb)
	}

	return models, nil
}

//TODO if remove params in model, no need to update model
func (s *service) UpdateModel(ctx context.Context, params types.ModelParams, modelInfo types.Model) error {
	//modelStored := types.StorageModel{
	//	ID:      params.ID,
	//	Version: params.VersionID,
	//}
	//_, err := s.rdb.Set(ctx, cache.MakeModelVersionKey(modelInfo.SchedulerClusterID, modelInfo.Hostname, modelInfo.IP, params.ID, params.VersionID), modelStored, -1).Result()
	//if err != nil {
	//	return err
	//}
	return nil
}

func (s *service) DeleteModel(ctx context.Context, params types.ModelParams, modelInfo types.Model) error {
	_, err := s.rdb.Del(ctx, cache.MakeModelVersionKey(modelInfo.SchedulerClusterID, modelInfo.Hostname, modelInfo.IP, params.ID, params.VersionID)).Result()
	if err != nil {
		return err
	}
	return nil
}

func (s *service) GetVersion(ctx context.Context, params types.ModelParams, modelInfo types.Model) (*types.Version, error) {
	version, err := s.rdb.Get(ctx, cache.MakeModelKey(modelInfo.SchedulerClusterID, modelInfo.Hostname, modelInfo.IP, params.ID)).Result()
	if err != nil {
		return nil, err
	}
	var versionFromDb types.Version
	err = json.Unmarshal([]byte(version), &versionFromDb)
	if err != nil {
		return nil, err
	}
	return &versionFromDb, nil
}

func (s *service) GetVersions(ctx context.Context, modelInfo types.Model) ([]*types.Version, error) {
	var (
		versions []*types.Version
		cursor   uint64
		keys     []string
		err      error
	)
	keys, cursor, err = s.rdb.Scan(ctx, cursor, cache.MakePrefixKey(modelInfo.SchedulerClusterID, modelInfo.Hostname, modelInfo.IP)+":?", 0).Result()
	if err != nil {
		return nil, err
	}

	for _, key := range keys {
		version, err := s.rdb.Get(ctx, key).Result()
		if err != nil {
			return nil, err
		}
		var versionFromDb types.Version
		err = json.Unmarshal([]byte(version), &versionFromDb)
		if err != nil {
			return nil, err
		}
		versions = append(versions, &versionFromDb)
	}

	return versions, nil
}

func (s *service) UpdateVersion(ctx context.Context, params types.ModelParams, modelInfo types.Model) (*types.Version, error) {
	versionStored := types.Version{
		VersionID: modelInfo.VersionID,
		Recall:    modelInfo.Recall,
		Precision: modelInfo.Precision,
	}

	_, err := s.rdb.Set(ctx, cache.MakeModelKey(modelInfo.SchedulerClusterID, modelInfo.Hostname, modelInfo.IP, params.ID), versionStored, -1).Result()
	if err != nil {
		return nil, err
	}
	return &versionStored, nil
}

func (s *service) DeleteVersion(ctx context.Context, params types.ModelParams, modelInfo types.Model) error {
	_, err := s.rdb.Del(ctx, cache.MakeModelKey(modelInfo.SchedulerClusterID, modelInfo.Hostname, modelInfo.IP, params.ID)).Result()
	if err != nil {
		return err
	}
	return nil
}
