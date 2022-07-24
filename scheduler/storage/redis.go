package storage

import (
	"context"
	"encoding/json"
	"fmt"

	"d7y.io/dragonfly/v2/manager/cache"
	"d7y.io/dragonfly/v2/manager/model"

	"d7y.io/dragonfly/v2/scheduler/config"
	"github.com/go-redis/redis/v8"
)

type redisStorage struct {
	client *redis.Client
	cfg    *config.Config
}

func (rs *redisStorage) UpSert(ctx context.Context, model model.MachineModel) error {
	key := cache.MakeModelKey(rs.cfg.Manager.SchedulerClusterID, rs.cfg.Server.Host, rs.cfg.Server.IP, model.ID, model.Version)
	_, err := rs.client.Set(ctx, key, model, -1).Result()
	if err != nil {
		return err
	}

	return nil
}

func (rs *redisStorage) Delete(ctx context.Context, ID, version string) error {
	key := cache.MakeModelKey(rs.cfg.Manager.SchedulerClusterID, rs.cfg.Server.Host, rs.cfg.Server.IP, ID, version)
	_, err := rs.client.Del(ctx, key).Result()
	if err != nil {
		return err
	}

	return nil
}

func (rs *redisStorage) List(ctx context.Context, ID string) ([]*model.MachineModel, error) {
	var (
		modelList []*model.MachineModel
		cursor    uint64
		keys      []string
		err       error
	)
	keys, cursor, err = rs.client.Scan(ctx, cursor, cache.MakeVersionKey(rs.cfg.Manager.SchedulerClusterID, rs.cfg.Server.Host, rs.cfg.Server.IP, ID)+":*", 0).Result()
	if err != nil {
		return nil, err
	}

	for _, key := range keys {
		result, err := rs.client.Get(ctx, key).Result()
		if err != nil {
			return nil, err
		}
		var model model.MachineModel
		err = json.Unmarshal([]byte(result), &model)
		if err != nil {
			return nil, err
		}
		modelList = append(modelList, &model)
	}
	return modelList, nil
}

func (rs *redisStorage) Clear(ctx context.Context, ID string) error {
	modelList, err := rs.List(ctx, ID)
	if err != nil {
		return err
	}

	for _, model := range modelList {
		err := rs.Delete(ctx, ID, model.Version)
		if err != nil {
			return err
		}
	}

	return nil
}

func NewRedis(cfg *config.RedisConfig, info *config.Config) (*redisStorage, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%d", cfg.Host, cfg.Port),
		Password: cfg.Password,
	})

	if err := client.Ping(context.Background()).Err(); err != nil {
		return nil, err
	}

	redisClient := &redisStorage{
		client: client,
		cfg:    info,
	}
	return redisClient, nil
}
